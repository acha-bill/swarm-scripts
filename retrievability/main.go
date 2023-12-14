package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
)

type Stewardship struct {
	IsRetrievable bool `json:"isRetrievable"`
}

func main() {
	in, err := os.Open("/Users/macbookpro/Desktop/swarm/data_durability.txt")
	if err != nil {
		panic(err)
	}
	defer in.Close()

	out, err := os.Create("out.txt")
	if err != nil {
		panic(err)
	}
	defer out.Close()

	scanner := bufio.NewScanner(in)
	var refs []string
	for scanner.Scan() {
		refs = append(refs, scanner.Text())
	}
	refs = refs[1:]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	putterC := make(chan string)
	getterC := make(chan string)
	errC := make(chan error)
	doneC := make(chan struct{})

	go func() {
		limit := make(chan struct{}, 100)
		for {
			select {
			case <-ctx.Done():
				return
			case ref := <-putterC:
				limit <- struct{}{}
				go func() {
					defer func() {
						<-limit
					}()
					req, err := http.NewRequest("PUT", "http://bee-2.dev-bee-gateway.mainnet.internal/stewardship/"+ref, nil)
					if err != nil {
						errC <- fmt.Errorf("http new request: ref=%s %w", ref, err)
						return
					}
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Accept", "application/json")
					req.Header.Set("Swarm-Postage-Batch-Id", "a2838ebe5b60a4b5d4b0bbe71e4c08b8b5e9f0674f0310cfbc59ff6c2e8eea51")
					client := &http.Client{}
					resp, err := client.Do(req)
					if err != nil {
						errC <- fmt.Errorf("http do: ref=%s %w", ref, err)
						return
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						errC <- fmt.Errorf("stewardship put: ref=%s, status=%d", ref, resp.StatusCode)
						return
					}
					getterC <- ref
				}()
			}
		}
	}()

	go func() {
		limit := make(chan struct{}, 100)
		for {
			select {
			case <-ctx.Done():
				return
			case ref := <-getterC:
				limit <- struct{}{}
				go func() {
					defer func() {
						<-limit
					}()
					resp, err := http.Get("http://bee-2.dev-bee-gateway.mainnet.internal/stewardship/" + ref)
					if err != nil {
						errC <- fmt.Errorf("http get: ref=%s %w", ref, err)
						return
					}
					defer resp.Body.Close()
					var stewardship Stewardship
					d, err := io.ReadAll(resp.Body)
					if err != nil {
						errC <- fmt.Errorf("io readall: ref=%s %w", ref, err)
						return
					}
					err = json.Unmarshal(d, &stewardship)
					if err != nil {
						errC <- fmt.Errorf("josn unmarshal: ref=%s, json=%s %w", ref, string(d), err)
						return
					}

					if stewardship.IsRetrievable {
						doneC <- struct{}{}
						return
					}

					fmt.Println(ref, "is not retrievable. Putting it back.")
					putterC <- ref
				}()
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(len(refs))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errC:
				fmt.Fprintf(out, "%v\n", err)
				fmt.Println(err)
				wg.Done()
			case <-doneC:
				wg.Done()
			}
		}
	}()

	for i, ref := range refs {
		fmt.Printf("i=%d of %d\n", i, len(refs))
		getterC <- ref
	}

	fmt.Println("waiting for all to finish")
	wg.Wait()
}

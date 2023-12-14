package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

type StewardshipRes struct {
	IsRetrievable bool `json:"isRetrievable"`
}

var durabilityCmd = &cobra.Command{
	Use: "durability",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var durabilityFileCmd = &cobra.Command{
	Use:   "file",
	Short: "Checks if the file is retrievable",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("file reference is required")
		}
		log.Println("checking", args[0])
		resp, err := http.Get(apiUrl + "/stewardship/" + args[0])
		if err != nil {
			return fmt.Errorf("http get: %w", err)
		}
		defer resp.Body.Close()
		var stewardship StewardshipRes
		err = json.NewDecoder(resp.Body).Decode(&stewardship)
		if err != nil {
			return fmt.Errorf("json decode: %w", err)
		}
		log.Println(stewardship.IsRetrievable)
		return nil
	},
}

var (
	inputFile      string
	postageBatchID string
)
var durabilityChunksCmd = &cobra.Command{
	Use:   "chunks",
	Short: "Checks every chunk in the input file if it is retrievable",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("input file is required")
		}

		in, err := os.Open(args[0])
		if err != nil {
			return err
		}
		defer in.Close()

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
						req, err := http.NewRequest("PUT", apiUrl+"/stewardship/"+ref, nil)
						if err != nil {
							errC <- fmt.Errorf("http new request: ref=%s %w", ref, err)
							return
						}
						req.Header.Set("Content-Type", "application/json")
						req.Header.Set("Accept", "application/json")
						if postageBatchID != "" {
							req.Header.Set("Swarm-Postage-Batch-Id", postageBatchID)
						}
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
						resp, err := http.Get(apiUrl + "/stewardship/" + ref)
						if err != nil {
							errC <- fmt.Errorf("http get: ref=%s %w", ref, err)
							return
						}
						defer resp.Body.Close()
						var stewardship StewardshipRes
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

						log.Println(ref, "is not retrievable. Putting it back.")
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
					log.Println(err)
					wg.Done()
				case <-doneC:
					wg.Done()
				}
			}
		}()

		start := time.Now()
		for i, ref := range refs {
			log.Printf("i=%d of %d\n", i, len(refs))
			getterC <- ref
		}

		wg.Wait()
		log.Println("done in ", time.Since(start))
		return nil
	},
}

func init() {
	durabilityChunksCmd.Flags().StringVar(&inputFile, "input-file", "", "Input file(required)")
	durabilityChunksCmd.Flags().StringVar(&postageBatchID, "batch-id", "", "Postage Batch ID")
	durabilityChunksCmd.MarkFlagRequired("input-file")

	durabilityCmd.AddCommand(durabilityFileCmd)
	durabilityCmd.AddCommand(durabilityChunksCmd)
	rootCmd.AddCommand(durabilityCmd)
}

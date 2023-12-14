package cmd

import "github.com/spf13/cobra"

var apiUrl string

var rootCmd = &cobra.Command{
	Use: "scripts",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&apiUrl, "api-url", "http://bee-2.dev-bee-gateway.mainnet.internal", "Bee API URL")
}

package cli

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sleeve",
	Short: "trace analysis tool for Kubernetes control planaes",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

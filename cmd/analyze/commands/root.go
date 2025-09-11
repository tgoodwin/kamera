package commands

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sleeve",
	Short: "Trace analysis tool for Kubernetes controllers",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

package commands

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/runtime"
)

var stalenessCmd = &cobra.Command{
	Use:   "staleness [trace-file] --reconcile-id <id>",
	Short: "Analyze stale reads in controller traces",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		tracePath := args[0]
		reconcileID, _ := cmd.Flags().GetString("reconcile-id")
		if reconcileID == "" {
			fmt.Println("error: --reconcile-id flag is required")
			return
		}

		eb := tracecheck.NewExplorerBuilder(runtime.NewScheme())
		lm, err := eb.BuildLensManager(tracePath)
		if err != nil {
			fmt.Println("failed to load trace:", err)
			return
		}
		err = lm.KnowledgeLens(reconcileID)
		if err != nil {
			fmt.Println("failed to analyze trace:", err)
			return
		}
	},
}

func init() {
	stalenessCmd.Flags().String("reconcile-id", "", "Specify the reconcile ID to analyze")
	rootCmd.AddCommand(stalenessCmd)
}

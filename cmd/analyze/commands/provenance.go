package commands

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/runtime"
)

var provenanceCmd = &cobra.Command{
	Use:   "provenance [trace-file] --reconcile-id <id>",
	Short: "Analyze stale reads in controller traces",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		tracePath := args[0]
		objectID, _ := cmd.Flags().GetString("sleeve-object-id")
		if objectID == "" {
			fmt.Println("error: --sleeve-object-id flag is required")
			return
		}

		eb := tracecheck.NewExplorerBuilder(runtime.NewScheme())
		lm, err := eb.BuildLensManager(tracePath)
		if err != nil {
			fmt.Println("failed to load trace:", err)
			return
		}
		err = lm.ProvenanceLens(objectID)
		if err != nil {
			fmt.Println("failed to analyze trace:", err)
			return
		}
	},
}

func init() {
	provenanceCmd.Flags().String("sleeve-object-id", "", "Specify the reconcile ID to analyze")
	rootCmd.AddCommand(stalenessCmd)
}

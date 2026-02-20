package interactive

import (
	"testing"

	"github.com/rivo/tview"
	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func TestPopulateSteps_HeadersExcludeObservationsColumn(t *testing.T) {
	table := tview.NewTable()
	states := []tracecheck.ResultState{
		{
			Paths: []tracecheck.ExecutionHistory{
				{
					&tracecheck.ReconcileResult{
						ControllerID: "ServiceReconciler",
						Changes: tracecheck.Changes{
							Effects:      []tracecheck.Effect{{}},
							Observations: []tracecheck.Effect{{}, {}},
						},
					},
				},
			},
		},
	}

	populateSteps(table, states, 0, 0)

	require.Equal(t, "[::b]Idx[::-]", table.GetCell(0, 0).Text)
	require.Equal(t, "[::b]Controller[::-]", table.GetCell(0, 1).Text)
	require.Equal(t, "[::b]Effects[::-]", table.GetCell(0, 2).Text)
	require.Equal(t, "[::b]ContentsHash[::-]", table.GetCell(0, 3).Text)
	require.NotEqual(t, "[::b]Observations[::-]", table.GetCell(0, 3).Text)
}

package interactive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func TestDumpReconcileResult_RoundTripsObservations(t *testing.T) {
	writeKey := snapshot.NewCompositeKeyWithGroup("apps", "Deployment", "default", "app", "obj-write")
	readKey := snapshot.NewCompositeKeyWithGroup("apps", "Deployment", "default", "app", "obj-read")

	writeHash := snapshot.VersionHash{Strategy: snapshot.AnonymizedHash, Value: "write-hash"}
	readHash := snapshot.VersionHash{Strategy: snapshot.AnonymizedHash, Value: "read-hash"}

	writeEffect := tracecheck.Effect{
		OpType:  event.CREATE,
		Key:     writeKey,
		Version: writeHash,
	}
	readObservation := tracecheck.Effect{
		OpType:  event.GET,
		Key:     readKey,
		Version: readHash,
	}

	step := &tracecheck.ReconcileResult{
		ControllerID: "DeploymentController",
		FrameID:      "frame-1",
		FrameType:    tracecheck.FrameTypeExplore,
		Changes: tracecheck.Changes{
			ObjectVersions: tracecheck.ObjectVersions{
				writeKey: writeHash,
			},
			Effects: []tracecheck.Effect{
				writeEffect,
			},
			Observations: []tracecheck.Effect{
				readObservation,
			},
		},
	}

	dumped := toDumpReconcileResult(step, nil)
	require.Len(t, dumped.Changes.Effects, 1)
	require.Len(t, dumped.Changes.Observations, 1)
	require.Equal(t, event.GET, dumped.Changes.Observations[0].OpType)

	restored := fromDumpReconcileResult(dumped, nil)
	require.NotNil(t, restored)
	require.Len(t, restored.Changes.Effects, 1)
	require.Len(t, restored.Changes.Observations, 1)
	require.Equal(t, event.GET, restored.Changes.Observations[0].OpType)
	require.Equal(t, readKey, restored.Changes.Observations[0].Key)
	require.Equal(t, readHash, restored.Changes.Observations[0].Version)
}

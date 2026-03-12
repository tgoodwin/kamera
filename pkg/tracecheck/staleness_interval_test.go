package tracecheck

import (
	"testing"
)

func TestEvaluateStalenessIntervals_BeforeWindow(t *testing.T) {
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 2, // before StaleAt=3
			},
		},
		stalenessIntervals: []StalenessInterval{
			{
				ReconcilerID: "WidgetReconciler",
				Kind:         "example.org/Widget",
				StaleAt:      3,
				CatchUpAt:    7,
				Lag:          -1,
			},
		},
	}

	result := sn.evaluateStalenessIntervals("WidgetReconciler")
	if len(result) != 0 {
		t.Fatalf("expected no stale sequences before window, got %v", result)
	}
}

func TestEvaluateStalenessIntervals_DuringWindowFrozen(t *testing.T) {
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 5, // in [3, 7)
			},
		},
		stalenessIntervals: []StalenessInterval{
			{
				ReconcilerID: "WidgetReconciler",
				Kind:         "example.org/Widget",
				StaleAt:      3,
				CatchUpAt:    7,
				Lag:          -1,
			},
		},
	}

	result := sn.evaluateStalenessIntervals("WidgetReconciler")
	if len(result) != 1 {
		t.Fatalf("expected 1 stale sequence, got %d", len(result))
	}
	if result["example.org/Widget"] != 3 {
		t.Fatalf("expected frozen at StaleAt=3, got %d", result["example.org/Widget"])
	}
}

func TestEvaluateStalenessIntervals_DuringWindowLag(t *testing.T) {
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 5, // in [3, 7), lag=2 → observe at 3
			},
		},
		stalenessIntervals: []StalenessInterval{
			{
				ReconcilerID: "WidgetReconciler",
				Kind:         "example.org/Widget",
				StaleAt:      3,
				CatchUpAt:    7,
				Lag:          2,
			},
		},
	}

	result := sn.evaluateStalenessIntervals("WidgetReconciler")
	if len(result) != 1 {
		t.Fatalf("expected 1 stale sequence, got %d", len(result))
	}
	if result["example.org/Widget"] != 3 {
		t.Fatalf("expected lag to clamp at StaleAt=3, got %d", result["example.org/Widget"])
	}
}

func TestEvaluateStalenessIntervals_DuringWindowLagNoClamp(t *testing.T) {
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 6, // in [3, 7), lag=1 → observe at 5
			},
		},
		stalenessIntervals: []StalenessInterval{
			{
				ReconcilerID: "WidgetReconciler",
				Kind:         "example.org/Widget",
				StaleAt:      3,
				CatchUpAt:    7,
				Lag:          1,
			},
		},
	}

	result := sn.evaluateStalenessIntervals("WidgetReconciler")
	if len(result) != 1 {
		t.Fatalf("expected 1 stale sequence, got %d", len(result))
	}
	if result["example.org/Widget"] != 5 {
		t.Fatalf("expected frontier(6) - lag(1) = 5, got %d", result["example.org/Widget"])
	}
}

func TestEvaluateStalenessIntervals_AfterWindow(t *testing.T) {
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 7, // at CatchUpAt, should be current
			},
		},
		stalenessIntervals: []StalenessInterval{
			{
				ReconcilerID: "WidgetReconciler",
				Kind:         "example.org/Widget",
				StaleAt:      3,
				CatchUpAt:    7,
				Lag:          -1,
			},
		},
	}

	result := sn.evaluateStalenessIntervals("WidgetReconciler")
	if len(result) != 0 {
		t.Fatalf("expected no stale sequences after window, got %v", result)
	}
}

func TestEvaluateStalenessIntervals_DifferentReconciler(t *testing.T) {
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 5, // in window for WidgetReconciler
			},
		},
		stalenessIntervals: []StalenessInterval{
			{
				ReconcilerID: "WidgetReconciler",
				Kind:         "example.org/Widget",
				StaleAt:      3,
				CatchUpAt:    7,
				Lag:          -1,
			},
		},
	}

	result := sn.evaluateStalenessIntervals("OtherReconciler")
	if len(result) != 0 {
		t.Fatalf("expected no stale sequences for different reconciler, got %v", result)
	}
}

func TestEvaluateStalenessIntervals_NoIntervals(t *testing.T) {
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 5,
			},
		},
	}

	result := sn.evaluateStalenessIntervals("WidgetReconciler")
	if result != nil {
		t.Fatalf("expected nil for no intervals, got %v", result)
	}
}

func TestObserveAs_WithStalenessIntervals(t *testing.T) {
	// When stalenessIntervals are configured, ObserveAs should use interval
	// evaluation and ignore stuckReconcilerPositions.
	sn := StateNode{
		Contents: StateSnapshot{
			KindSequences: KindSequences{
				"example.org/Widget": 5,
			},
		},
		stalenessIntervals: []StalenessInterval{
			{
				ReconcilerID: "WidgetReconciler",
				Kind:         "example.org/Widget",
				StaleAt:      3,
				CatchUpAt:    7,
				Lag:          -1,
			},
		},
		// stuckReconcilerPositions should be ignored when intervals are set
		stuckReconcilerPositions: map[ReconcilerID]KindSequences{
			"WidgetReconciler": {"example.org/Widget": 1},
		},
	}

	// For a reconciler not in any interval, should get all objects
	allObjs := sn.ObserveAs("OtherReconciler")
	currentObjs := sn.Contents.All()
	if len(allObjs) != len(currentObjs) {
		t.Fatalf("expected ObserveAs for non-stale reconciler to return all objects")
	}
}

package explore

import (
	"context"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestRunHarnessCLIValidatesBuilderFactory(t *testing.T) {
	err := RunHarnessCLI(context.Background(), HarnessCLIOptions{})
	if err == nil {
		t.Fatal("expected missing builder factory error")
	}
}

func TestRunHarnessBatchValidatesCompiler(t *testing.T) {
	builder := tracecheck.NewExplorerBuilder(runtime.NewScheme())
	err := runHarnessBatch(context.Background(), builder, []coverage.Input{{Name: "test"}}, HarnessCLIOptions{})
	if err == nil {
		t.Fatal("expected missing compiler error")
	}
}

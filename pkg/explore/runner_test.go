package explore

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

func TestRunnerWritesDumpContext(t *testing.T) {
	oldInteractive := *interactiveFlag
	oldDumpPath := *dumpPathFlag
	oldInputsPath := *inputsPathFlag
	oldConfigPath := *configPathFlag
	t.Cleanup(func() {
		*interactiveFlag = oldInteractive
		*dumpPathFlag = oldDumpPath
		*inputsPathFlag = oldInputsPath
		*configPathFlag = oldConfigPath
	})

	*interactiveFlag = false
	dumpPath := filepath.Join(t.TempDir(), "runner-dump.jsonl")
	*dumpPathFlag = dumpPath
	*inputsPathFlag = "/tmp/generated-inputs.json"
	*configPathFlag = "/tmp/explore-config.json"

	builder, state := newTestBuilder(t)
	runner, err := NewRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	if err := runner.Run(context.Background(), RunInput{EnvironmentState: state}); err != nil {
		t.Fatalf("run runner: %v", err)
	}

	dump, err := analysis.LoadDump(dumpPath)
	if err != nil {
		t.Fatalf("load dump: %v", err)
	}
	if dump.Context == nil || dump.Context.Scenario == nil {
		t.Fatalf("expected dump context to be written")
	}
	if dump.Context.Scenario.Name != "standalone" {
		t.Fatalf("expected standalone scenario name, got %q", dump.Context.Scenario.Name)
	}
	if dump.Context.Scenario.RunIndex == nil || *dump.Context.Scenario.RunIndex != 0 {
		t.Fatalf("expected run index 0 for standalone run")
	}
	if dump.Context.Scenario.InputRef != "/tmp/generated-inputs.json" {
		t.Fatalf("expected input ref in dump context, got %q", dump.Context.Scenario.InputRef)
	}
	if dump.Context.Scenario.Workflow != "standalone" {
		t.Fatalf("expected standalone workflow, got %q", dump.Context.Scenario.Workflow)
	}
	if dump.Context.Scenario.Attributes["exploreConfig"] != "/tmp/explore-config.json" {
		t.Fatalf("expected explore config attribute in dump context")
	}
}

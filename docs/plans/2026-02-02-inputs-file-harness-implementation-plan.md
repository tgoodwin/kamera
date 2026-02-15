# Inputs File Harness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let project-specific harnesses accept `--inputs <file>` containing `coverage.Input` and run all scenarios via `ParallelRunner`.

**Architecture:** Add a shared `--inputs` flag, a loader in `pkg/coverage`, and batch-mode branches in each harness that load inputs, convert to `explore.Scenario`, and execute `ParallelRunner` with dump/stats directories. Batch mode ignores the interactive inspector.

**Tech Stack:** Go 1.24+, stdlib `flag/json/os`, existing `pkg/coverage` and `pkg/explore`.

## Assumptions
- The `Input -> Scenario` conversion is not implemented yet; batch mode should return a clear error until that conversion is provided (hooked via a harness-specific `scenariosFromInputs` function).

---

### Task 1: Add an inputs file loader in coverage

**Files:**
- Create: `pkg/coverage/inputs.go`
- Create: `pkg/coverage/inputs_test.go`

**Step 1: Write the failing tests**

```go
func TestLoadInputsOK(t *testing.T) {
	inputs := []Input{{Name: "case-1"}}
	path := writeInputsFile(t, inputs)

	got, err := LoadInputs(path)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "case-1", got[0].Name)
}

func TestLoadInputsBadJSON(t *testing.T) {
	path := writeRawFile(t, []byte("{not json"))
	_, err := LoadInputs(path)
	require.Error(t, err)
}

func TestLoadInputsEmpty(t *testing.T) {
	path := writeInputsFile(t, []Input{})
	_, err := LoadInputs(path)
	require.Error(t, err)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/coverage -run TestLoadInputs`
Expected: FAIL with "undefined: LoadInputs"

**Step 3: Write minimal implementation**

```go
func LoadInputs(path string) ([]Input, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read inputs: %w", err)
	}
	var inputs []Input
	if err := json.Unmarshal(data, &inputs); err != nil {
		return nil, fmt.Errorf("parse inputs: %w", err)
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("inputs file contains no scenarios")
	}
	return inputs, nil
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/coverage -run TestLoadInputs`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/coverage/inputs.go pkg/coverage/inputs_test.go
git commit -m "add coverage inputs loader"
```

---

### Task 2: Add shared --inputs flag in explore

**Files:**
- Modify: `pkg/explore/flags.go`
- Create: `pkg/explore/flags_test.go`

**Step 1: Write the failing test**

```go
func TestInputsPathDefault(t *testing.T) {
	if InputsPath() != "" {
		t.Fatalf("expected empty inputs path by default")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/explore -run TestInputsPathDefault`
Expected: FAIL with "undefined: InputsPath"

**Step 3: Write minimal implementation**

```go
inputsPathFlag = flag.String("inputs", "", "path to input JSON file")

func InputsPath() string {
	return *inputsPathFlag
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/explore -run TestInputsPathDefault`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/explore/flags.go pkg/explore/flags_test.go
git commit -m "add explore inputs flag"
```

---

### Task 3: Wire batch mode in knative-serving harness

**Files:**
- Modify: `examples/knative-serving/main.go`
- (Optional) Modify: `examples/knative-serving/scenario.go` (add stub converter)

**Step 1: Write the failing test**

Add a small compile check via `go test` for the module after code changes.

**Step 2: Run test to verify it fails**

Run: `go test ./examples/knative-serving/...`
Expected: FAIL until new imports/logic compile cleanly

**Step 3: Write minimal implementation**

Add a batch-mode branch after config load:

```go
if inputsPath := explore.InputsPath(); inputsPath != "" {
	if explore.InteractiveEnabled() {
		fmt.Fprintln(os.Stderr, "interactive ignored in batch mode")
	}
	inputs, err := coverage.LoadInputs(inputsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load inputs: %v\n", err)
		os.Exit(1)
	}
	scenarios, err := scenariosFromInputs(builder, inputs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "convert inputs: %v\n", err)
		os.Exit(1)
	}
	runner, err := explore.NewParallelRunner(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
		os.Exit(1)
	}
	opts := explore.ParallelOptions{DumpDir: explore.DumpPath(), StatsDir: explore.DumpStatsPath()}
	if _, err := runner.RunAll(ctx, scenarios, opts); err != nil {
		fmt.Fprintf(os.Stderr, "batch run error: %v\n", err)
		os.Exit(1)
	}
	return
}
```

Define a stub converter in `scenario.go` (or a new file) that returns a clear error
until the Input->Scenario conversion is implemented.

**Step 4: Run test to verify it passes**

Run: `go test ./examples/knative-serving/...`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/knative-serving/main.go examples/knative-serving/scenario.go
git commit -m "add inputs flag to knative harness"
```

---

### Task 4: Wire batch mode in crossplane and karpenter harnesses

**Files:**
- Modify: `examples/crossplane/main.go`
- Modify: `examples/karpenter/main.go`

**Step 1: Write the failing test**

Use module-level compile tests once code is added.

**Step 2: Run test to verify it fails**

Run: `go test ./examples/crossplane/...`
Expected: FAIL until new imports/logic compile cleanly

Run: `go test ./examples/karpenter/...`
Expected: FAIL until new imports/logic compile cleanly

**Step 3: Write minimal implementation**

Apply the same batch-mode branch and stub `scenariosFromInputs` helper (if needed)
inside each harness.

**Step 4: Run test to verify it passes**

Run: `go test ./examples/crossplane/...`
Expected: PASS

Run: `go test ./examples/karpenter/...`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/crossplane/main.go examples/karpenter/main.go
git commit -m "add inputs flag to crossplane and karpenter harnesses"
```

---

### Task 5: Wire batch mode in kratix harness

**Files:**
- Modify: `examples/kratix/main.go`

**Step 1: Write the failing test**

Use module-level compile tests once code is added.

**Step 2: Run test to verify it fails**

Run: `go test ./examples/kratix/...`
Expected: FAIL until new imports/logic compile cleanly

**Step 3: Write minimal implementation**

Add the same batch-mode branch after flow selection and config loading,
reusing the stub `scenariosFromInputs` function in the kratix example.

**Step 4: Run test to verify it passes**

Run: `go test ./examples/kratix/...`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/kratix/main.go
git commit -m "add inputs flag to kratix harness"
```

---

### Task 6: Document the new flag in example READMEs

**Files:**
- Modify: `examples/knative-serving/README.md`
- Modify: `examples/crossplane/README.md`
- Modify: `examples/karpenter/README.md`
- Modify: `examples/kratix/README.md`

**Step 1: Write the failing test**

Not applicable; doc-only change.

**Step 2: Make the change**

Add a short "Batch inputs" snippet showing `--inputs`, `--dump-output`, and
`--dump-stats` usage.

**Step 3: Commit**

```bash
git add examples/knative-serving/README.md examples/crossplane/README.md examples/karpenter/README.md examples/kratix/README.md
git commit -m "document inputs flag usage"
```

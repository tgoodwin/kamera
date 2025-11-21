# Repository Guidelines

## Project Structure & Module Organization
- `cmd/`: CLI entrypoints for trace analysis (`analyze`, `collect`), deterministic builds (`determinize`), inspection (`inspect`), and sleeve demos.
- `pkg/`: core libraries (`tracecheck`, `replay`, `interactive`, `tracegen`, `diff`, `util`, etc.) with tests beside implementations.
- `internal/`: helper wiring for CLIs and lenses; `mocks/` holds generated doubles.
- `examples/`: runnable scenarios such as `examples/knative-serving`; `visualizer/` and `analysis/` contain small scripts for rendering and post-processing traces.
- `tracestore/` provides MinIO manifests for trace storage; `webhook/` and `sleevectrl/` host sample components with Dockerfiles and manifests.

## Build, Test, and Development Commands
```bash
make test                     # runs go test ./... across the repo
go test ./pkg/...             # fast inner-loop unit tests for core libs
make determinize              # builds ./cmd/determinize into ./bin
go run ./cmd/inspect --dump <file>   # open a saved inspector dump
cd examples/knative-serving && go run .   # launch the Knative demo
```
Use Go 1.24+. After dependency changes run `go mod tidy`; before review run `go fmt ./...` (or `gofmt -w`) to keep imports and spacing clean.

## Coding Style & Naming Conventions
- Standard Go formatting; group imports stdlib/third-party/module-local and avoid unused aliases.
- Package names stay short and lower case; exported identifiers follow Go naming and add brief doc comments when public.
- Keep CLIs thin: place business logic in `pkg/` and unit-test it there; prefer small helpers over wide structs.
- Never commit kubeconfigs, credentials, or cluster dumps; rely on env vars or local overrides.

## Testing Guidelines
- Default suite uses `go test`; many packages use Ginkgo/Gomega (`sleevectrl/pkg`, `sleevectrl/test/e2e`).
- E2E under `sleevectrl/test/e2e` expects `kubectl`, Docker/Kind, and a reachable cluster; keep installs idempotent and clean up resources.
- Name tests with clear `It(...)`/`Describe(...)` blocks or `TestXxx` functions; table-driven cases are preferred in core libraries.
- When changing trace transformation/explorer code, add deterministic fixtures in `pkg/tracecheck` or `pkg/replay` to guard against path regressions.

## Commit & Pull Request Guidelines
- Match the existing concise style: imperative, lower-case subjects such as `update file structure for determinize`; wrap bodies near 72 chars and add a short Why/What when non-obvious.
- PRs should include scope summary, linked issues, tests run (`make test`, targeted `go test`, e2e notes), and artifacts that help reviewers (trace dumps, inspector screenshots).
- Keep PRs focused; split unrelated refactors. Document behavior changes in README sections or inline godoc comments before requesting review.

# Real-cluster Sieve baselines for Table 6

This optional workflow reruns the 11 Table 6 comparison rows with Sieve itself.
Unlike Kamera, Sieve creates a real kind cluster, loads an instrumented
controller and Kubernetes node image, executes the fault plan, runs its oracle,
and tears the cluster down. The `duration` stored in Sieve's result JSON covers
that complete operation; it is the measurement boundary used by the Sieve
column in Table 6.

These runs are not part of the short Kamera reproduction path. The 11 reported
Sieve durations total 4,719 seconds (about 79 minutes); budget approximately
1.5–2 hours for a complete run with setup and cold-cache overhead, and use a
dedicated machine or VM.

## Important safety and platform notes

- Sieve creates and deletes the default kind cluster named `kind`. Do not run
  it on a host where that cluster contains work you need. Check first with
  `kind get clusters`.
- Linux x86-64 can use Sieve's upstream images. Apple Silicon can run natively
  with the checked-in compatibility patch and the arm64 images listed below;
  Rosetta is not required for that path.
- The closest published environment uses Python 3.7, Go 1.19, kind 0.13.0,
  Docker, kubectl, Helm 3, and a configured `GOPATH` and `KUBECONFIG`.
- Sieve's upstream prebuilt images are hosted at
  `ghcr.io/sieve-project/action`. Authenticate with a GitHub token that can
  read packages if anonymous pulls are rejected. The Apple Silicon path uses
  the public `docker.io/tlg2132` images instead.
- Absolute times vary with image-cache state, registry latency, and host
  resources. Compare the produced result JSON rather than expecting exact
  second-for-second agreement.

## 1. Install and check the Sieve environment

Install Docker, kubectl, Helm 3, and kind 0.13.0. Then set explicit paths:

```bash
export GOPATH="${GOPATH:-$HOME/go}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
```

Clone the upstream Sieve source at the revision audited for this guide:

```bash
git clone https://github.com/sieve-project/sieve.git
cd sieve
git checkout 6c97abeb79e644fa5eda889a2c174b2436dbc264
```

On Apple Silicon, create an isolated native Python 3.11 environment and install
the artifact's reproduction-only requirements. They retain Sieve's runtime
pins, update PyYAML for modern Python, constrain `requests` and `urllib3` for
Sieve's Docker SDK, and omit `pysqlite3`, which Sieve uses only in learning
mode:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install \
  -r /path/to/kamera/artifact/sieve/requirements-reproduce.txt
python check_env.py
```

On Linux x86-64, Python 3.7 with `pip install -r requirements.txt` remains the
closest configuration to Sieve's original workflow. It is not the recommended
Apple Silicon route: Python 3.7 has no native arm64 build, and installing an
Intel build adds Rosetta without avoiding Sieve's other compatibility fixes.
The native Python 3.11 environment above was validated by this artifact
workflow.

On newer Go releases, `check_env.py` may warn while parsing `go env` entries
whose values contain `=`. If `go version` succeeds and `GOPATH` is exported,
that parser warning does not prevent reproduction.

If the registry requires authentication:

```bash
printf '%s' "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_USER" --password-stdin
```

Use Sieve's longer workload timeout, matching its bug-reproduction workflow:

```bash
printf '%s\n' '{"workload_conditional_wait_timeout": 1000}' > sieve_config.json
```

Confirm that Docker is running and that no existing cluster is named `kind`:

```bash
docker info
kind get clusters
```

### Apple Silicon setup

The repository includes a small patch that uses PyYAML's supported safe loader,
makes Sieve's server build target configurable, and lets the caller select an
instrumented kind node image. It also matches Sieve's bundled v4.1.1 hostpath
CSI driver with the corresponding snapshot CRDs, uses the artifact's vendored
CSI manifests instead of GitHub Raw, and fails before changing the default
StorageClass if CSI setup is incomplete. Apply it from the Sieve checkout,
replacing `/path/to/kamera` with the artifact checkout:

```bash
git apply /path/to/kamera/artifact/sieve/apple-silicon.patch
export SIEVE_SERVER_GOARCH=arm64
export SIEVE_KIND_IMAGE=docker.io/tlg2132/node:v1.24.10-macos-test
export KAMERA_AE_SIEVE_REGISTRY=docker.io/tlg2132
```

Pull the complete native image set before running:

```bash
docker pull docker.io/tlg2132/node:v1.24.10-macos-test
docker pull docker.io/tlg2132/zookeeper-operator:test
docker pull docker.io/tlg2132/rabbitmq-operator:test
docker pull docker.io/tlg2132/cass-operator:test
```

All four images are `linux/arm64`. Their published manifest digests are:

| Image | Manifest digest |
|---|---|
| `node:v1.24.10-macos-test` | `sha256:a6807c9f2cfeaffef2fee755374ced339ce9433a7929c9bbd2f453a6bdc1e991` |
| `zookeeper-operator:test` | `sha256:208a0a00fa73216bd816082d3c809d2cb611e90271629c0b906a4479ac91663b` |
| `rabbitmq-operator:test` | `sha256:8472f7abb71b5e8cb55431348de34ce4b899157ff0c848f64bd95f5b785e7a1e` |
| `cass-operator:test` | `sha256:16fccdbcb912ea289e9b54e66b8e91f26589510939eb0c5ad29f40aca89d9089` |

Keep the three exported variables set when invoking the Kamera wrapper. The
wrapper automatically sets `SIEVE_CSI_MANIFEST_DIR` to its vendored manifest
directory. If invoking Sieve directly, also export its absolute path:

```bash
export SIEVE_CSI_MANIFEST_DIR=/absolute/path/to/kamera/artifact/sieve/manifests
```

The patch changes no fault plans or oracles. The ten CSI manifests are pinned
and attributed in
[`artifact/sieve/manifests`](manifests/README.md); the CSI installer makes no
GitHub Raw requests when this directory is set.

## 2. Optional: validate the environment with one row

From the Kamera checkout, run RabbitMQ intermediate-state-1 through the same
wrapper used for the full matrix. This row also checks the CSI setup:

```bash
./artifact/run-sieve-baselines.sh \
  --only rmq/intermediate-state-1 \
  /absolute/path/to/sieve
```

Sieve should finish with `reproduced=True` and report that the RabbitMQ PVC
remained at `10Gi` instead of the requested `15Gi`. It writes
`bug_reproduction_stats.tsv` and places the detailed JSON under
`sieve_test_results/`; the result JSON contains both `duration` and
`number_errors`. Additional schema differences are expected when the arm64
Kubernetes 1.24 image is compared with Sieve's stored reference state, so use
the PVC mismatch—not the raw inconsistency count—as the row-specific check.

This is only an environment check. It does not replace the complete 11-row
command in the next section.

Each invocation replaces `sieve_test_results`, so copy results before starting
another row. The Kamera wrapper below does that automatically.

## 3. Run the complete 11-row Table 6 matrix

This is the command that reproduces every Sieve result represented in Table 6.
Return to the Kamera checkout and pass the Sieve checkout path to:

```bash
./artifact/run-sieve-baselines.sh /absolute/path/to/sieve
```

To select an output directory:

```bash
./artifact/run-sieve-baselines.sh \
  /absolute/path/to/sieve \
  artifact-results/table6-sieve
```

The wrapper invokes Sieve's own `reproduce_bugs.py` once per row. It does not
simulate Sieve or substitute Kamera. It copies every result before Sieve clears
its working output and writes `table6-sieve.tsv` with the paper time, observed
Sieve duration, reproduced flag, error count, and preserved result path.
With no `--only` option, the wrapper always runs all 11 rows: four ZooKeeper,
four RabbitMQ, and three Cassandra bugs. Use `--only` solely for setup checks,
reruns, or diagnosis of a particular row.

Inspect the exact commands without creating a cluster:

```bash
./artifact/run-sieve-baselines.sh --dry-run /absolute/path/to/sieve
```

The complete mapping is:

| Table 6 row | Sieve controller and bug | Pinned Sieve test plan |
|---|---|---|
| `zk/stale-state-1` | `zookeeper-operator`, `stale-state-1` | `zookeeper-operator-stale-state-1.yaml` |
| `zk/stale-state-2` | `zookeeper-operator`, `stale-state-2` | `zookeeper-operator-stale-state-2.yaml` |
| `zk/unobserved-state-1` | `zookeeper-operator`, `unobserved-state-1` | `zookeeper-operator-unobserved-state-1.yaml` |
| `zk/indirect-1` | `zookeeper-operator`, `indirect-1` | `zookeeper-operator-indirect-1.yaml` |
| `rmq/stale-state-1` | `rabbitmq-operator`, `stale-state-1` | `rabbitmq-operator-stale-state-1.yaml` |
| `rmq/stale-state-2` | `rabbitmq-operator`, `stale-state-2` | `rabbitmq-operator-stale-state-2.yaml` |
| `rmq/unobserved-state-1` | `rabbitmq-operator`, `unobserved-state-1` | `rabbitmq-operator-unobserved-state-1.yaml` |
| `rmq/intermediate-state-1` | `rabbitmq-operator`, `intermediate-state-1` | `rabbitmq-operator-intermediate-state-1.yaml` |
| `cass/stale-state-1` | `cass-operator`, `stale-state-1` | `cass-operator-stale-state-1.yaml` |
| `cass/intermediate-state-1` | `cass-operator`, `intermediate-state-1` | `cass-operator-intermediate-state-1.yaml` |
| `cass/intermediate-state-2` | `cass-operator`, `intermediate-state-2` | `cass-operator-intermediate-state-2.yaml` |

This mapping was audited against both `artifact/run-table6.sh` and the pinned
Sieve revision's `reprod_map`: all 11 Table 6 IDs match, and every referenced
test-plan file is present. The three published arm64 controller images cover
all rows because the rows use only these three controller families.

## Troubleshooting

- If cluster creation fails, run `kind delete cluster --name kind`, confirm the
  Docker daemon has enough CPU and memory, and retry that row.
- If CSI setup fails, confirm `SIEVE_CSI_MANIFEST_DIR` is an absolute path to
  `artifact/sieve/manifests`. The patched installer checks all ten files and
  waits for the hostpath and snapshot-controller workloads before changing the
  default StorageClass.
- An image-pull denial means registry authentication is missing; log in to
  `ghcr.io` and retry.
- Preserve `sieve_test_results`, `bug_reproduction_stats.tsv`, the wrapper log,
  and `table6-sieve.tsv` when reporting a failure.
- Sieve's `reproduced=True` means its oracle detected an error or inconsistency.
  Read `detected_errors` in the copied JSON to confirm the row-specific symptom;
  older Kubernetes objects can also yield incidental schema differences on a
  ported environment.

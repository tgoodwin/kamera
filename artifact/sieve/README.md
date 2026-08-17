# Real-cluster Sieve baselines for Table 6

This optional workflow reruns the 11 Table 6 comparison rows with Sieve itself.
Unlike Kamera, Sieve creates a real kind cluster, loads an instrumented
controller and Kubernetes node image, executes the fault plan, runs its oracle,
and tears the cluster down. The `duration` stored in Sieve's result JSON covers
that complete operation; it is the measurement boundary used by the Sieve
column in Table 6.

These runs are not part of the short Kamera reproduction path. Budget several
hours for all 11 rows and use a dedicated machine or VM.

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

Create an isolated Python environment. Python 3.7 is the version in Sieve's
bug-reproduction workflow; newer Python releases may not build the pinned
packages in `requirements.txt`.

```bash
python3.7 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python check_env.py
```

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

The repository includes a small patch that makes Sieve's server build target
configurable and lets the caller select an instrumented kind node image. Apply
it from the Sieve checkout, replacing `/path/to/kamera` with the artifact
checkout:

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

Keep the three exported variables set when invoking either Sieve directly or
the Kamera wrapper. The patch changes no fault plans or oracles.

## 2. Kick the tires with one row

From the Sieve checkout, run RabbitMQ intermediate-state-1:

```bash
python3 reproduce_bugs.py \
  -c rabbitmq-operator \
  -b intermediate-state-1 \
  -r "${KAMERA_AE_SIEVE_REGISTRY:-ghcr.io/sieve-project/action}"
```

Sieve should report the PVC storage mismatch, write
`bug_reproduction_stats.tsv`, and place the detailed JSON under
`sieve_test_results/`. The result JSON contains both `duration` and
`number_errors`.

Each invocation replaces `sieve_test_results`, so copy results before starting
another row. The Kamera wrapper below does that automatically.

## 3. Run all 11 Table 6 rows

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

Inspect the exact commands without creating a cluster:

```bash
./artifact/run-sieve-baselines.sh --dry-run /absolute/path/to/sieve
```

The row mapping is:

| Table 6 row | Sieve controller and bug |
|---|---|
| `zk/stale-state-1` | `zookeeper-operator`, `stale-state-1` |
| `zk/stale-state-2` | `zookeeper-operator`, `stale-state-2` |
| `zk/unobserved-state-1` | `zookeeper-operator`, `unobserved-state-1` |
| `zk/indirect-1` | `zookeeper-operator`, `indirect-1` |
| `rmq/stale-state-1` | `rabbitmq-operator`, `stale-state-1` |
| `rmq/stale-state-2` | `rabbitmq-operator`, `stale-state-2` |
| `rmq/unobserved-state-1` | `rabbitmq-operator`, `unobserved-state-1` |
| `rmq/intermediate-state-1` | `rabbitmq-operator`, `intermediate-state-1` |
| `cass/stale-state-1` | `cass-operator`, `stale-state-1` |
| `cass/intermediate-state-1` | `cass-operator`, `intermediate-state-1` |
| `cass/intermediate-state-2` | `cass-operator`, `intermediate-state-2` |

## Troubleshooting

- If cluster creation fails, run `kind delete cluster --name kind`, confirm the
  Docker daemon has enough CPU and memory, and retry that row.
- An image-pull denial means registry authentication is missing; log in to
  `ghcr.io` and retry.
- Preserve `sieve_test_results`, `bug_reproduction_stats.tsv`, the wrapper log,
  and `table6-sieve.tsv` when reporting a failure.
- Sieve's `reproduced=True` means its oracle detected an error or inconsistency.
  Read `detected_errors` in the copied JSON to confirm the row-specific symptom;
  older Kubernetes objects can also yield incidental schema differences on a
  ported environment.

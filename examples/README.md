# Examples

Most harnesses live in-tree under their respective subdirectories. The KCP
harness is checked in at `examples/kcp`.

The Karpenter, Kratix, and KRO harnesses exercise small simulation adapters
against pinned upstream revisions. Reconstruct those source dependencies with:

```sh
make setup-harness-deps
```

The command clones the pinned revisions into the ignored
`artifact-deps/harnesses/` directory. It applies the existing artifact patches
for Karpenter and KRO plus the checked-in Kratix harness patch. It is
idempotent and rejects a checkout whose revision or working-tree changes do
not match the recorded inputs. No machine-specific paths are required.

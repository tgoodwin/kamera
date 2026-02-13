# Dependency Analysis

The dependency-analysis workflow is now maintained as a reusable skill:

- `.agents/skills/dependency-analysis/SKILL.md`

Normative contract for artifact validity:

- `docs/design/dependency-graph-contract.md`

One-command validation:

```bash
scripts/validate-dependency-graph.sh \
  --graph dependency-graph.json \
  --schema-map schema-map.json
```

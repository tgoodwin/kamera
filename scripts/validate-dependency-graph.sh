#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate a dependency graph artifact against docs/design/dependency-graph-contract.md.

Usage:
  scripts/validate-dependency-graph.sh --graph <path> [--schema-map <path>] [--max-errors N] [--quiet]

Options:
  --graph <path>       Path to dependency-graph.json (required)
  --schema-map <path>  Optional schema-map.json for resource coverage checks
  --max-errors <N>     Maximum errors to print (default: 80; 0 = print all)
  --quiet              Only emit pass/fail summary
  -h, --help           Show this help
EOF
}

graph_path=""
schema_map_path=""
quiet=0
max_errors=80

while [[ $# -gt 0 ]]; do
  case "$1" in
    --graph)
      graph_path="${2:-}"
      shift 2
      ;;
    --schema-map)
      schema_map_path="${2:-}"
      shift 2
      ;;
    --quiet)
      quiet=1
      shift
      ;;
    --max-errors)
      max_errors="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$max_errors" =~ ^[0-9]+$ ]]; then
  echo "--max-errors must be a non-negative integer" >&2
  exit 2
fi

if [[ -z "$graph_path" ]]; then
  echo "--graph is required" >&2
  usage >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required but not found in PATH" >&2
  exit 2
fi

if [[ ! -f "$graph_path" ]]; then
  echo "graph file not found: $graph_path" >&2
  exit 2
fi

if [[ -n "$schema_map_path" && ! -f "$schema_map_path" ]]; then
  echo "schema-map file not found: $schema_map_path" >&2
  exit 2
fi

graph_jq='
def valid_role($v): (["user-facing", "supporting", "builtin"] | index($v)) != null;
def valid_trigger($v): (["primary", "secondary", "owns", "manual"] | index($v)) != null;
def valid_surface($v): (["spec", "status", "metadata", "any"] | index($v)) != null;
def keyset_ok($allowed): ((keys_unsorted | sort) == ($allowed | sort));

if type != "object" then
  ["top-level JSON must be an object"]
else
  (
    []
    + (if (.nodes | type != "array")
       then ["nodes must be an array"]
       else [] end)
    + (if (.edges | type != "array")
       then ["edges must be an array"]
       else [] end)
    + (if ((keys_unsorted | sort) != ["edges", "nodes"])
       then ["top-level keys must be exactly [nodes,edges]"]
       else [] end)

    + (if (.nodes | type == "array") then
         [ .nodes[]? as $n
           | if ($n.kind == "controller") then
               if (($n | keyset_ok(["kind", "id"])) | not)
               then "controller node has invalid keys: \($n | tojson)"
               else empty end,
               if (($n.id | type) != "string" or ($n.id | length) == 0)
               then "controller node id must be non-empty string: \($n | tojson)"
               else empty end
             elif ($n.kind == "resource") then
               if (($n | keyset_ok(["kind", "id", "gvk", "role"])) | not)
               then "resource node has invalid keys: \($n | tojson)"
               else empty end,
               if (($n.id | type) != "string" or ($n.gvk | type) != "string" or
                   ($n.id | length) == 0 or ($n.gvk | length) == 0)
               then "resource node id/gvk must be non-empty strings: \($n | tojson)"
               else empty end,
               if ($n.id != $n.gvk)
               then "resource node id must equal gvk: \($n | tojson)"
               else empty end,
               if (($n.gvk | test("^[^/]+/[^/]+/[^/]+$")) | not)
               then "resource node gvk must be group/version/kind: \($n | tojson)"
               else empty end,
               if (valid_role($n.role) | not)
               then "resource node role must be one of user-facing|supporting|builtin: \($n | tojson)"
               else empty end
             else
               "node kind must be controller|resource: \($n | tojson)"
             end
         ]
       else [] end)

    + (if (.nodes | type == "array") then
         ([ .nodes[]?.id ] as $ids
          | [ $ids[] | select(type != "string" or length == 0) ] as $bad
          | (if ($bad | length) > 0
             then ["all node ids must be non-empty strings"]
             else [] end)
            + [ $ids | group_by(.)[] | select(length > 1) | "duplicate node id: \(.[0])" ])
       else [] end)

    + (if (.edges | type == "array") then
         [ .edges[]? as $e
           | if ($e.kind == "triggers") then
               if (($e | keyset_ok(["kind", "from", "to", "trigger"])) | not)
               then "triggers edge has invalid keys: \($e | tojson)"
               else empty end,
               if (valid_trigger($e.trigger) | not)
               then "triggers edge trigger must be one of primary|secondary|owns|manual: \($e | tojson)"
               else empty end
             elif ($e.kind == "reads" or $e.kind == "writes") then
               if (($e | keyset_ok(["kind", "from", "to", "surface"])) | not)
               then "\($e.kind) edge has invalid keys: \($e | tojson)"
               else empty end,
               if (valid_surface($e.surface) | not)
               then "\($e.kind) edge surface must be one of spec|status|metadata|any: \($e | tojson)"
               else empty end
             else
               "edge kind must be triggers|reads|writes: \($e | tojson)"
             end,
             if (($e.from | type) != "string" or ($e.from | length) == 0 or
                 ($e.to | type) != "string" or ($e.to | length) == 0)
             then "edge from/to must be non-empty strings: \($e | tojson)"
             else empty end
         ]
       else [] end)

    + (if ((.nodes | type == "array") and (.edges | type == "array")) then
         (.nodes | map(select((.id | type) == "string" and (.id | length) > 0) | {(.id): .kind}) | add // {}) as $kinds
         | [ .edges[]? as $e
             | if (($kinds[$e.from] // "") != "controller")
               then "edge from must reference existing controller id: \($e | tojson)"
               else empty end,
               if (($kinds[$e.to] // "") != "resource")
               then "edge to must reference existing resource id: \($e | tojson)"
               else empty end
           ]
       else [] end)

    + (if (.edges | type == "array") then
         (
           [ [ .edges[]? | select(.kind == "triggers") | "\(.kind)|\(.from)|\(.to)|\(.trigger)" ]
             | group_by(.)[]?
             | select(length > 1)
             | "duplicate edge tuple: \(.[0])"
           ]
           +
           [ [ .edges[]? | select(.kind == "reads" or .kind == "writes") | "\(.kind)|\(.from)|\(.to)|\(.surface)" ]
             | group_by(.)[]?
             | select(length > 1)
             | "duplicate edge tuple: \(.[0])"
           ]
         )
       else [] end)

    + (if ((.nodes | type == "array") and (.edges | type == "array")) then
         [ .nodes[]? | select(.kind == "controller" and (.id | type) == "string" and (.id | length) > 0) | .id ] as $controllers
         | [ $controllers[] as $cid
             | select(([ .edges[]? | select(.kind == "triggers" and .trigger == "primary" and .from == $cid) ] | length) == 0)
             | "controller missing primary trigger edge: \(.)"
           ]
       else [] end)
  )
end
| .[]
'

schema_map_jq='
[
  if ($s | type) != "object" then
    "schema-map top-level JSON must be an object"
  else empty end,
  if (($s.mapping | type) != "object") then
    "schema-map must contain object field: mapping"
  else empty end
]
+
(if (($g | type) == "object" and ($g.nodes | type) == "array" and ($s.mapping | type) == "object") then
   [ $g.nodes[] | select(.kind == "resource") | .id | select(type == "string" and length > 0) ] as $resources
   | [ $resources[] as $rid | select(($s.mapping | has($rid)) | not) | "schema-map missing resource key: \($rid)" ]
 else [] end)
| .[]
'

errors=()

if ! graph_errors_raw="$(jq -r "$graph_jq" "$graph_path")"; then
  echo "failed to parse/validate graph JSON: $graph_path" >&2
  exit 1
fi

if [[ -n "$graph_errors_raw" ]]; then
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    errors+=("$line")
  done <<<"$graph_errors_raw"
fi

if [[ -n "$schema_map_path" ]]; then
  if ! schema_errors_raw="$(jq -n -r --argfile g "$graph_path" --argfile s "$schema_map_path" "$schema_map_jq")"; then
    echo "failed to parse/validate schema-map JSON: $schema_map_path" >&2
    exit 1
  fi
  if [[ -n "$schema_errors_raw" ]]; then
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      errors+=("$line")
    done <<<"$schema_errors_raw"
  fi
fi

if [[ ${#errors[@]} -gt 0 ]]; then
  if [[ "$quiet" -eq 0 ]]; then
    echo "FAIL: dependency graph contract validation failed (${#errors[@]} errors)." >&2
    printed=0
    for err in "${errors[@]}"; do
      if [[ "$max_errors" -gt 0 && "$printed" -ge "$max_errors" ]]; then
        break
      fi
      echo "  - $err" >&2
      printed=$((printed + 1))
    done
    if [[ "$max_errors" -gt 0 && "${#errors[@]}" -gt "$max_errors" ]]; then
      echo "  ... and $(( ${#errors[@]} - max_errors )) more (use --max-errors 0 to print all)." >&2
    fi
    echo "  Hint: run with --quiet for summary-only output." >&2
  else
    echo "FAIL (${#errors[@]} errors)" >&2
  fi
  exit 1
fi

if [[ "$quiet" -eq 0 ]]; then
  controllers="$(jq '[.nodes[] | select(.kind=="controller")] | length' "$graph_path")"
  resources="$(jq '[.nodes[] | select(.kind=="resource")] | length' "$graph_path")"
  edges="$(jq '.edges | length' "$graph_path")"
  echo "PASS: dependency graph satisfies contract."
  echo "  graph: $graph_path"
  echo "  controllers: $controllers"
  echo "  resources: $resources"
  echo "  edges: $edges"
  if [[ -n "$schema_map_path" ]]; then
    echo "  schema-map coverage: OK ($schema_map_path)"
  fi
else
  echo "PASS"
fi

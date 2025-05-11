#!/bin/bash
reconcile_prefix="$1"
file="test.jsonl"
if [[ "$1" == "--file" ]]; then
    file="$2"
    shift 2
fi

reconcile_prefix="$1"
mode="$2"

if [[ "$mode" == "events" ]]; then
    filter="rg -v operation_id"
elif [[ "$mode" == "records" ]]; then
    filter="rg operation_id | jq 'del(.. | .managedFields?)'"
else
    filter="cat"
fi

cat "$file" | rg "\"reconcile_id\":\"${reconcile_prefix}[^\"]*\"" | eval $filter | jq

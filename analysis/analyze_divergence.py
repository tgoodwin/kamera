#!/usr/bin/env python3
import json
import sys
import re

def get_object_by_hash(objects, hash_value):
    """Find object in the dump by its hash value"""
    for obj in objects:
        if obj['hash']['Value'] == hash_value:
            return obj['object']
    return None

def extract_conditions(obj):
    """Extract status.conditions from a k8s object"""
    if not obj:
        return []
    status = obj.get('status', {})
    conditions = status.get('conditions', [])
    return conditions

def analyze_divergence(dump_path):
    with open(dump_path) as f:
        d = json.load(f)

    print(f"Number of objects: {len(d['objects'])}")
    print(f"Number of states: {len(d['states'])}")

    if len(d['states']) < 2:
        print("Only one converged state - no divergence to analyze")
        return

    path0 = d['states'][0]['paths'][0]
    path1 = d['states'][1]['paths'][0]

    print(f"\nPath 0 length: {len(path0)}")
    print(f"Path 1 length: {len(path1)}")

    # Find first divergence point
    min_len = min(len(path0), len(path1))
    divergence_idx = None
    for i in range(min_len):
        if path0[i]['stateAfter'] != path1[i]['stateAfter']:
            divergence_idx = i
            break

    if divergence_idx is not None:
        print(f"\n=== DIVERGENCE FOUND AT STEP {divergence_idx} ===")
        print(f"Path 0 stateBefore: {path0[divergence_idx]['stateBefore']}")
        print(f"Path 1 stateBefore: {path1[divergence_idx]['stateBefore']}")
        print(f"Path 0 stateAfter: {path0[divergence_idx]['stateAfter']}")
        print(f"Path 1 stateAfter: {path1[divergence_idx]['stateAfter']}")
        print(f"Path 0 controllerId: {path0[divergence_idx]['controllerId']}")
        print(f"Path 1 controllerId: {path1[divergence_idx]['controllerId']}")

        # Check if stateBefore matches
        if path0[divergence_idx]['stateBefore'] == path1[divergence_idx]['stateBefore']:
            print("\n*** SAME STARTING STATE, DIFFERENT OUTPUTS ***")
            print("This is nondeterminism in the reconciler itself!")

        # Show what changed in each path
        print(f"\n--- Path 0 changes at divergence point ---")
        for change in path0[divergence_idx].get('changes', []):
            print(f"  {change}")

        print(f"\n--- Path 1 changes at divergence point ---")
        for change in path1[divergence_idx].get('changes', []):
            print(f"  {change}")

        # Show deltas
        print(f"\n--- Path 0 deltas at divergence point ---")
        deltas0 = path0[divergence_idx].get('deltas', [])
        print(f"  Number of deltas: {len(deltas0)}")
        for delta in deltas0[:5]:
            print(f"  {json.dumps(delta, indent=4)}")

        print(f"\n--- Path 1 deltas at divergence point ---")
        deltas1 = path1[divergence_idx].get('deltas', [])
        print(f"  Number of deltas: {len(deltas1)}")
        for delta in deltas1[:5]:
            print(f"  {json.dumps(delta, indent=4)}")

        # Extract and compare conditions from diffs
        print("\n=== CONDITION COMPARISON ===")
        for i, (d0, d1) in enumerate(zip(deltas0, deltas1)):
            if d0['key'] == d1['key']:
                diff0 = d0['value']
                diff1 = d1['value']

                # Find conditions in diff
                import re
                cond_pattern = r'type: (\w+)'
                conds0 = re.findall(cond_pattern, diff0)
                conds1 = re.findall(cond_pattern, diff1)

                print(f"\nObject: {d0['key']['Name']}")
                print(f"  Path 0 conditions: {conds0}")
                print(f"  Path 1 conditions: {conds1}")
                print(f"  Difference: Path 0 has {set(conds0) - set(conds1)}, Path 1 has {set(conds1) - set(conds0)}")

        # Look at subsequent steps to see cascading effects
        print("\n=== CASCADE ANALYSIS ===")
        print("Tracking how divergence cascades through subsequent steps...")
        for offset in range(1, min(5, min(len(path0), len(path1)) - divergence_idx)):
            idx = divergence_idx + offset
            ctrl0 = path0[idx]['controllerId']
            ctrl1 = path1[idx]['controllerId']
            state0 = path0[idx]['stateAfter']
            state1 = path1[idx]['stateAfter']

            print(f"\n  Step {idx}:")
            print(f"    Path 0 controller: {ctrl0}")
            print(f"    Path 1 controller: {ctrl1}")
            print(f"    States match: {state0 == state1}")

        # Extract and compare actual Revision objects
        print("\n=== ACTUAL OBJECT COMPARISON ===")
        objects = d['objects']

        # Find the Revision object hashes from the deltas
        for delta in deltas0:
            if 'kamera-test' in delta['key'].get('Name', ''):
                # Find the stateAfter entry for this object
                for entry in path0[divergence_idx]['stateAfter']:
                    if entry['key'].get('Name') == 'kamera-test':
                        hash0 = entry['hash']['Value']
                        obj0 = get_object_by_hash(objects, hash0)
                        if obj0:
                            conds0 = extract_conditions(obj0)
                            print(f"\nPath 0 Revision conditions ({len(conds0)}):")
                            for c in conds0:
                                print(f"  - {c.get('type')}: {c.get('status')} (reason: {c.get('reason', 'N/A')})")
                        break

        for delta in deltas1:
            if 'kamera-test' in delta['key'].get('Name', ''):
                for entry in path1[divergence_idx]['stateAfter']:
                    if entry['key'].get('Name') == 'kamera-test':
                        hash1 = entry['hash']['Value']
                        obj1 = get_object_by_hash(objects, hash1)
                        if obj1:
                            conds1 = extract_conditions(obj1)
                            print(f"\nPath 1 Revision conditions ({len(conds1)}):")
                            for c in conds1:
                                print(f"  - {c.get('type')}: {c.get('status')} (reason: {c.get('reason', 'N/A')})")
                        break
    else:
        print("\nNo divergence found in shared prefix")
        # Check if paths diverge after shared prefix
        if len(path0) != len(path1):
            print(f"Paths have different lengths: {len(path0)} vs {len(path1)}")

if __name__ == "__main__":
    dump_path = sys.argv[1] if len(sys.argv) > 1 else "/Users/tgoodwin/projects/kamera/analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl"
    analyze_divergence(dump_path)

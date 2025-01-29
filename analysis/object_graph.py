import json
from dataclasses import dataclass
from collections import defaultdict
from graphviz import Digraph
from colors import assign_colors_to_ids
from log2json import strip_logtype_from_lines
from versionmapper import process as version_process

SLEEVE_LOG_KEYWORD = "sleevelog"
EVENT_KEYWORD = "sleeve:controller-operation"
RECORD_KEYWORD = "sleeve:object-version"

READ_OPERATIONS = {"GET", "LIST"}
WRITE_OPERATIONS = {"CREATE", "PATCH", "UPDATE", "DELETE"}

def shorter(id):
    try:
        return id.split("-")[0]
    except:
        return id

def graph(data, versionmap):
    # Assign colors to reconcile IDs
    colors = assign_colors_to_ids([key for key in data.keys() if len(data[key]["writeset"]) > 0])
    dot = Digraph(comment='Event Graph2')

    # Add nodes and edges
    for reconcile_id, rw in data.items():
        for read_event in rw["readset"]:
            version = versionmap.get(read_event.id())
            if version:
                annotations = version.status_conditions()
                subtitle = f'\\n{annotations}' if annotations else ''
                dot.node(read_event.id(), f'{read_event.kind}:{shorter(read_event.object_id)}@{shorter(read_event.causal_id)}{subtitle}')
            else:
                print(f"Missing version for read event: {read_event.id()}")
                dot.node(read_event.id(), f'{read_event.kind}:{shorter(read_event.object_id)}@{shorter(read_event.causal_id)}')
        for write_event in rw["writeset"]:
            dot.node(write_event.id(), f'{write_event.kind}:{shorter(write_event.object_id)}@{shorter(write_event.causal_id)}')
            for read_event in rw["readset"]:
                dot.edge(read_event.id(), write_event.id(), label=shorter(reconcile_id), color=colors[reconcile_id], penwidth='2')

    # Add legend as a table
    legend_table = '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">'
    legend_table += '<TR><TD><B>Controller Reconciles</B></TD><TD><B>Color</B></TD></TR>'
    for reconcile_id, color in colors.items():
        if len(data[reconcile_id]["writeset"]) > 0:
            controller_id = data[reconcile_id]["writeset"][0].controller_id
            legend_table += f'<TR><TD>{controller_id}-{shorter(reconcile_id)}</TD><TD BGCOLOR="{color}"></TD></TR>'
    legend_table += '</TABLE>>'

    dot.node('legend', legend_table, shape='plaintext')

    dot.render(args.outfile, format='png', view=True)


@dataclass
class Event:
    causal_id: str
    timestamp: str
    reconcile_id: str
    controller_id: str
    root_event_id: str
    op_type: str
    kind: str
    object_id: str
    version: str
    labels: dict = None

    @classmethod
    def from_json(self, event_json):
        obj = json.loads(event_json)
        # get every key in the dictionary that starts with the prefix "label:discrete.events"
        labels = {
            k[len("label:discrete.events/") :]: v
            for k, v in obj.items()
            if k.startswith("label:discrete.events/")
        }

        # causal_id is change-id unless change-id is unset, then it is root-event-id
        causal_id = labels.get("change-id") or obj.get("label:tracey-uid", None)
        if not causal_id:
            print("oops missing causal id")
            # raise ValueError(f"Missing change-id and root-event-id labels: {obj}")

        # causal_id = obj.get("kind") + "/" + causal_id

        return Event(
            causal_id=causal_id,
            timestamp=obj.get("timestamp"),
            reconcile_id=obj.get("reconcile_id"),
            controller_id=obj.get("controller_id"),
            root_event_id=obj.get("root_event_id"),
            op_type=obj.get("op_type"),
            kind=obj.get("kind"),
            object_id=obj.get("object_id"),
            version=obj.get("version"),
            labels=labels,
        )

    def id(self):
        return f'{self.kind}/{self.causal_id}'

    def label(self, key):
        return self.labels.get(key)

    def __repr__(self) -> str:
        obj_id_short = self.object_id.split("-")[0]
        return f"Object({self.kind}, id={obj_id_short}, version={self.version} causal-id={self.causal_id}, op={self.op_type})"


def backfill_labels(events):
    """
    Events with CREATE op type are missing version, kind, and object_id.
    The 'full' object may exist within a later read event.
    We can correlate the CREATE event with the later read event by the 'change-id' label.
    So, we can backfill the missing fields from the read events.
    """
    read_events = [e for e in events if e.op_type in READ_OPERATIONS]
    by_change_id = {}
    for event in read_events:
        change_id = event.label("change-id")
        if not change_id:
            event.change_id = event.label("root-event-id")
        by_change_id[change_id] = event

    create_events = [e for e in events if e.op_type in WRITE_OPERATIONS]
    for create_event in create_events:
        change_id = create_event.label("change-id")
        if change_id not in by_change_id:
            print(f"Missing read event for CREATE event: {create_event}")
            # there could be a create event that gets clobbered by an update event
            # which is kind of weird, but it technically could happen
            continue
            # raise RuntimeError(f"Missing read event for CREATE event: {create_event}")

        read_event = by_change_id[change_id]
        create_event.kind = read_event.kind
        create_event.object_id = read_event.object_id
        create_event.version = read_event.version

    return events


def analyze(lines, versions):
    by_reconcile_id = defaultdict(list)
    events = []
    for line in lines:
        # validate the line is a JSON object
        if not line.startswith("{"):
            continue
            # raise ValueError(f"Invalid JSON object: {line}")

        # parse the JSON object
        event = Event.from_json(line)
        events.append(event)

    backfill_labels(events)
    for event in events:
        reconcile_id = event.reconcile_id
        by_reconcile_id[reconcile_id].append(event)

    # sanity checks
    for reconcile_id, events in by_reconcile_id.items():
        read_events = [e for e in events if e.op_type in READ_OPERATIONS]
        write_events = [e for e in events if e.op_type in WRITE_OPERATIONS]
        assert len(read_events) + len(write_events) == len(events)
        assert len(set([e.controller_id for e in events])) == 1

    reads_to_writes = readsets_to_writesets(by_reconcile_id)
    for reconcile_id, rw in reads_to_writes.items():
        if len(rw["writeset"]) == 0:
            continue

        print("---")
        print(f"Reconcile ID: {reconcile_id}, Controller: {rw['writeset'][0].controller_id}")
        print("Readset:")
        for event in rw["readset"]:
            print(f"\t{event}")
        print("Writeset:")
        for event in rw["writeset"]:
            print(f"\t{event}")

    graph(reads_to_writes, versions)


def readsets_to_writesets(events_by_reconcile_id):
    out = {}
    for reconcile_id, events in events_by_reconcile_id.items():
        readset = [e for e in events if e.op_type in READ_OPERATIONS]
        writeset = [e for e in events if e.op_type in WRITE_OPERATIONS]
        assert len(readset) + len(writeset) == len(events)
        out[reconcile_id] = {"readset": readset, "writeset": writeset}

    return out


def process_raw_logs(lines):
    # first, separate out controller observations
    # log lines from our instrumentation
    content = [line for line in lines if SLEEVE_LOG_KEYWORD in line]
    content = [line.split(SLEEVE_LOG_KEYWORD)[1].strip() for line in content]

    # split into conroller-operation and object-version
    controller_ops = [line for line in content if EVENT_KEYWORD in line]
    object_versions = [line for line in content if RECORD_KEYWORD in line]

    controller_ops = strip_logtype_from_lines(controller_ops)
    object_versions = strip_logtype_from_lines(object_versions)
    process_json(controller_ops, object_versions)


def process_json(controller_ops, object_versions):
    versions = version_process(object_versions)
    analyze(controller_ops, versions)


def main():
    with open(args.logfile, "r") as infile:
        lines = infile.readlines()
        if args.mode == "raw":
            process_raw_logs(lines)
        else:
            controller_ops = [line for line in lines if "op_type" in line]
            versions = [line for line in lines if "op_type" not in line]
            process_json(controller_ops, versions)

    return 0

if __name__ == "__main__":
    import sys
    import time
    import argparse

    parser = argparse.ArgumentParser(description="Process sleeve log files into an event graph.")
    parser.add_argument("logfile", help="Path to the log file")
    parser.add_argument("--outfile", help="Name of the output file", default=f'event_graph-{int(time.time())}')
    parser.add_argument("--mode", choices=["raw", "synthesized"], help="Mode of processing the log file", default="raw")
    args = parser.parse_args()

    sys.exit(main())

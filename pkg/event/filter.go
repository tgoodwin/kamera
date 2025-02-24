package event

func FilterReadsWrites(events []Event) (reads, writes []Event) {
	for _, e := range events {
		if e.OpType == "GET" || e.OpType == "LIST" {
			reads = append(reads, e)
		} else {
			writes = append(writes, e)
		}
	}
	return
}

func IsReadOp(e Event) bool {
	return e.OpType == "GET" || e.OpType == "LIST"
}

func IsWriteOp(e Event) bool {
	return !IsReadOp(e)
}

// IsTopLevel returns true if the event is a top-level declarative state change event.
func IsTopLevel(e Event) bool {
	labels := e.Labels
	if labels == nil {
		return false
	}
	_, ok := labels["tracey-uid"]
	return ok
}

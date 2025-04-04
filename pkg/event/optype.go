package event

type OperationType string

const (
	INIT              OperationType = "INIT"
	GET               OperationType = "GET"
	LIST              OperationType = "LIST"
	CREATE            OperationType = "CREATE"
	UPDATE            OperationType = "UPDATE"
	MARK_FOR_DELETION OperationType = "DELETE"
	PATCH             OperationType = "PATCH"
	APPLY             OperationType = "APPLY"

	// custom op types

	// REMOVE is a custom operation type used to represnt an object
	// being purged from cluster state. This is different from DELETE
	// which simply marks an object for deletion.
	REMOVE OperationType = "REMOVE"
)

var MutationTypes = map[OperationType]struct{}{
	CREATE:            {},
	UPDATE:            {},
	MARK_FOR_DELETION: {},
	PATCH:             {},
}

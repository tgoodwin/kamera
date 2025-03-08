package event

type OperationType string

const (
	INIT   OperationType = "INIT"
	GET    OperationType = "GET"
	LIST   OperationType = "LIST"
	CREATE OperationType = "CREATE"
	UPDATE OperationType = "UPDATE"
	DELETE OperationType = "DELETE"
	PATCH  OperationType = "PATCH"
	APPLY  OperationType = "APPLY"
)

var MutationTypes = map[OperationType]struct{}{
	CREATE: {},
	UPDATE: {},
	DELETE: {},
	PATCH:  {},
}

package snapshot

import "github.com/tgoodwin/kamera/pkg/util"

// VersionKey uniquely identifies the state of an object at a given resource version
type VersionKey struct {
	Kind     string
	ObjectID string // TODO don't always diff on object ID as it is not yet present in create events
	Version  string
}

type KnowledgeSet util.Set[VersionKey]

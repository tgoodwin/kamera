package snapshot

import (
	"errors"
	"sync"
)

type IdentityKey struct {
	Kind     string
	ObjectID string
}

type LifeEventSource string

const (
	Traced    LifeEventSource = "TRACED"
	Synthetic LifeEventSource = "SYNTHETIC"
)

type LifeEvent struct {
	Version     VersionHash
	ReconcileID string
	Source      LifeEventSource
}

type LifecycleContainer struct {
	data map[IdentityKey][]LifeEvent
	mu   sync.RWMutex
}

func (l *LifecycleContainer) InsertSynthesized(key IdentityKey, val VersionHash, reconcileID string) {
	l.Insert(key, val, reconcileID, Synthetic)
}

func (l *LifecycleContainer) Insert(key IdentityKey, val VersionHash, reconcileID string, source LifeEventSource) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.data[key]; !ok {
		l.data[key] = make([]LifeEvent, 0)
	}
	evt := LifeEvent{
		Version:     val,
		ReconcileID: reconcileID,
		Source:      source,
	}
	l.data[key] = append(l.data[key], evt)
}

func (l *LifecycleContainer) Latest(key IdentityKey) (VersionHash, bool) {
	l.mu.RLock()
	defer l.mu.Unlock()

	if _, ok := l.data[key]; !ok {
		return "", false
	}
	return l.data[key][len(l.data[key])-1].Version, true
}

// VersionFromFrame gets the version hash for a given frameID (reconcileID). frameID corresponds to
// the reconcile in which the version was produced.
func (l *LifecycleContainer) VersionFromFrame(key IdentityKey, frameID string) (VersionHash, error) {
	versions, ok := l.data[key]
	if !ok {
		return "", errors.New("identity key not found")
	}
	for _, evt := range versions {
		if evt.ReconcileID == frameID {
			return evt.Version, nil
		}
	}
	return "", errors.New("reconcile ID not found in lifecycle for key")
}

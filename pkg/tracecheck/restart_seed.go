package tracecheck

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SeedObject captures a concrete Kubernetes object for restartable exploration.
// JSON holds the serialized object, GVK is retained for clarity, and Key carries the sleeve identity.
type SeedObject struct {
	GVK  schema.GroupVersionKind `json:"gvk"`
	Key  snapshot.CompositeKey   `json:"key"`
	JSON []byte                  `json:"json"`
}

// RestartSeed bundles materialized objects and pending reconciles for a fresh exploration run.
type RestartSeed struct {
	Objects           []SeedObject       `json:"objects"`
	PendingReconciles []PendingReconcile `json:"pendingReconciles"`
}

// RestartRequest bundles a seed plus config overrides for the next run.
type RestartRequest struct {
	Seed   RestartSeed
	Config ExploreConfig
}

// BuildRestartSeedFromState resolves ObjectVersions to concrete objects and produces a serializable seed.
func BuildRestartSeedFromState(objects ObjectVersions, resolver VersionManager, pending []PendingReconcile) (RestartSeed, error) {
	if resolver == nil {
		return RestartSeed{}, fmt.Errorf("resolver is nil")
	}

	keys := make([]snapshot.CompositeKey, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})

	seed := RestartSeed{
		PendingReconciles: slices.Clone(pending),
		Objects:           make([]SeedObject, 0, len(objects)),
	}

	for _, key := range keys {
		hash := objects[key]
		obj := resolver.Resolve(hash)
		if obj == nil {
			return RestartSeed{}, fmt.Errorf("failed to resolve object %s with hash %s", key, hash.Value)
		}
		gvk := obj.GroupVersionKind()
		if gvk.Empty() {
			gvk = util.GetGroupVersionKind(obj)
			obj.SetGroupVersionKind(gvk)
		}
		data, err := obj.MarshalJSON()
		if err != nil {
			return RestartSeed{}, fmt.Errorf("marshal object %s: %w", key, err)
		}
		seed.Objects = append(seed.Objects, SeedObject{
			GVK:  gvk,
			Key:  key,
			JSON: data,
		})
	}
	return seed, nil
}

// SeedToStateNode rebuilds a StateNode from a restart seed using the ExplorerBuilder's snapshot store.
func SeedToStateNode(seed RestartSeed, builder *ExplorerBuilder) (StateNode, error) {
	if builder == nil {
		return StateNode{}, fmt.Errorf("explorer builder is nil")
	}
	objs := make([]client.Object, 0, len(seed.Objects))
	for idx, entry := range seed.Objects {
		if len(entry.JSON) == 0 {
			return StateNode{}, fmt.Errorf("seed object %d has no JSON content", idx)
		}
		var u unstructured.Unstructured
		if err := json.Unmarshal(entry.JSON, &u); err != nil {
			return StateNode{}, fmt.Errorf("unmarshal seed object %d: %w", idx, err)
		}
		u.SetGroupVersionKind(entry.GVK)
		// Ensure metadata matches the composite key.
		u.SetNamespace(entry.Key.ResourceKey.Namespace)
		u.SetName(entry.Key.ResourceKey.Name)

		labels := u.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		if _, ok := labels[tag.TraceyObjectID]; !ok {
			labels[tag.TraceyObjectID] = entry.Key.IdentityKey.ObjectID
		}
		u.SetLabels(labels)

		objs = append(objs, &u)
	}

	return builder.BuildStartStateFromObjects(objs, seed.PendingReconciles)
}

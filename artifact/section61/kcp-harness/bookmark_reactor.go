package main

import (
	kcptesting "github.com/kcp-dev/client-go/third_party/k8s.io/client-go/testing"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// newBookmarkWatchReactor creates a watch reactor that sends an initial BOOKMARK
// event with the correct typed object for the watched resource. This is required
// by the KCP custom reflector which waits for a typed bookmark before considering
// the informer sync complete.
//
// typeMap maps GroupVersionResource to a factory function that creates an empty
// instance of the correct type with bookmark metadata set.
func newBookmarkWatchReactor(typeMap map[schema.GroupVersionResource]func() runtime.Object) kcptesting.WatchReactionFunc {
	return func(action kcptesting.Action) (bool, watch.Interface, error) {
		gvr := action.GetResource()
		factory, ok := typeMap[gvr]
		if !ok {
			// Unknown resource type — fall back to default reactor
			return false, nil, nil
		}

		obj := factory()
		w := watch.NewFake()
		go func() {
			w.Action(watch.Bookmark, obj)
		}()
		return true, w, nil
	}
}

// bookmarkObj creates a runtime.Object with bookmark metadata set.
func bookmarkObj(obj metav1.ObjectMetaAccessor) runtime.Object {
	meta := obj.GetObjectMeta()
	meta.SetResourceVersion("1")
	meta.SetAnnotations(map[string]string{
		"k8s.io/initial-events-end": "true",
	})
	return obj.(runtime.Object)
}

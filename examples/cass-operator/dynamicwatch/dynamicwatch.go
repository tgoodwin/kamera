package dynamicwatch

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type DynamicWatches interface {
	UpdateWatch(watcher types.NamespacedName, watched []types.NamespacedName) error
	RemoveWatcher(watcher types.NamespacedName) error
	FindWatchers(meta metav1.Object, object runtime.Object) []types.NamespacedName
}

type NoopDynamicWatches struct{}

func (n *NoopDynamicWatches) UpdateWatch(watcher types.NamespacedName, watched []types.NamespacedName) error {
	return nil
}

func (n *NoopDynamicWatches) RemoveWatcher(watcher types.NamespacedName) error {
	return nil
}

func (n *NoopDynamicWatches) FindWatchers(meta metav1.Object, object runtime.Object) []types.NamespacedName {
	return nil
}

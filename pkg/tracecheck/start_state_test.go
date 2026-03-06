package tracecheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/tag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBuildStartStateFromObjectsAssignsDeterministicUIDWhenMissing(t *testing.T) {
	builder := NewExplorerBuilder(runtime.NewScheme())

	obj := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "example",
		},
	}

	state, err := builder.BuildStartStateFromObjects([]client.Object{obj}, nil)
	require.NoError(t, err)

	vs := NewVersionStore(builder.snapStore, builder.scheme)
	objects := state.Objects()
	require.Len(t, objects, 1)

	for _, hash := range objects {
		stored := vs.Resolve(hash)
		require.NotNil(t, stored)
		expectedUID := types.UID(tag.GetSleeveObjectID(obj))
		assert.NotEmpty(t, stored.GetUID())
		assert.Equal(t, expectedUID, stored.GetUID())
	}
}

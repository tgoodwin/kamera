package snapshot

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "github.com/tgoodwin/sleeve/pkg/test/integration/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestAsRecord(t *testing.T) {
	// Mock object
	obj := &unstructured.Unstructured{}
	obj.SetKind("TestKind")
	obj.SetAPIVersion("v1")
	obj.SetName("test-object")
	obj.SetNamespace("default")
	obj.SetUID("12345")

	// Mock frameID
	frameID := "test-frame-id"

	// Call AsRecord
	record, err := AsRecord(obj, frameID)

	// Assertions
	require.NoError(t, err)
	assert.Equal(t, "12345", record.ObjectID)
	assert.Equal(t, "test-frame-id", record.ReconcileID)
	assert.Equal(t, "TestKind", record.Kind)

	// Verify JSON value
	var valueMap map[string]interface{}
	err = json.Unmarshal([]byte(record.Value), &valueMap)
	require.NoError(t, err)
	assert.Equal(t, "test-object", valueMap["metadata"].(map[string]interface{})["name"])
	assert.Equal(t, "default", valueMap["metadata"].(map[string]interface{})["namespace"])
	assert.Equal(t, "TestKind", valueMap["kind"])
}

func TestAsRecord_setKind(t *testing.T) {
	topLevelObj := &appsv1.Foo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-uid",
			Labels: map[string]string{
				"tracey-uid":                       "foo",
				"discrete.events/sleeve-object-id": "foo-123",
			},
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "appsv1",
			Kind:       "Foo",
		},
		Spec: appsv1.FooSpec{
			Mode: "A",
		},
	}

	record, err := AsRecord(topLevelObj, "test-frame-id")
	require.NoError(t, err)
	assert.Equal(t, "Foo", record.Kind)
	assert.Equal(t, "foo-uid", record.ObjectID)
	assert.Equal(t, "test-frame-id", record.ReconcileID)
}

func TestAsRecord_setKindNoTypeMeta(t *testing.T) {
	topLevelObj := &appsv1.Foo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-uid",
			Labels: map[string]string{
				"tracey-uid":                       "foo",
				"discrete.events/sleeve-object-id": "foo-123",
			},
		},
		Spec: appsv1.FooSpec{
			Mode: "A",
		},
	}

	record, err := AsRecord(topLevelObj, "test-frame-id")
	require.NoError(t, err)

	assert.Equal(t, "Foo", record.Kind)
	assert.Equal(t, "foo-uid", record.ObjectID)
	assert.Equal(t, "test-frame-id", record.ReconcileID)

	var valueMap map[string]interface{}
	err = json.Unmarshal([]byte(record.Value), &valueMap)
	require.NoError(t, err)

	assert.Equal(t, "v1", valueMap["apiVersion"])
}

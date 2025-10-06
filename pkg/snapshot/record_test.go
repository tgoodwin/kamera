package snapshot

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
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

func TestAsRecord_Serialization(t *testing.T) {
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
	expectedRecordJSON := `{
		"object_id": "foo-uid",
		"reconcile_id": "test-frame-id",
		"operation_id": "",
		"op_type": "",
		"hash": "SOME_HASH",
		"kind": "Foo",
		"version": "",
		"value": {
			"apiVersion": "v1",
			"kind": "Foo",
			"metadata": {
				"creationTimestamp": null,
				"labels": {
					"discrete.events/sleeve-object-id": "foo-123",
					"tracey-uid": "foo"
				},
				"name": "foo",
				"namespace": "default",
				"uid": "foo-uid"
			},
			"spec": {
				"mode": "A"
			},
			"status": {}
		}
	}`
	recordJSON, err := json.Marshal(record)
	require.NoError(t, err)
	recordJSON = []byte(regexp.MustCompile(`"hash":\s*".*?"`).ReplaceAllString(string(recordJSON), `"hash": "SOME_HASH"`))
	assert.JSONEq(t, expectedRecordJSON, string(recordJSON))
}

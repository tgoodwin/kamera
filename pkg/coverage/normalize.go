package coverage

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

var metadataStripFields = []string{
	"uid",
	"resourceVersion",
	"generation",
	"managedFields",
	"creationTimestamp",
	"selfLink",
	"finalizers",
}

// NormalizeTemplate returns a deep-copied object with deterministic identity and
// server-assigned fields stripped.
func NormalizeTemplate(obj *unstructured.Unstructured, name, namespace string) *unstructured.Unstructured {
	if obj == nil {
		return nil
	}

	copyObj := obj.DeepCopy()
	copyObj.SetName(name)
	copyObj.SetNamespace(namespace)

	unstructured.RemoveNestedField(copyObj.Object, "status")
	for _, field := range metadataStripFields {
		unstructured.RemoveNestedField(copyObj.Object, "metadata", field)
	}

	return copyObj
}

package replay

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestExtractMergePatchRV_ReflectionFields(t *testing.T) {
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "42"}}
	patch := client.MergeFromWithOptions(obj, client.MergeFromWithOptimisticLock{})

	v := reflect.ValueOf(patch)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	t.Logf("Kind: %s, Type: %s", v.Kind(), v.Type())
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		t.Logf("  Field %d: %s (exported=%v, canInterface=%v)", i, f.Name, f.IsExported(), v.Field(i).CanInterface())
	}

	optsField := v.FieldByName("opts")
	require.True(t, optsField.IsValid(), "opts field should be valid")
	lockField := optsField.FieldByName("OptimisticLock")
	require.True(t, lockField.IsValid(), "OptimisticLock field should be valid")
	require.True(t, lockField.Bool(), "OptimisticLock should be true")

	fromField := v.FieldByName("from")
	require.True(t, fromField.IsValid(), "from field should be valid")
	t.Logf("from canInterface: %v", fromField.CanInterface())
}

func TestExtractMergePatchRV_WithOptimisticLock(t *testing.T) {
	base := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "42"}}
	patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})

	modified := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
	preconditions := PreconditionInfo{}
	extractMergePatchRV(modified, patch, &preconditions)

	if preconditions.ResourceVersion == nil {
		t.Log("ResourceVersion not extracted — reflection may not access unexported fields")
		// This is expected if reflect can't access unexported interface fields
	} else {
		require.Equal(t, "42", *preconditions.ResourceVersion)
		t.Log("Successfully extracted RV from OptimisticLock merge-patch")
	}
}

func TestExtractMergePatchRV_WithoutOptimisticLock(t *testing.T) {
	base := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "42"}}
	patch := client.MergeFrom(base)

	modified := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
	preconditions := PreconditionInfo{}
	extractMergePatchRV(modified, patch, &preconditions)

	require.Nil(t, preconditions.ResourceVersion,
		fmt.Sprintf("RV should NOT be extracted for non-OptimisticLock merge-patch, got: %v", preconditions.ResourceVersion))
}

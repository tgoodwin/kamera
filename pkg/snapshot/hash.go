package snapshot

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/davecgh/go-spew/spew"
	"github.com/tgoodwin/kamera/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// type Hasher interface {
// 	Hash(obj *unstructured.Unstructured) VersionHash
// }

const (
	TimestampPlaceholder = "KAMERA_NON_NIL_TIMESTAMP"
)

var hashPrinter = &spew.ConfigState{
	Indent:         "",
	SortKeys:       true,
	DisableMethods: true,
	SpewKeys:       true,
}

func stableHashString(obj interface{}) string {
	hasher := sha256.New()
	hashPrinter.Fprintf(hasher, "%#v", obj)
	return hex.EncodeToString(hasher.Sum(nil))
}

type VersionHash struct {
	Value    string
	Strategy HashStrategy
}

func NewDefaultHash(value string) VersionHash {
	return VersionHash{Value: value, Strategy: DefaultHash}
}

func NewAnonHash(value string) VersionHash {
	return VersionHash{Value: value, Strategy: AnonymizedHash}
}

type JSONHasher struct {
}

func NewDefaultHasher() *JSONHasher {
	return &JSONHasher{}
}

func (h *JSONHasher) Hash(obj *unstructured.Unstructured) (VersionHash, error) {
	objCopy := obj.DeepCopy()
	return NewDefaultHash(stableHashString(objCopy.Object)), nil
}

func normalizeStatusConditions(obj *unstructured.Unstructured) {
	if status, ok := obj.Object["status"].(map[string]interface{}); ok {
		if conds, ok := status["conditions"].([]interface{}); ok {
			for _, c := range conds {
				if cond, ok := c.(map[string]interface{}); ok {
					cond["lastTransitionTime"] = TimestampPlaceholder
					cond["lastUpdateTime"] = TimestampPlaceholder
					if _, exists := cond["lastProbeTime"]; exists {
						cond["lastProbeTime"] = TimestampPlaceholder
					}
				}
			}
		}
	}
}

type AnonymizingHasher struct {
	Replacements map[string]string
}

func NewAnonymizingHasher(labelReplacements map[string]string) *AnonymizingHasher {
	return &AnonymizingHasher{Replacements: labelReplacements}
}

func (h *AnonymizingHasher) Hash(obj *unstructured.Unstructured) (VersionHash, error) {
	objCopy := obj.DeepCopy()
	origLabels := objCopy.GetLabels()
	anonymizedLabels := make(map[string]string, len(origLabels))
	for k, v := range origLabels {
		if replacement, ok := h.Replacements[k]; ok {
			anonymizedLabels[k] = replacement
		} else {
			anonymizedLabels[k] = v
		}
	}
	objCopy.SetLabels(anonymizedLabels)
	normalizeStatusConditions(objCopy)
	return VersionHash{Value: stableHashString(objCopy.Object), Strategy: AnonymizedHash}, nil
}

// DefaultLabelReplacements is a map of label replacements for
// the labels that take on non-deterministic values.
var DefaultLabelReplacements = map[string]string{
	tag.TraceyReconcileID: "RECONCILE_ID",
	tag.ChangeID:          "CHANGE_ID",
	tag.TraceyObjectID:    "OBJECT_ID",
	tag.DeletionID:        "DELETION_ID",
	tag.TraceyCreatorID:   "CREATOR_ID",
}

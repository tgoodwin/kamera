package replay

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type FrameType string

const (
	FrameTypeTraced    FrameType = "TRACED"
	FrameTypeSynthetic FrameType = "SYNTHETIC"
)

// Like the frames of a movie, a Frame is a snapshot of the state of the world at a particular point in time.
type Frame struct {
	ID   string // ReconcileID
	Type FrameType

	// for ordering. In practice this is just a timestamp
	sequenceID string

	Req reconcile.Request

	TraceyRootID string
}

type frameIDKey struct{}

func WithFrameID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, frameIDKey{}, id)
}

func FrameIDFromContext(ctx context.Context) string {
	id, ok := ctx.Value(frameIDKey{}).(string)
	if !ok {
		panic("frame id not found in context")
	}
	if id == "" {
		panic("frame id from context is empty")
	}
	return id
}

// CacheFrame is a map of kind -> namespace/name -> object
// and it is keyed by namespace/name cause that is the access pattern that controller code uses.
type CacheFrame map[string]map[types.NamespacedName]*unstructured.Unstructured

func (c CacheFrame) Copy() CacheFrame {
	newFrame := make(CacheFrame)
	for kind, objs := range c {
		newFrame[kind] = make(map[types.NamespacedName]*unstructured.Unstructured)
		for nn, obj := range objs {
			newFrame[kind][nn] = obj
		}
	}
	return newFrame
}

func (c CacheFrame) Dump() {
	for kind, objs := range c {
		for nn := range objs {
			fmt.Printf("\t%s/%s/%s\n", kind, nn.Namespace, nn.Name)
		}
	}
}

// frameContainer maps reconcileID -> CacheFrame
type frameContainer map[string]CacheFrame

type FrameManager struct {
	Frames frameContainer
}

func NewFrameManager(initialFrames frameContainer) *FrameManager {
	if initialFrames == nil {
		initialFrames = make(frameContainer)
	}
	return &FrameManager{
		Frames: initialFrames,
	}
}

func (fm *FrameManager) InsertFrame(reconcileID string, data CacheFrame) {
	if _, ok := fm.Frames[reconcileID]; ok {
		panic(fmt.Sprintf("frame %s already exists", reconcileID))
	}
	fm.Frames[reconcileID] = data
}

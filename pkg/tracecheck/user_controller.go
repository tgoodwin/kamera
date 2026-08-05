package tracecheck

import (
	"context"
	"fmt"
	"strconv"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	UserControllerID ReconcilerID = "External User"

	UserActionIDMetadataKey    = "userAction.id"
	UserActionTypeMetadataKey  = "userAction.type"
	UserActionIndexMetadataKey = "userAction.index"
)

type UserAction struct {
	ID      string              `json:"id"`
	OpType  event.OperationType `json:"type"`
	Payload runtime.Object      `json:"payload"`
}

type UserController struct {
	effectContextManager EffectContextManager
	container            *ReconcilerContainer
	reconciler           *userActionReconciler
}

type userActionClient interface {
	client.Client
}

type userActionReconciler struct {
	actions []UserAction
	client  userActionClient
}

func userActionStepMetadata(action UserAction, actionIdx int) map[string]string {
	return map[string]string{
		UserActionIDMetadataKey:    action.ID,
		UserActionTypeMetadataKey:  string(action.OpType),
		UserActionIndexMetadataKey: strconv.Itoa(actionIdx),
	}
}

func (r *userActionReconciler) HasActionAt(actionIdx int) bool {
	return actionIdx >= 0 && actionIdx < len(r.actions)
}

func (r *userActionReconciler) actionCount() int {
	return len(r.actions)
}

func (r *userActionReconciler) actionForRequest(req reconcile.Request) (UserAction, error) {
	actionIdx, err := strconv.Atoi(req.Name)
	if err != nil {
		return UserAction{}, fmt.Errorf("invalid user action request index %q: %w", req.Name, err)
	}
	if !r.HasActionAt(actionIdx) {
		return UserAction{}, fmt.Errorf("user action index %d out of range (len=%d)", actionIdx, len(r.actions))
	}
	return r.actions[actionIdx], nil
}

func (r *userActionReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	action, err := r.actionForRequest(req)
	if err != nil {
		return reconcile.Result{}, err
	}
	if r.client == nil {
		return reconcile.Result{}, fmt.Errorf("user action client is not configured")
	}
	if action.Payload == nil {
		return reconcile.Result{}, fmt.Errorf("user action %q payload must implement client.Object", action.ID)
	}

	payload := action.Payload.DeepCopyObject()
	obj, ok := payload.(client.Object)
	if !ok || obj == nil {
		return reconcile.Result{}, fmt.Errorf("user action %q payload must implement client.Object", action.ID)
	}

	switch action.OpType {
	case event.CREATE:
		if err := r.client.Create(ctx, obj); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	case event.UPDATE:
		// Workflow inputs describe the desired object and commonly omit the
		// server-managed resourceVersion. Model the read-modify-write performed
		// by a real API client before issuing Update, while preserving an
		// explicitly supplied RV so scenarios can intentionally exercise stale
		// user writes.
		if obj.GetResourceVersion() == "" {
			current := obj.DeepCopyObject().(client.Object)
			if err := r.client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
				return reconcile.Result{}, err
			}
			obj.SetResourceVersion(current.GetResourceVersion())
		}
		if err := r.client.Update(ctx, obj); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	case event.MARK_FOR_DELETION:
		if err := r.client.Delete(ctx, obj); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	default:
		return reconcile.Result{}, fmt.Errorf("unsupported user action op type %q", action.OpType)
	}
}

func NewUserController(
	effectContextManager EffectContextManager,
	container *ReconcilerContainer,
	reconciler *userActionReconciler,
) *UserController {
	return &UserController{
		effectContextManager: effectContextManager,
		container:            container,
		reconciler:           reconciler,
	}
}

func (u *UserController) HasActionAt(actionIdx int) bool {
	return u != nil && u.reconciler != nil && u.reconciler.HasActionAt(actionIdx)
}

func (u *UserController) ExecuteNextAction(ctx context.Context, observableState ObjectVersions, actionIdx int) (*ReconcileResult, error) {
	if u == nil {
		return nil, fmt.Errorf("user controller is nil")
	}
	if !u.HasActionAt(actionIdx) {
		total := 0
		if u.reconciler != nil {
			total = u.reconciler.actionCount()
		}
		return nil, fmt.Errorf("user action index %d out of range (len=%d)", actionIdx, total)
	}
	if u.effectContextManager == nil {
		return nil, fmt.Errorf("user controller effect context manager is nil")
	}
	if u.container == nil {
		return nil, fmt.Errorf("user controller container is nil")
	}
	if u.reconciler == nil {
		return nil, fmt.Errorf("user controller reconciler is nil")
	}
	action := u.reconciler.actions[actionIdx]
	metadata := userActionStepMetadata(action, actionIdx)

	frameID := util.UUID()
	stepCtx := replay.WithFrameID(ctx, frameID)
	if err := u.effectContextManager.PrepareEffectContext(stepCtx, observableState); err != nil {
		return nil, err
	}
	defer u.effectContextManager.CleanupEffectContext(stepCtx)

	req := reconcile.Request{}
	req.Name = strconv.Itoa(actionIdx)
	result, err := u.container.doReconcile(stepCtx, observableState, req)
	if err != nil {
		if result != nil {
			result.StepMetadata = metadata
		}
		return &ReconcileResult{
			ControllerID: UserControllerID,
			FrameID:      frameID,
			FrameType:    FrameTypeExplore,
			Error:        err.Error(),
			StepMetadata: metadata,
		}, err
	}
	result.StepMetadata = metadata
	return result, nil
}

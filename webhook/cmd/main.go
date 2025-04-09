package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

var KUBECTL_USERNAME = "kubernetes-admin"

var UsernameSubstringValidationWhitelist = []string{
	"system:node",               // kubelet
	"garbage-collector",         // default garbage collector
	"pvc-protection-controller", // removes finalizers from PVCs
	"sleeve-system:sleevectrl",
	"sleeve:controller-user", // the rest.Impersonate username when running controllers locally to distinguish from whatever it picks up in ~/.kube/config
}

func main() {
	setLogger()

	handler, err := NewHandler()
	if err != nil {
		logrus.Fatal(err)
	}

	http.HandleFunc("/tag-resource", handler.ServeTagResource)
	http.HandleFunc("/validate-resource", handler.ServeValidateResource)
	http.HandleFunc("/health", ServeHealth)

	// start the server
	// listens to clear text http on port 8080 unless TLS env var is set to "true"
	if os.Getenv("TLS") == "true" {
		cert := "/etc/admission-webhook/tls/tls.crt"
		key := "/etc/admission-webhook/tls/tls.key"
		logrus.Print("Listening on port 443...")
		logrus.Fatal(http.ListenAndServeTLS(":443", cert, key, nil))
	} else {
		logrus.Print("Listening on port 8080...")
		logrus.Fatal(http.ListenAndServe(":8080", nil))
	}
}

// ServeHealth returns 200 when things are good
func ServeHealth(w http.ResponseWriter, r *http.Request) {
	logrus.WithField("uri", r.RequestURI).Debug("healthy")
	fmt.Fprint(w, "OK")
}

type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

type withlabels struct {
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

// N.B. if running controllers outside of the cluster,
// the request username will be that of the kubectl user
// because they use the same kubeconfig credentials.
func cameFromTheOutside(in *admissionv1.AdmissionReview) bool {
	return in.Request.UserInfo.Username == KUBECTL_USERNAME
}

func notTaggedYet(labels map[string]string) bool {
	_, hasWebhookTag := labels[tag.TraceyWebhookLabel]
	_, hasPropagatedTag := labels[tag.TraceyRootID]
	return !hasWebhookTag && !hasPropagatedTag
}

// hasSleeveLabels checks that objects created by controllers
// are instrumented under sleeve. WARNING - its possible that
// a non-instrumented controller is operating on an instrumented
// object, so dont use this as a way to determine controller identity
func hasSleeveLabels(in *admissionv1.AdmissionReview) bool {
	var rawData []byte
	if in.Request.Operation == admissionv1.Delete {
		rawData = in.Request.OldObject.Raw
	} else {
		rawData = in.Request.Object.Raw
	}
	wl := withlabels{}
	if err := json.Unmarshal(rawData, &wl); err != nil {
		return false
	}
	var labels map[string]string
	if wl.Labels == nil {
		labels = make(map[string]string)
	} else {
		labels = wl.Labels
	}
	// current heuristic: if the object has a creator tag, it's a sleeve object
	_, hasCreatorTag := labels[tag.TraceyCreatorID]
	return hasCreatorTag
}

type Handler struct {
	emitter event.Emitter
}

// TODO de-hardcode
func NewHandler() (*Handler, error) {
	clientCfg := event.MinioConfig{
		Endpoint:        event.ClusterInternalEndpoint,
		AccessKeyID:     "myaccesskey",
		SecretAccessKey: "mysecretkey",
		UseSSL:          false,
		BucketName:      event.DefaultBucketName,
	}

	emitter, err := event.NewMinioEmitter(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Minio emitter")
	}

	sh := &Handler{
		emitter: emitter,
	}

	return sh, nil
}

func (s *Handler) ServeTagResource(w http.ResponseWriter, r *http.Request) {
	in, err := parseRequest(*r)
	logger := logrus.WithField("resource", in.Request.RequestResource)
	logger.WithFields(logrus.Fields{
		"kind":            in.Request.Kind,
		"subResource":     in.Request.SubResource,
		"requestKind":     in.Request.RequestKind,
		"requestResource": in.Request.RequestResource,
		"user":            in.Request.UserInfo.Username,
	}).Debug("received tag resource request")

	if err != nil {
		logger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wl := withlabels{}
	if err := json.Unmarshal(in.Request.Object.Raw, &wl); err != nil {
		logger.Errorf("could not unmarshal labels: %s", err.Error())
	}

	var labels map[string]string
	if wl.Labels == nil {
		labels = make(map[string]string)
	} else {
		labels = wl.Labels
	}

	// only label the object if it doesn't already have a propagated root label
	if cameFromTheOutside(in) && notTaggedYet(labels) {
		// set the top-level label, which is used to track
		// causal relationships between downstream events.
		// also assign a unique object ID to the object to be consistent
		// with simulation contexts when there is no APIServer to assign
		// a UID.
		labels[tag.TraceyWebhookLabel] = uuid.New().String()
		labels[tag.TraceyObjectID] = uuid.New().String()
	}

	patches := []patchOperation{
		{
			Op:    "add",
			Path:  "/metadata/labels",
			Value: labels,
		},
	}

	patchBytes, err := json.Marshal(patches)
	if err != nil {
		logger.Error(err)
	}

	patchType := admissionv1.PatchTypeJSONPatch
	out := &admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
		Response: &admissionv1.AdmissionResponse{
			UID:       in.Request.UID,
			Allowed:   true,
			PatchType: &patchType,
			Patch:     patchBytes,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	jout, err := json.Marshal(out)
	if err != nil {
		e := fmt.Sprintf("could not parse admission response: %v", err)
		logger.Error(e)
		http.Error(w, e, http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%s", jout)
}

func canByPassvalidation(userInfo authenticationv1.UserInfo) bool {
	for _, s := range UsernameSubstringValidationWhitelist {
		if strings.Contains(userInfo.Username, s) {
			return true
		}
	}
	return false
}

// ServeValidateResource validates an admission request and then writes an admission
// review to `w`
func (h *Handler) ServeValidateResource(w http.ResponseWriter, r *http.Request) {
	logger := logrus.WithField("uri", r.RequestURI)
	in, err := parseRequest(*r)
	if err != nil {
		logger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logger = logger.WithFields(logrus.Fields{
		"requestID":       in.Request.UID,
		"kind":            in.Request.Kind,
		"subResource":     in.Request.SubResource,
		"requestKind":     in.Request.RequestKind,
		"requestResource": in.Request.RequestResource,
		"user":            in.Request.UserInfo.Username,
	})
	logger.Debug("received validate resource request")

	// let kubelet operations through
	cameFromOutside := cameFromTheOutside(in)

	// used to guard against objects created by non-instrumented controllers
	hasSleeveTags := hasSleeveLabels(in)

	// let kubelet and garbage collector just do their thing
	canBypass := canByPassvalidation(in.Request.UserInfo)

	// Ensure we capture delete events in the traces
	// the Username is 'system:serviceaccount:kube-system:generic-garbage-collector'
	isGarbageCollector := strings.Contains(in.Request.UserInfo.Username, "garbage-collector")
	if isGarbageCollector {
		// this is not complete - garbage collector only handles objects that are being cleaned
		// up through owner references.
		if err := h.emitGarbageCollectionEvent(in.Request); err != nil {
			logger.Error(err, "emitting garbage collection event")
		}
	}

	if err := h.captureObjectRemovalEvent(in.Request); err != nil {
		logger.Error(err, "capturing object removal event")
	}

	// this requires explicitly impersonating the sleeve controller user
	// in he controller setup configuration:
	// 	cfg := ctrl.GetConfigOrDie()
	// cfg.Impersonate = rest.ImpersonationConfig{
	// 	UserName: "sleeve:controller-user",
	// 	Groups:   []string{"system:masters"},
	// }
	// mgr, err := ctrl.NewManager(cfg, options)

	isSleeve := in.Request.UserInfo.Username == util.SleeveControllerUsername

	if cameFromOutside {
		logger.WithField("userinfo", in.Request.UserInfo).Debug("logging external declaration event")
		err := h.emitDeclarativeEvent(in.Request)
		if err != nil {
			logger.Error(err)
			return
		}
	}

	var message string
	switch {
	case cameFromOutside:
		message = "external request"
	case hasSleeveTags:
		message = "Has sleeve tags"
	case canBypass:
		message = fmt.Sprintf("user %s whitelisted: can bypass validation", in.Request.UserInfo.Username)
	case isSleeve:
		message = "you are sleeve"
	default:
		message = "sleeve tags are required for this namespace"
	}

	// I removed the `hasSleeveTags` check because it was allowing some
	// operations from non-sleeve controllers to pass through.
	// this went against my goals, but I didn't realize that these operations that were slipping
	// through were actually helping the statefulset controller to create the pods it needed to.
	// When I remove the `hasSleeveTags` check, the statefulset controller is able to create the pods
	// but they do not fully become ready (as the core statefulset controller is no longer helping).
	// creating / deleting is all I really need for the bug I am experimenting with, so I will leave
	// the code as it is now.
	// - TODO - get the sleeve:statefulset-controller working so it doesnt need help
	allowed := cameFromOutside || isSleeve || canBypass
	statusCode := http.StatusForbidden
	if allowed {
		statusCode = http.StatusAccepted
	}

	logger.WithFields(logrus.Fields{
		"requestID":       in.Request.UID,
		"user":            in.Request.UserInfo.Username,
		"allowed":         allowed,
		"message":         message,
		"cameFromOutside": cameFromOutside,
		"hasSleeveTags":   hasSleeveTags,
		"canBypass":       canBypass,
	}).Debug("evaluated validation checks")

	out := &admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
		Response: &admissionv1.AdmissionResponse{
			UID:     in.Request.UID,
			Allowed: allowed,
			Result: &metav1.Status{
				Code:    int32(statusCode),
				Message: message,
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	jout, err := json.Marshal(out)
	if err != nil {
		e := fmt.Sprintf("could not parse admission response: %v", err)
		logger.Error(e)
		http.Error(w, e, http.StatusInternalServerError)
		return
	}

	logger.Debugf("%s", jout)
	fmt.Fprintf(w, "%s", jout)
}

// setLogger sets the logger using env vars, it defaults to text logs on
// debug level unless otherwise specified
func setLogger() {
	logrus.SetLevel(logrus.DebugLevel)

	lev := os.Getenv("LOG_LEVEL")
	if lev != "" {
		llev, err := logrus.ParseLevel(lev)
		if err != nil {
			logrus.Fatalf("cannot set LOG_LEVEL to %q", lev)
		}
		logrus.SetLevel(llev)
	}

	if os.Getenv("LOG_JSON") == "true" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}
}

// parseRequest extracts an AdmissionReview from an http.Request if possible
func parseRequest(r http.Request) (*admissionv1.AdmissionReview, error) {
	if r.Header.Get("Content-Type") != "application/json" {
		return nil, fmt.Errorf("Content-Type: %q should be %q",
			r.Header.Get("Content-Type"), "application/json")
	}

	bodybuf := new(bytes.Buffer)
	bodybuf.ReadFrom(r.Body)
	body := bodybuf.Bytes()

	if len(body) == 0 {
		return nil, fmt.Errorf("admission request body is empty")
	}

	var a admissionv1.AdmissionReview

	if err := json.Unmarshal(body, &a); err != nil {
		return nil, fmt.Errorf("could not parse admission review request: %v", err)
	}

	if a.Request == nil {
		return nil, fmt.Errorf("admission review can't be used: Request field is nil")
	}

	return &a, nil
}

func parseObject(req *admissionv1.AdmissionRequest) (*unstructured.Unstructured, error) {
	var rawBytes []byte
	if req.Operation == admissionv1.Delete {
		rawBytes = req.OldObject.Raw
	} else {
		rawBytes = req.Object.Raw
	}
	if len(rawBytes) == 0 {
		return nil, fmt.Errorf("object is empty")
	}

	var obj unstructured.Unstructured
	if err := json.Unmarshal(rawBytes, &obj.Object); err != nil {
		return nil, fmt.Errorf("could not unmarshal object into unstructured: %v", err)
	}
	return &obj, nil
}

func (h *Handler) emitGarbageCollectionEvent(req *admissionv1.AdmissionRequest) error {
	op := req.Operation
	if op != admissionv1.Delete {
		return nil
	}
	obj, err := parseObject(req)
	if err != nil {
		return fmt.Errorf("could not parse object: %v", err)
	}

	logrus.WithFields(logrus.Fields{
		"requestID": req.UID,
		"user":      req.UserInfo.Username,
		"opType":    req.Operation,
		"kind":      req.Kind.Kind,
		"ObjectID":  obj.GetUID(),
	}).Debug("emitting garbage collection event")

	// garbage collector does not actually purge data,
	// it uses the DELETE API like everything else.
	// However, its DELETE operation does not return here to this webhook endpoint,
	// so lets just emit a REMOVE event here.
	opType := event.REMOVE

	return h.emitEvent(req.UID, obj, util.GarbageCollectorName, opType)
}

func (h *Handler) captureObjectRemovalEvent(req *admissionv1.AdmissionRequest) error {
	// case 1: the operation is an UPDATE, and the update is removing the last finalizer
	// case 2: the operation is a DELETE and there are no finalizers
	logger := logrus.WithFields(
		logrus.Fields{
			"user":    req.UserInfo.Username,
			"req.UID": req.UID,
		})
	switch req.Operation {
	case admissionv1.Update:
		// case 1: UPDATE operation removing the last finalizer from an object with deletion timestamp
		var oldObj, newObj unstructured.Unstructured
		if err := json.Unmarshal(req.OldObject.Raw, &oldObj.Object); err != nil {
			return fmt.Errorf("could not unmarshal old object: %v", err)
		}
		if err := json.Unmarshal(req.Object.Raw, &newObj.Object); err != nil {
			return fmt.Errorf("could not unmarshal new object: %v", err)
		}
		// check if the object has a deletion timestamp
		if newObj.GetDeletionTimestamp() != nil {

			// check if we're removing the last finalizer
			oldFinalizers := oldObj.GetFinalizers()
			newFinalizers := newObj.GetFinalizers()
			if len(oldFinalizers) > 0 && len(newFinalizers) == 0 {
				logger.WithFields(logrus.Fields{
					"requestID":     req.UID,
					"user":          req.UserInfo.Username,
					"opType":        req.Operation,
					"kind":          req.Kind.Kind,
					"ObjectID":      newObj.GetUID(),
					"oldFinalizers": oldFinalizers,
					"newFinalizers": newFinalizers,
				}).Debug("finalizer change detected, emitting REMOVE event for UPDATE operation")
				h.emitEvent(req.UID, &newObj, util.APIServerPurgeName, event.REMOVE)
				return nil
			}
		}

	case admissionv1.Delete:
		// case 2: DELETE operation with no finalizers
		var obj unstructured.Unstructured
		if err := json.Unmarshal(req.OldObject.Raw, &obj.Object); err != nil {
			return fmt.Errorf("could not unmarshal object: %v", err)
		}

		if len(obj.GetFinalizers()) == 0 {
			logger.WithFields(logrus.Fields{
				"requestID": req.UID,
				"user":      req.UserInfo.Username,
				"opType":    req.Operation,
				"kind":      req.Kind.Kind,
				"ObjectID":  obj.GetUID(),
			}).Debug("no finalizers on deleted object, emitting REMOVE event for DELETE operation")
			h.emitEvent(req.UID, &obj, util.APIServerPurgeName, event.REMOVE)
			return nil
		}
	}
	return nil
}

func (h *Handler) emitDeclarativeEvent(req *admissionv1.AdmissionRequest) error {
	op := req.Operation
	kind := req.Kind.Kind

	logrus.WithFields(logrus.Fields{
		"kind":     kind,
		"Username": req.UserInfo.Username,
		"opType":   op,
	}).Debug("emitting declarative event")

	obj, err := parseObject(req)
	if err != nil {
		return fmt.Errorf("could not parse object: %v", err)
	}

	sleeveOp, ok := admissionV1ToSleeveOp[op]
	if !ok {
		return fmt.Errorf("could not map admission v1 operation to sleeve operation")
	}
	return h.emitEvent(req.UID, obj, "TraceyWebhook", sleeveOp)
}

func (h *Handler) emitEvent(id types.UID, obj *unstructured.Unstructured, ControllerID string, op event.OperationType) error {
	rootEventID, err := tag.GetRootID(obj)
	if err != nil {
		logrus.Error(err)
		return err
	}
	recordJSON, err := json.Marshal(obj.Object)
	if err != nil {
		logrus.Error(err)
		return err
	}
	eventID := uuid.New().String()
	hash := util.ShortenHash(string(recordJSON))
	baseEvent := event.Event{
		ID:           eventID,
		Timestamp:    event.FormatTimeStr(time.Now()),
		OpType:       string(op),
		Kind:         obj.GetKind(),
		ObjectID:     string(obj.GetUID()),
		ReconcileID:  string(id),
		ControllerID: ControllerID,
		RootEventID:  rootEventID,
		Version:      obj.GetResourceVersion(),
		Labels:       tag.GetSleeveLabels(obj),
	}

	record := snapshot.Record{
		ObjectID:      string(obj.GetUID()),
		ReconcileID:   string(id),
		OperationID:   baseEvent.ID,
		OperationType: string(op),
		Kind:          obj.GetKind(),
		Version:       obj.GetResourceVersion(),
		Value:         recordJSON,
		Hash:          hash,
	}

	h.emitter.LogOperation(context.TODO(), &baseEvent)
	h.emitter.LogObjectVersion(context.TODO(), record)

	logrus.WithFields(logrus.Fields{
		"eventID":      eventID,
		"opType":       op,
		"kind":         obj.GetKind(),
		"objectID":     obj.GetUID(),
		"controllerID": ControllerID,
		"rootEventID":  rootEventID,
		"Record":       string(recordJSON),
	}).Debug("emitted event")

	return nil
}

var admissionV1ToSleeveOp = map[admissionv1.Operation]event.OperationType{
	admissionv1.Create: event.CREATE,
	admissionv1.Update: event.UPDATE,
	admissionv1.Delete: event.MARK_FOR_DELETION,
}

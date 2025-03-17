package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func main() {
	setLogger()

	http.HandleFunc("/tag-resource", ServeTagResource)
	http.HandleFunc("/validate-resource", ServeValidateResource)
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
// are instrumented under sleeve.
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

var KUBECTL_USERNAME = "kubernetes-admin"

func ServeTagResource(w http.ResponseWriter, r *http.Request) {
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
		// set the top-level label. that's all we do here.
		labels[tag.TraceyWebhookLabel] = uuid.New().String()
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
	validSubstrings := []string{
		"system:node",       // kubelet
		"garbage-collector", // default garbage collector
	}
	for _, s := range validSubstrings {
		if strings.Contains(userInfo.Username, s) {
			return true
		}
	}
	return false
}

// ServeValidateResource validates an admission request and then writes an admission
// review to `w`
func ServeValidateResource(w http.ResponseWriter, r *http.Request) {
	logger := logrus.WithField("uri", r.RequestURI)
	in, err := parseRequest(*r)
	if err != nil {
		logger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logger.WithFields(logrus.Fields{
		"kind":            in.Request.Kind,
		"subResource":     in.Request.SubResource,
		"requestKind":     in.Request.RequestKind,
		"requestResource": in.Request.RequestResource,
		"user":            in.Request.UserInfo.Username,
	}).Debug("received validate resource request")

	// let kubelet operations through
	cameFromOutside := cameFromTheOutside(in)

	// used to guard against objects created by non-instrumented controllers
	hasSleeveTags := hasSleeveLabels(in)

	// let kubelet and garbage collector just do their thing
	canBypass := canByPassvalidation(in.Request.UserInfo)

	// this requires explicitly impersonating the sleeve controller user
	// in he controller setup configuration:
	// 	cfg := ctrl.GetConfigOrDie()
	// cfg.Impersonate = rest.ImpersonationConfig{
	// 	UserName: "sleeve:controller-user",
	// 	Groups:   []string{"system:masters"},
	// }
	// mgr, err := ctrl.NewManager(cfg, options)
	// if err != nil {
	// 	setupLog.Error(err, "unable to start manager")

	isSleeve := in.Request.UserInfo.Username == "sleeve:controller-user"

	if cameFromOutside {
		logger.WithField("userinfo", in.Request.UserInfo).Debug("logging external declaration event")
		err := emitDeclarativeEvent(in.Request, "")
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
		message = "user whitelisted: can bypass validation"
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
		"user":            in.Request.UserInfo.Username,
		"allowed":         allowed,
		"message":         message,
		"cameFromOutside": cameFromOutside,
		"hasSleeveTags":   hasSleeveTags,
		"isSleeve":        isSleeve,
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

func emitDeclarativeEvent(req *admissionv1.AdmissionRequest, rootID string) error {
	op := req.Operation
	kind := req.Kind.Kind
	namespace := req.Namespace
	name := req.Name

	logrus.WithFields(logrus.Fields{
		"kind": kind,
	}).Debug("emitting declarative event")
	// Temp hack: hard-code the CRDs being declared externally via kubectl.
	// This is necessary because external requests cannot be reliably
	// distinguished when controllers are run locally using the same
	// client configuration as the kubectl CLI tool.
	// TODO
	externalKinds := []string{
		"CassandraDatacenter",
	}
	if !lo.Contains(externalKinds, kind) {
		// return early if the kind is not in the list of external kinds
		return nil
	}

	logrus.WithFields(logrus.Fields{
		"kind": kind,
		"op":   op,
	}).Debug("emitting event")
	event := event.DeclarativeEvent{
		ID:        uuid.New().String(),
		Timestamp: event.FormatTimeStr(time.Now()),
		OpType:    string(op),
		ResourceKey: snapshot.ResourceKey{
			Kind:      kind,
			Namespace: namespace,
			Name:      name,
		},
		// Value:       string(serializedObj),
		RootEventID: rootID,
	}
	logger := logrus.WithField("event", event)
	logger.Debug("emitting event")

	return nil
}

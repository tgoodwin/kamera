package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	// TODO this is imported as client. rename it to sleeve at the source.
	sleevetag "github.com/tgoodwin/sleeve/tag"
	admissionv1 "k8s.io/api/admission/v1"
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

func cameFromTheOutside(in *admissionv1.AdmissionReview) bool {
	return in.Request.UserInfo.Username == KUBECTL_USERNAME
}

func hasSleeveLabels(in *admissionv1.AdmissionReview) bool {
	wl := withlabels{}
	if err := json.Unmarshal(in.Request.Object.Raw, &wl); err != nil {
		return false
	}
	var labels map[string]string
	if wl.Labels == nil {
		labels = make(map[string]string)
	} else {
		labels = wl.Labels
	}
	// current heuristic: if the object has a creator tag, it's a sleeve object
	_, hasCreatorTag := labels[sleevetag.TRACEY_CREATOR_ID]
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

	notTaggedYet := func() bool {
		_, hasWebhookTag := labels[sleevetag.TRACEY_WEBHOOK_LABEL]
		_, hasPropagatedTag := labels[sleevetag.TRACEY_ROOT_ID]
		return !hasWebhookTag && !hasPropagatedTag
	}

	// only label the object if it doesn't already have a propagated root label
	if cameFromTheOutside(in) && notTaggedYet() {
		// set the top-level label. that's all we do here.
		labels[sleevetag.TRACEY_WEBHOOK_LABEL] = uuid.New().String()
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

// ServeValidateResource validates an admission request and then writes an admission
// review to `w`
func ServeValidateResource(w http.ResponseWriter, r *http.Request) {
	logger := logrus.WithField("uri", r.RequestURI)
	logger.Debug("received validation request")

	in, err := parseRequest(*r)
	if err != nil {
		logger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cameFromOutside := cameFromTheOutside(in)
	hasSleeveTags := hasSleeveLabels(in)

	var message string
	if cameFromOutside {
		message = "external request"
	} else if hasSleeveTags {
		message = "Has sleeve tags"
	} else {
		message = "sleeve tags are required for this namespace"
	}

	allowed := cameFromOutside || hasSleeveTags
	statusCode := http.StatusForbidden
	if allowed {
		statusCode = http.StatusAccepted
	}

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

	logger.Debug("sending response")
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

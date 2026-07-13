package main

import (
	"io"
	"net/http"
	"strings"
)

// configureSimulatedManagementAPI keeps the offline harness self-contained.
// The imported cass-operator calls its node management API before applying
// lifecycle labels; successful deterministic responses let that real control
// flow execute without opening network connections.
func configureSimulatedManagementAPI() {
	http.DefaultClient = &http.Client{Transport: simulatedManagementAPITransport{}}
}

type simulatedManagementAPITransport struct{}

func (simulatedManagementAPITransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body := "{}"
	if request.URL.Path == "/api/v0/metadata/endpoints" {
		body = `{"entity":[]}`
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

# syntax=docker/dockerfile:experimental
# ---
FROM golang:1.23 AS build

ENV GOOS=linux
ENV GOARCH=amd64
ENV CGO_ENABLED=0

WORKDIR /work
COPY webhook/ /work
COPY pkg/ /work/pkg/
COPY go.mod /work
COPY go.sum /work

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Build admission-webhook
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=private \
  go build -o bin/admission-webhook cmd/main.go

# ---
FROM scratch AS run

COPY --from=build /work/bin/admission-webhook /usr/local/bin/

CMD ["admission-webhook"]

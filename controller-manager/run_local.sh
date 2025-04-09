#!/bin/bash

set -x

SLEEVE_MINIO_ENDPOINT=localhost:9000 \
SLEEVE_MINIO_ACCESS_KEY=myaccesskey \
SLEEVE_MINIO_SECRET_KEY=mysecretkey \
SLEEVE_MINIO_BUCKET=sleeve \
go run cmd/main.go --health-probe-bind-address :8082

.PHONY: test
test:
	@echo "🧪 Running tests..."
	go test ./...

.PHONY: build-webhook
build-webhook:
	@echo "\n🔧  Building Go binaries..."
	GOOS=darwin GOARCH=amd64 go build webhook/main.go -o bin/admission-webhook-darwin-amd64 .
	GOOS=linux GOARCH=amd64 go build webhook/main.go -o bin/admission-webhook-linux-amd64 .

.PHONY: docker-build-webhook
docker-build-webhook:
	@echo "\n📦 Building simple-kubernetes-webhook Docker image..."
	docker build -t simple-kubernetes-webhook:latest -f webhook/Dockerfile .


.PHONY: push-webhook
push-webhook: docker-build-webhook
	@echo "\n📦 Pushing admission-webhook image into Kind's Docker daemon..."
	kind load docker-image simple-kubernetes-webhook:latest

.PHONY: deploy-config
deploy-config:
	@echo "\n⚙️  Applying cluster config..."
	kubectl apply -f webhook/dev/manifests/cluster-config/

.PHONY: delete-webhook
delete-webhook:
	@echo "\n♻️  Deleting webhook deployment if existing..."
	kubectl delete -f webhook/dev/manifests/webhook/ || true

.PHONY: deploy-webhook
deploy-webhook: push-webhook delete-webhook deploy-config
	@echo "\n🚀 Deploying webhook..."
	kubectl apply -f webhook/dev/manifests/webhook/

.PHONY: docker-build-controllers
docker-build-controllers:
	@echo "building sleeve-controller-manager docker image"
	docker build -t sleeve-controller-manager:latest -f controller-manager/Dockerfile .

.PHONY: containers
containers: docker-build-controllers docker-build-webhook
	@echo "building containers"

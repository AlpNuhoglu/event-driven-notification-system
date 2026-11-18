# Pinned tool and image versions (each defined once).
GO_IMAGE              = golang:1.27.4-trixie
GOLANGCI_LINT_VERSION = v2.15.0
SQLC_VERSION          = v1.31.1
OAPI_CODEGEN_VERSION  = v2.8.2
MIGRATE_VERSION       = v4.20.1

GOLANGCI_LINT = go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
SQLC          = go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
OAPI_CODEGEN  = go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)
MIGRATE       = go run -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)

TEST_CMD = go test -race -tags=integration -count=1 ./...

.DEFAULT_GOAL := help

.PHONY: help build generate lint fmt test test-docker tools image \
	test-e2e load demo up-mock verify-suite fuzz

help: ## Show this help
	@echo "Targets:"
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | \
		sed -e 's/:.*## /|/' | \
		awk -F'|' '{ printf "  %-14s %s\n", $$1, $$2 }'

build: ## Build all packages
	go build ./...

generate: ## Run sqlc, and oapi-codegen when its input exists
	$(SQLC) generate
	@if [ -f api/openapi.yaml ]; then \
		echo "oapi-codegen"; $(OAPI_CODEGEN) --config api/oapi-codegen.yaml api/openapi.yaml || exit 1; \
	else \
		echo "generate: skipped oapi-codegen, input file api/openapi.yaml is absent"; \
	fi

lint: ## Run golangci-lint (also checks formatting)
	$(GOLANGCI_LINT) run ./...

fmt: ## Format code with golangci-lint fmt
	$(GOLANGCI_LINT) fmt

test: ## Run all tests with the race detector and integration tag
	$(TEST_CMD)

test-docker: ## Run the tests inside the Go container (cgo, Docker socket)
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v "$(CURDIR)":"$(CURDIR)" -w "$(CURDIR)" \
		--add-host=host.docker.internal:host-gateway \
		-e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
		-e TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
		-v edns-gomod:/go/pkg/mod \
		-v edns-gobuild:/root/.cache/go-build \
		$(GO_IMAGE) $(TEST_CMD)

tools: ## Print the version of each pinned tool
	$(GOLANGCI_LINT) version
	$(SQLC) version
	$(OAPI_CODEGEN) --version
	$(MIGRATE) -version

image: ## Build the container image notifications:dev
	docker build -t notifications:dev .

test-e2e: ## Not implemented yet
	@echo "test-e2e: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

load: ## Not implemented yet
	@echo "load: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

demo: ## Not implemented yet
	@echo "demo: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

up-mock: ## Not implemented yet
	@echo "up-mock: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

verify-suite: ## Not implemented yet
	@echo "verify-suite: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

fuzz: ## Not implemented yet
	@echo "fuzz: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

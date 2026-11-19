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

COMPOSE_MOCK = docker-compose -f docker-compose.yml -f docker-compose.mock.yml

# make test-e2e runs the stack under its own Compose project, so it never
# reuses or removes the stack of make up-mock. The host ports can be changed
# when the defaults are taken (the other host ports as in .env.example).
# The tests receive E2E_PROJECT too: the Redis fallback scenario stops and
# starts the stack's redis service and reads the worker's /metrics through
# docker-compose. E2E_OVERRIDE shortens the durations the scenarios wait for
# (the circuit's open period after a provider_fatal answer); it is used here
# only, never by make up-mock.
E2E_PROJECT       ?= edns-e2e
E2E_TIMEOUT       ?= 10m
API_PORT          ?= 8080
GATEWAY_PORT      ?= 8082
MOCKPROVIDER_PORT ?= 8090
E2E_OVERRIDE       = test/e2e/docker-compose.e2e.yml
E2E_COMPOSE        = API_PORT=$(API_PORT) GATEWAY_PORT=$(GATEWAY_PORT) MOCKPROVIDER_PORT=$(MOCKPROVIDER_PORT) $(COMPOSE_MOCK) -f $(E2E_OVERRIDE) -p $(E2E_PROJECT)

TEST_CMD = go test -race -tags=integration -count=1 ./...

.DEFAULT_GOAL := help

.PHONY: help build generate lint fmt test test-docker tools image \
	test-e2e load demo up-mock down verify-suite fuzz

help: ## Show this help
	@echo "Targets:"
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | \
		sed -e 's/:.*## /|/' | \
		awk -F'|' '{ printf "  %-14s %s\n", $$1, $$2 }'

build: ## Build all packages
	go build ./...

generate: ## Run sqlc and oapi-codegen
	$(SQLC) generate
	$(OAPI_CODEGEN) --config api/oapi-codegen.yaml api/openapi.yaml

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

test-e2e: ## Compose stack with the mock override, e2e tests and load-smoke (needs Go and Docker)
	@rm -rf e2e-logs; \
	trap '$(E2E_COMPOSE) down -v --remove-orphans' EXIT; \
	trap 'exit 130' INT TERM; \
	status=0; \
	$(E2E_COMPOSE) up -d --build --wait || status=$$?; \
	if [ $$status -eq 0 ]; then \
		E2E_API_URL=http://localhost:$(API_PORT) E2E_GATEWAY_URL=ws://localhost:$(GATEWAY_PORT)/v1/ws E2E_MOCK_URL=http://localhost:$(MOCKPROVIDER_PORT) E2E_PROJECT=$(E2E_PROJECT) \
			go test -tags e2e -count=1 -v -timeout $(E2E_TIMEOUT) ./test/e2e/... || status=$$?; \
	fi; \
	if [ $$status -ne 0 ]; then \
		mkdir -p e2e-logs; \
		$(E2E_COMPOSE) ps -a > e2e-logs/ps.log 2>&1; \
		for svc in $$($(E2E_COMPOSE) config --services); do \
			$(E2E_COMPOSE) logs --no-color --timestamps $$svc > e2e-logs/$$svc.log 2>&1; \
		done; \
		echo "test-e2e: failed (exit $$status); service logs are in e2e-logs/" >&2; \
	fi; \
	exit $$status

load: ## Not implemented yet
	@echo "load: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

demo: ## Not implemented yet
	@echo "demo: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

up-mock: ## Start the stack with mockprovider as the provider
	$(COMPOSE_MOCK) up -d --build --wait

down: ## Stop the stack and remove its volumes
	$(COMPOSE_MOCK) down -v

verify-suite: ## Not implemented yet
	@echo "verify-suite: not implemented yet; see docs/plan/implementation-plan.md" >&2; exit 1

# Time box of each fuzz target. The minimisation of a new input is bounded
# as well: by default the engine may spend up to 60s on each one, and with
# the long inputs of the SMS counter that used up the whole time box.
FUZZTIME     ?= 30s
FUZZMINIMIZE ?= 1000x

fuzz: ## Run each fuzz target for FUZZTIME (default 30s)
	@for t in FuzzCountSMS FuzzDecodeCursor FuzzCursorRoundTrip FuzzParseRetryAfter FuzzCompileTemplate; do \
		echo "fuzz: $$t for $(FUZZTIME)"; \
		go test -run '^$$' -fuzz "^$$t$$" -fuzztime $(FUZZTIME) -fuzzminimizetime $(FUZZMINIMIZE) ./internal/domain/ || exit 1; \
	done

BUF ?= buf
GO ?= go
GITLEAKS ?= $(shell $(GO) env GOPATH)/bin/gitleaks
CLI_DIR ?= ../ags-cli
CLI_REVISION ?= faf0164263075bf957722c70d42ad48b2946ec16

BUF_VERSION := v1.47.2
PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_GEN_CONNECT_GO_VERSION := v1.18.1
GITLEAKS_VERSION := v8.30.0

.PHONY: all tools generate gen test test-race vet fmt-check sync-control-contract check-control-contract verify-contracts verify-docs verify-api verify-generate verify-consumers verify-e2e-compile verify-secrets verify test-e2e

all: verify

tools:
	$(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go@$(PROTOC_GEN_CONNECT_GO_VERSION)
	$(GO) install github.com/zricethezav/gitleaks/v8@$(GITLEAKS_VERSION)

generate gen:
	cd proto && PATH="$$($(GO) env GOPATH)/bin:$$PATH" $(BUF) generate

test:
	GOTOOLCHAIN=local $(GO) test ./...

test-race:
	GOTOOLCHAIN=local $(GO) test -race ./...

vet:
	GOTOOLCHAIN=local $(GO) vet ./...

fmt-check:
	@files="$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"; test -z "$$files" || { echo "gofmt required:"; echo "$$files"; exit 1; }

verify-contracts:
	GOTOOLCHAIN=local $(GO) run ./internal/cmd/controlcontract verify
	GOTOOLCHAIN=local $(GO) test ./internal/cmd/controlcontract
	GOTOOLCHAIN=local $(GO) test . -run 'Test(ControlPlaneActionRegistryMatchesContract|OfficialControlPlaneCallsStayInsideActionContract|MetricsRegistryMatchesContract|ErrorCodesMatchContract|ProtoHashesMatchContract)'

sync-control-contract:
	GOTOOLCHAIN=local $(GO) run ./internal/cmd/controlcontract sync --cli-dir "$(CLI_DIR)" --revision "$(CLI_REVISION)"

check-control-contract:
	GOTOOLCHAIN=local $(GO) run ./internal/cmd/controlcontract sync --cli-dir "$(CLI_DIR)" --revision "$(CLI_REVISION)" --check

verify-docs:
	GOTOOLCHAIN=local $(GO) run ./internal/cmd/verifyrepo

verify-api:
	GO="$(GO)" bash scripts/verify-api.sh

verify-generate:
	GO="$(GO)" bash scripts/verify-generate.sh

verify-consumers:
	GO="$(GO)" bash scripts/verify-consumers.sh

verify-e2e-compile:
	cd test && GOTOOLCHAIN=local $(GO) test -run '^$$' ./...

verify-secrets:
	$(GITLEAKS) detect --source=. --redact --no-banner
	$(GITLEAKS) dir --redact --no-banner .

verify: fmt-check test vet test-race verify-contracts verify-docs verify-api verify-generate verify-consumers verify-e2e-compile verify-secrets

test-e2e:
	GO="$(GO)" bash scripts/test-e2e.sh

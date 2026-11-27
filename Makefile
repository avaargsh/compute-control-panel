# Local helpers for a checkout of this repository.
#
# These targets mirror the cluster-free static steps in
# .github/workflows/go.yml (gofmt, go vet, go mod verify, and
# `go test -race -count=1 ./...`). They do not start PostgreSQL, apply
# migrations, or create a kind cluster. PostgreSQL integration tests skip
# when DATABASE_URL is unset, so `make test` is not the full Go validation job.
#
# GitHub Actions pins Go 1.27.4 with GOTOOLCHAIN=local. go.mod requires
# Go 1.27.0 or newer. This Makefile does not download a toolchain.

.PHONY: help build lint test demo clean

# Keep build and clean inside the documented repository-local output directory.
override BIN_DIR := bin

# VERSION, COMMIT, and DATE are embedded with -ldflags. VERSION defaults to
# git describe --tags --always --dirty, or dev when git is unavailable.
# DATE is UTC and uses SOURCE_DATE_EPOCH when that is set.
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
ifdef SOURCE_DATE_EPOCH
DATE := $(shell date -u -d '@$(SOURCE_DATE_EPOCH)' +%Y-%m-%dT%H:%M:%SZ)
else
DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
endif
LDFLAGS := -X github.com/avaargsh/compute-control-panel/internal/buildinfo.Version=$(VERSION) -X github.com/avaargsh/compute-control-panel/internal/buildinfo.Commit=$(COMMIT) -X github.com/avaargsh/compute-control-panel/internal/buildinfo.Date=$(DATE)

help:
	@echo "Targets:"
	@echo "  make build  - compile ccp-demo, compute-control-panel, migrate, and kueue-observe into $(BIN_DIR)/"
	@echo "               VERSION defaults to git describe --tags --always --dirty, or dev when git is unavailable."
	@echo "               Override with: make build VERSION=<version>"
	@echo "  make lint   - gofmt -l ., go vet ./..., and go mod verify (no database, no cluster)"
	@echo "  make test   - go test -race -count=1 ./... (PostgreSQL tests skip without DATABASE_URL)"
	@echo "  make demo   - synthetic Demo Console on 127.0.0.1:8081 (loopback only, not authenticated)"
	@echo "  make clean  - delete $(BIN_DIR)/ only; does not clear the Go build cache"

build:
	mkdir -p $(BIN_DIR)
	go build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/ccp-demo ./cmd/ccp-demo
	go build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/compute-control-panel ./cmd/compute-control-panel
	go build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/migrate ./cmd/migrate
	go build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/kueue-observe ./cmd/kueue-observe

lint:
	unformatted=$$(gofmt -l .) || exit $$?; \
	if [ -n "$$unformatted" ]; then \
		printf 'Unformatted Go files:\n%s\n' "$$unformatted"; \
		exit 1; \
	fi
	go vet ./...
	go mod verify

test:
	go test -race -count=1 ./...

demo:
	@echo "Starting synthetic Demo Console. Open http://127.0.0.1:8081 in a browser on this machine."
	@echo "This process is not production, is not authenticated, and rejects non-loopback binds."
	go run ./cmd/ccp-demo -addr 127.0.0.1:8081

clean:
	rm -rf $(BIN_DIR)

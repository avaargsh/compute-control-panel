# Compute Control Panel local development helpers

.PHONY: help test demo clean

help:
	@echo "Targets:"
	@echo "  make test   - run the full Go test suite (race, count=1)"
	@echo "  make demo   - start the loopback-only synthetic Demo Console on 127.0.0.1:8081"
	@echo "  make clean  - remove build artifacts if any"

test:
	go test -race -count=1 ./...

demo:
	@echo "Starting synthetic Demo Console. Open http://127.0.0.1:8081 in a browser on this machine."
	@echo "This is not production, not authenticated, and rejects non-loopback binds."
	go run ./cmd/ccp-demo -addr 127.0.0.1:8081

clean:
	go clean -cache -testcache

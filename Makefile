.PHONY: build test race fuzz crash model bench inspector lint vet fmt release docker repl

GO ?= go
BIN := bin
# Race detector needs cgo (a C compiler). Set CC if gcc is not installed.
FUZZTIME ?= 10s
CRASH_ITERS ?= 200
MODEL_SEEDS ?= 8
MODEL_STEPS ?= 3000

build:
	$(GO) build -o $(BIN)/nebuladb ./cmd/nebuladb
	$(GO) build -o $(BIN)/nebula ./cmd/nebula

vet:
	$(GO) vet ./...

fmt:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "gofmt needed" && exit 1)

test:
	$(GO) test ./...

race:
	CGO_ENABLED=1 $(GO) test -race ./...

# Short fuzz run of every fuzz target (one target per `go test -fuzz` invocation).
fuzz:
	./scripts/fuzz.sh $(FUZZTIME)

crash: build
	$(GO) run ./tests/crash -bin $(BIN)/nebuladb -iters $(CRASH_ITERS)

model:
	NEBULA_MODEL_SEEDS=$(MODEL_SEEDS) NEBULA_MODEL_STEPS=$(MODEL_STEPS) $(GO) test ./tests/model -count=1 -v

bench:
	$(GO) test ./... -run '^$$' -bench . -benchmem

inspector:
	cd inspector && npm install && npm run build

lint: vet fmt
	@which staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed; skipped (CI runs it)"

# Release archives for 6 OS/arch targets in dist/ (nothing is published).
release:
	./scripts/release.sh $(VERSION)

docker:
	docker build -t nebuladb:local --build-arg VERSION=$${VERSION:-dev} .

# Replication kill tests (spawn real servers; ~90 s).
repl:
	$(GO) test ./tests/repl -count=1 -v

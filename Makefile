.PHONY: build test race fuzz crash bench inspector lint vet fmt

GO ?= go
BIN := bin
# Race detector needs cgo (a C compiler). Set CC if gcc is not installed.
FUZZTIME ?= 10s
CRASH_ITERS ?= 200

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

# Short fuzz run of every fuzz target (go test -fuzz accepts one target per package run).
fuzz:
	@for pkg in $$($(GO) list ./...); do \
	  for t in $$($(GO) test -list 'Fuzz.*' $$pkg | grep '^Fuzz'); do \
	    echo "== fuzz $$pkg $$t"; \
	    $(GO) test $$pkg -run '^$$' -fuzz "^$$t$$" -fuzztime $(FUZZTIME) || exit 1; \
	  done; \
	done

crash: build
	$(GO) run ./tests/crash -bin $(BIN)/nebuladb -iters $(CRASH_ITERS)

bench:
	$(GO) test ./... -run '^$$' -bench . -benchmem

inspector:
	cd inspector && npm install && npm run build

lint: vet fmt
	@which staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed; skipped (CI runs it)"

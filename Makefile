VERSION := 0.1.0
OUT     := dist
GO      ?= go

.PHONY: all build test vet fmt freebsd linux clean

all: build

build:
	$(GO) build ./...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

# CPA loads plugins as C shared objects, so -buildmode=c-shared is required.
freebsd:
	@mkdir -p $(OUT)
	CGO_ENABLED=1 GOOS=freebsd GOARCH=amd64 \
		$(GO) build -buildmode=c-shared -trimpath -buildvcs=false \
		-o $(OUT)/riverbridge.so ./cmd/riverbridge/

linux:
	@mkdir -p $(OUT)
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
		$(GO) build -buildmode=c-shared -trimpath -buildvcs=false \
		-o $(OUT)/riverbridge.so ./cmd/riverbridge/

clean:
	rm -rf $(OUT)

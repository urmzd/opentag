.PHONY: all init build test lint fmt check run proto install

MOD := $(shell basename $(CURDIR))
CMD := cmd/$(MOD)

all: check

init:
	git config core.hooksPath .githooks
	go mod download && go mod tidy

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/$(MOD) ./$(CMD)

test:
	go test ./...

lint:
	golangci-lint run

fmt:
	gofmt -w .

check: fmt lint test

run: build
	./bin/$(MOD) serve

proto:
	buf generate

install:
	CGO_ENABLED=0 go install -trimpath -ldflags="-s -w" ./$(CMD)

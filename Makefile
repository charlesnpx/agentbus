.PHONY: build test race lint tidy

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./engine/... ./internal/...

lint:
	go vet ./...

tidy:
	go mod tidy

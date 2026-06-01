.PHONY: build test lint run-dev

build:
	go build -o bin/clob-server ./cmd/server

test:
	go test ./...

lint:
	golangci-lint run ./...

run-dev:
	docker compose up --build

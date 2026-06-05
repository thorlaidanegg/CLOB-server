.PHONY: build test test-integration test-all lint run-dev test-db-up test-db-down seed

build:
	go build -o bin/clob-server ./cmd/server

# Unit tests + no-infra integration tests (engine boundary). Postgres tests skip.
test:
	go test ./...

# Run an ephemeral Postgres for integration tests.
test-db-up:
	docker run -d --name clob-test-pg \
		-e POSTGRES_DB=clob_test -e POSTGRES_USER=clob -e POSTGRES_PASSWORD=clob \
		-p 55432:5432 postgres:16-alpine

test-db-down:
	docker rm -f clob-test-pg

# Run every test including Postgres-backed integration tests.
# Requires a running Postgres (see test-db-up).
test-integration:
	TEST_POSTGRES_DSN="postgres://clob:clob@localhost:55432/clob_test?sslmode=disable" \
		go test ./... -count=1

# Bring up the DB, run everything, tear down.
test-all: test-db-up
	sleep 3
	$(MAKE) test-integration; status=$$?; $(MAKE) test-db-down; exit $$status

lint:
	golangci-lint run ./...

run-dev:
	docker compose up --build

# Seed a couple of markets + a demo user (run after the stack is up).
seed:
	bash scripts/seed.sh

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Claude's role in this repository (mandatory)

- **Claude owns the test suite.** Writing and maintaining tests (`*_test.go` files, plus test fixtures/helpers that live only in `_test.go` files or `testdata/` directories) is Claude's responsibility.
- **Claude is forbidden from writing any code other than tests.** This includes `main.go`, everything under `src/` that is not a `_test.go` file, `docker-compose.yaml`, `.env.exemple`, and any other production/infra code. No new production files, no edits, no "small fixes" — even when a bug is found or a test would be easier to write after a refactor.
- When a test exposes a bug or the production code is hard to test (e.g. `FailOnError` calling `os.Exit`, no interfaces to fake `amqp` types), Claude must **describe** the needed change to the user (file, line, what and why) and leave it for the user to implement. A test that documents a known bug may be written but must be clearly marked (e.g. `t.Skip` with the reason) so the suite still passes.
- Adding a test-only dependency to `go.mod`/`go.sum` (e.g. a mocking or testcontainers library) requires asking the user first.
- Reviews, explanations and documentation changes (like this file) are still allowed when the user asks.
- The work plan lives in `TODO.md` and the target architecture (including the decision to inject `*slog.Logger` via constructors) in `docs/ARCHITECTURE.md`. Tick items in `TODO.md` as tests land.

## Project overview

A small Go application demonstrating a RabbitMQ consumer with a dead-letter queue (DLQ) pattern. On startup it declares a main exchange/queue plus a paired dead-letter exchange/queue, then consumes messages: a handler error triggers `Nack` (routing the message to the DLQ), while success triggers `Ack`.

## Commands

```sh
# Start RabbitMQ (management UI on :15672, AMQP on :5672)
docker-compose up -d

# Run the consumer (requires a .env file, see below)
go run ./cmd/consumer   # from the repo root: LoadEnv reads .env relative to the cwd

# Build
go build ./...

# Tidy / verify dependencies
go mod tidy

# Run tests (maintained by Claude, see above)
go test ./...

# Run a single test
go test ./internal/config -run TestName -v

# Integration tests (build tag `integration`; need the docker-compose RabbitMQ running)
go test -tags=integration ./...
```

Unit tests run with plain `go test ./...`. Integration tests live in `*_integration_test.go` files behind the `integration` build tag; they read credentials from the environment or the repo-root `.env`, skip when the broker is unreachable, use a unique `it-<nanos>-<n>` service name per test and delete their exchanges/queues on cleanup. Tests documenting known bugs are marked with `t.Skip("BUG conhecido ...")` and must be un-skipped once the fix lands. gopls needs `"buildFlags": ["-tags=integration"]` to analyze the integration files.

### Environment setup

Copy `.env.exemple` to `.env` (note the filename typo — "exemple", not "example") and fill in:

- `RABBITMQ_DEFAULT_USER`, `RABBITMQ_DEFAULT_PASS` — required, no defaults
- `RABBITMQ_DEFAULT_HOST` (default `localhost`), `RABBITMQ_DEFAULT_PORT` (default `5672`)
- `SERVICE_NAME` (default `exemple`) — used to derive exchange/queue names

`docker-compose.yaml` only consumes `RABBITMQ_DEFAULT_USER`/`RABBITMQ_DEFAULT_PASS` from the environment (via shell/`.env`), so make sure those are exported when running `docker-compose up`.

## Architecture

Layout is in transition towards `docs/ARCHITECTURE.md` (step A1 — move to `cmd/` + `internal/` — is done).

- `cmd/consumer/main.go` — wires everything together: loads env, configures the logger, builds the AMQP URL, constructs a `RabbitMQConsumer`, and starts consuming with a `handler(body string) error` callback (currently just prints the message body). Builds the AMQP URL with `net/url` (`url.UserPassword` + `net.JoinHostPort`), so credentials with special characters are escaped; the URL is not logged.
- `internal/messaging/consumer.go` — the `Consumer` port (`Consume(handler func(message string) error)`). Constructors return concrete types (`*RabbitMQConsumer`), not this interface.
- `internal/rabbitmq/consumer.go` — the RabbitMQ adapter and the core of the project:
  - `NewRabbitMQConsumer(url, serviceName)` opens the connection/channel and calls `createQueueWithDlq`, which declares **two** exchange+queue+bind sets per service: `{serviceName}-dlx`/`{serviceName}-dlq` first, then `{serviceName}-exchange`/`{serviceName}-queue` configured with `x-dead-letter-exchange` pointing at the dlx. This ordering matters — the DLX must exist before the main queue references it.
  - `Consume` runs the delivery loop in a goroutine and blocks the calling goroutine forever on a nil `chan struct{}` (`forever`) — there is no clean shutdown, and if the connection drops the process hangs without consuming. `StopConsumer` exists but its `defer` in `main.go` never runs (Ctrl+C kills the process; `os.Exit` skips defers).
  - No `channel.Qos` prefetch is configured.
  - Nack is called with requeue=`false` (`message.Nack(false, false)`), so failed messages go to the DLQ rather than looping back onto the main queue.
- `internal/logger/logger.go` — thin wrapper around `log/slog` with JSON output at Debug level; `SetLoggerSettings()` must be called once at startup. The `Info/Warn/Error/Debug` wrappers are currently unused. Temporary: goes away once the logger is injected (A4/A6).
- `internal/config/env.go` — `.env` loading (`godotenv`) plus `GetEnv`/`GetEnvOrDefault` helpers. Base for step A2.
- `internal/custom_errors/error.go` — `FailOnError` logs via `slog.Error` and then calls `os.Exit(1)`. Temporary: goes away once errors are returned (A6). Consequences for testing: any code path that hits it on error kills the test binary, so error paths in `internal/rabbitmq` (and `NewRabbitMQConsumer`, which always returns a `nil` error) cannot be unit-tested as-is — report this to the user rather than changing it.

## Notes for making changes

- Naming convention for queues/exchanges is derived entirely from `serviceName`: `{serviceName}-exchange`, `{serviceName}-queue`, `{serviceName}-dlx`, `{serviceName}-dlq`. Keep this convention if adding new queue types.
- The codebase mixes Portuguese and English (log messages, README, error strings are Portuguese; identifiers are English). Match the existing language per context rather than converting wholesale.

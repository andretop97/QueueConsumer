# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Claude's role in this repository (mandatory)

- **Claude owns the test suite.** Writing and maintaining tests (`*_test.go` files, plus test fixtures/helpers that live only in `_test.go` files or `testdata/` directories) is Claude's responsibility.
- **Claude is forbidden from writing any code other than tests.** This includes `main.go`, everything under `src/` that is not a `_test.go` file, `docker-compose.yaml`, `.env.exemple`, and any other production/infra code. No new production files, no edits, no "small fixes" — even when a bug is found or a test would be easier to write after a refactor.
- When a test exposes a bug or the production code is hard to test (e.g. no interfaces to fake `amqp` types), Claude must **describe** the needed change to the user (file, line, what and why) and leave it for the user to implement. A test that documents a known bug may be written but must be clearly marked (e.g. `t.Skip` with the reason) so the suite still passes.
- Adding a test-only dependency to `go.mod`/`go.sum` (e.g. a mocking or testcontainers library) requires asking the user first.
- Reviews, explanations and documentation changes (like this file) are still allowed when the user asks.
- The work plan lives in `TODO.md` and the target architecture (including the decision to inject `*slog.Logger` via constructors) in `docs/ARCHITECTURE.md`. Tick items in `TODO.md` as tests land.

## Project overview

A small Go application demonstrating a RabbitMQ consumer with a dead-letter queue (DLQ) pattern. On startup it declares a main exchange/queue plus a paired dead-letter exchange/queue, then consumes messages: a handler error triggers `Nack` (routing the message to the DLQ), while success triggers `Ack`.

## Commands

The module targets Go 1.26 (`go.mod`), so newer stdlib/testing APIs are fine: `slices.Backward`, `slog.DiscardHandler`, `t.Chdir`, `t.Context()`.

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

# Integration tests (build tag `integration`; need the docker-compose RabbitMQ running and
# all RABBITMQ_DEFAULT_* vars set, via environment or repo-root .env)
go test -tags=integration ./...
```

Unit tests run with plain `go test ./...`. Integration tests live in `*_integration_test.go` files behind the `integration` build tag; they read credentials from the environment or the repo-root `.env`, skip when the broker is unreachable, use a unique `it-<nanos>-<n>` service name per test and delete their exchanges/queues on cleanup. Tests documenting known bugs are marked with `t.Skip("BUG conhecido ...")` and must be un-skipped once the fix lands. gopls needs `"buildFlags": ["-tags=integration"]` to analyze the integration files.

### Environment setup

Copy `.env.exemple` to `.env` (note the filename typo — "exemple", not "example") and fill in:

- `RABBITMQ_DEFAULT_USER`, `RABBITMQ_DEFAULT_PASS` — required (`notEmpty`), no defaults
- `RABBITMQ_DEFAULT_HOST` (default `localhost`), `RABBITMQ_DEFAULT_PORT` (default `5672`)
- `SERVICE_NAME` (default `QueueConsumer`) — used to derive exchange/queue names
- `LOG_LEVEL` (default `INFO`; `DEBUG`/`INFO`/`WARN`/`ERROR`) — applied to the JSON logger built in `run()`

Config is loaded by `config.Load()` (`internal/config/env.go`): `godotenv.Load()` reads `.env` from the current directory (existing env vars win), then `caarlos0/env` parses the `Config` struct from tags, then `validate()` applies semantic rules (spec in `internal/config/config_test.go`).

`docker-compose.yaml` only consumes `RABBITMQ_DEFAULT_USER`/`RABBITMQ_DEFAULT_PASS` from the environment (via shell/`.env`), so make sure those are exported when running `docker-compose up`.

## Architecture

Layout is in transition towards `docs/ARCHITECTURE.md` (steps A1–A6 are done; see `TODO.md`).

- `cmd/consumer/main.go` — `main` only calls `run() error` and exits with code 1 on error (so `run`'s defers always execute). `run` loads config via `config.Load()`, builds a JSON `*slog.Logger` at `cfg.LogLevel`, creates a ctx with `signal.NotifyContext` (Ctrl+C/SIGTERM), constructs the `RabbitMQConsumer` (defer `StopConsumer`) and calls `Consume(ctx, Chain(HandlerFunc(handle), Logging(logger), Recover()))`. `handle(ctx, msg) error` just prints the body. The URL is not logged.
- `internal/messaging/` — broker-agnostic core (stdlib only, step A3): `Message{Body, Headers, Attempts}` (`Attempts` = previous failures, 0 on first delivery), `Handler` + `HandlerFunc`, `Consumer` port (`Consume(ctx, Handler) error`), `ErrPermanent` sentinel (match with `errors.Is`), and the pure `Decide(err, attempts, maxRetries) Action` policy. `Action` starts at `iota + 1` so the zero value is never `Ack`. With `maxRetries = N` a always-failing message runs N+1 times. `rabbitmq.RabbitMQConsumer` implements the `Consumer` port (checked by `var _ messaging.Consumer = (*RabbitMQConsumer)(nil)`). Middlewares (step A4, `middleware.go`): `Middleware func(Handler) Handler`; `Chain(h, mws...)` makes the first middleware outermost; `Recover()` turns a panic into an error matching both `ErrPanic` and `ErrPermanent` (→ `DeadLetter`); `Logging(logger)` emits one line per message (INFO on success, ERROR with `error` on failure; fields `attempts`, `body_bytes`, `duration`; never the body) and returns the handler's error unchanged. Recommended order: `Chain(h, Logging(l), Recover())`. Constructors return concrete types, not interfaces.
- `internal/rabbitmq/topology.go` — `Topology{Exchange, Queue, DLX, DLQ}` (step A5). `NewTopology(serviceName)` is pure (just derives the names); `Declare(ch)` declares **two** exchange+queue+bind sets and returns errors (no `FailOnError`): `{serviceName}-dlx`/`{serviceName}-dlq` first, then `{serviceName}-exchange`/`{serviceName}-queue` with `x-dead-letter-exchange` pointing at the dlx. This ordering matters — the DLX must exist before the main queue references it.
- `internal/rabbitmq/consumer.go` — the RabbitMQ adapter:
  - `NewRabbitMQConsumer(url, serviceName, logger)` stores `logger.With("component", "rabbitmq")` (`nil` → `slog.New(slog.DiscardHandler)`), opens the connection/channel (errors are returned; whatever was already opened is closed), then builds and declares the `Topology`, returning its error. The consumer keeps the `Topology` in an unexported `topology` field and consumes from `topology.Queue`.
  - `Consume(ctx, messaging.Handler) error` (step A6) uses `ConsumeWithContext` and loops in the calling goroutine: ctx canceled → returns `nil` (also when already canceled or when the deliveries channel closes because of the cancel); deliveries channel closed otherwise (connection drop) → returns an error. Each delivery becomes a `messaging.Message` (`Attempts` is always 0 until A8), the handler runs with `context.WithoutCancel(ctx)` so an in-flight message finishes on shutdown, and `Decide(err, attempts, maxRetries)` (`maxRetries = 0`) picks `Ack` or `Nack`. All logging goes through the injected logger (never the global `slog`).
  - `StopConsumer` only closes the connection (which closes its channels). Closing the channel first raced with the `Cancel` that `ConsumeWithContext` issues after a ctx cancel and could hang; `TestStopConsumerRightAfterCancelDoesNotHang` guards against regressions.
  - No `channel.Qos` prefetch is configured.
  - Nack is called with requeue=`false` (`message.Nack(false, false)`), so failed messages go to the DLQ rather than looping back onto the main queue.
- `internal/config/env.go` — `Config`/`RabbitMQ` structs, `Load()`, `validate()` and `RabbitMQ.URL()` (built with `url.UserPassword` + `net.JoinHostPort`, so special characters and IPv6 hosts are handled). Step A2.

## Notes for making changes

- Naming convention for queues/exchanges is derived entirely from `serviceName`: `{serviceName}-exchange`, `{serviceName}-queue`, `{serviceName}-dlx`, `{serviceName}-dlq`. Keep this convention if adding new queue types.
- The codebase mixes Portuguese and English (log messages, README, error strings are Portuguese; identifiers are English). Match the existing language per context rather than converting wholesale.

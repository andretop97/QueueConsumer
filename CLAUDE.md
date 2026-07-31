# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A small Go application demonstrating a RabbitMQ consumer with a dead-letter queue (DLQ) pattern. On startup it declares a main exchange/queue plus a paired dead-letter exchange/queue, then consumes messages: a handler error triggers `Nack` (routing the message to the DLQ), while success triggers `Ack`.

## Commands

```sh
# Start RabbitMQ (management UI on :15672, AMQP on :5672)
docker-compose up -d

# Run the consumer (requires a .env file, see below)
go run main.go

# Build
go build ./...

# Tidy / verify dependencies
go mod tidy
```

There is no test suite in this repo currently.

### Environment setup

Copy `.env.exemple` to `.env` (note the filename typo — "exemple", not "example") and fill in:
- `RABBITMQ_DEFAULT_USER`, `RABBITMQ_DEFAULT_PASS` — required, no defaults
- `RABBITMQ_DEFAULT_HOST` (default `localhost`), `RABBITMQ_DEFAULT_PORT` (default `5672`)
- `SERVICE_NAME` (default `exemple`) — used to derive exchange/queue names
- `QUEUE_NAME` — declared in `.env.exemple` but not currently read by the code

`docker-compose.yaml` only consumes `RABBITMQ_DEFAULT_USER`/`RABBITMQ_DEFAULT_PASS` from the environment (via shell/`.env`), so make sure those are exported when running `docker-compose up`.

## Architecture

- `main.go` — wires everything together: loads env, configures the logger, builds the AMQP URL, constructs a `RabbitMQConsumer`, and starts consuming with a `handler(body string) error` callback (currently just prints the message body).
- `src/consumer/IConsumer.go` — defines the `IConsumer` interface (`Consume(handler func(message string) error)`) that consumer implementations should satisfy.
- `src/consumer/RabbitmqConsumer.go` — the RabbitMQ implementation and the core of the project:
  - `NewRabbitMQConsumer(url, serviceName)` opens the connection/channel and calls `createQueueWithDlq`, which declares **two** exchange+queue+bind sets per service: `{serviceName}-dlx`/`{serviceName}-dlq` first, then `{serviceName}-exchange`/`{serviceName}-queue` configured with `x-dead-letter-exchange` pointing at the dlx. This ordering matters — the DLX must exist before the main queue references it.
  - `Consume` runs the delivery loop in a goroutine and blocks the calling goroutine forever on an unbuffered `chan struct{}` (`forever`) — there is currently no clean shutdown signal wired to it (`StopConsumer` exists but isn't hooked to an OS signal).
  - Nack is called with requeue=`false` (`message.Nack(false, false)`), so failed messages go to the DLQ rather than looping back onto the main queue.
- `src/logger/logger.go` — thin wrapper around `log/slog` with JSON output at Debug level; `SetLoggerSettings()` must be called once at startup before other logging helpers are used.
- `src/utils/env.go` — `.env` loading (`godotenv`) plus `GetEnv`/`GetEnvOrDefault` helpers.
- `src/utils/error.go` — `FailOnError` logs via `slog.Error` but **does not** exit or panic on error; callers that need to stop execution on failure must check the returned error themselves (several call sites in `RabbitmqConsumer.go` currently rely on `FailOnError` alone and will continue executing with a nil/zero-value receiver if the preceding call actually failed).

## Notes for making changes

- Naming convention for queues/exchanges is derived entirely from `serviceName`: `{serviceName}-exchange`, `{serviceName}-queue`, `{serviceName}-dlx`, `{serviceName}-dlq`. Keep this convention if adding new queue types.
- The codebase mixes Portuguese and English (log messages, README, error strings are Portuguese; identifiers are English). Match the existing language per context rather than converting wholesale.

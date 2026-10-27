# Arquitetura-alvo

Este documento descreve para onde o projeto está evoluindo. O estado atual está descrito no `CLAUDE.md`; a sequência de trabalho está no `TODO.md`.

## Estrutura

```
cmd/consumer/main.go          → só "wiring": config, logger, broker, handler, sinais
internal/
  config/      config.go      → struct Config + Load() (valida obrigatórios, monta URL)
  messaging/   message.go     → Message{Body []byte, Headers, Attempts}, Handler, Consumer, erros
               middleware.go  → Recover, Logging (envolvem um Handler)
               decision.go    → Decide(err, attempts, maxRetries) → Ack | Retry | DeadLetter
  rabbitmq/    topology.go    → Topology{Exchange, Queue, DLX, DLQ} derivada do serviceName + Declare()
               connection.go  → Dial, NotifyClose, reconexão/encerramento
               consumer.go    → implementa messaging.Consumer usando Decide()
  handlers/    print.go       → regra de negócio (hoje: imprimir o body)
```

Dependências apontam para dentro: `cmd` → `rabbitmq`, `handlers`, `config` → `messaging`. O pacote `messaging` não depende de nenhum outro.

## Contratos centrais

```go
type Handler interface { Handle(ctx context.Context, msg Message) error }
type Consumer interface { Consume(ctx context.Context, h Handler) error }

var ErrPermanent = errors.New("permanent") // vai direto para a DLQ, sem retry
```

## Decisões

1. **`Decide` é função pura** — a regra de ack/retry/DLQ é testável sem broker.
2. **Logger injetado via parâmetro do construtor** — ver seção abaixo.
3. **Erros sempre retornados** — só `main` encerra o processo; `FailOnError`/`os.Exit` não aparecem em pacotes `internal/`.
4. **Middleware `Recover`** — panic no handler vira erro e a mensagem segue o fluxo normal.
5. **`Topology` como tipo próprio** — centraliza a convenção `{serviceName}-exchange|queue|dlx|dlq`.
6. **Concorrência configurável** (N workers) junto com QoS/prefetch.

## Logging: injeção de dependência

- `main` é o **único** lugar que cria o `*slog.Logger` (JSON, nível vindo de `Config`). O pacote `src/logger` deixa de existir.
- Construtores recebem o logger como parâmetro: `rabbitmq.NewConsumer(url, topo, logger)`.
- `nil` nunca é aceito silenciosamente: substituir por `slog.New(slog.NewTextHandler(io.Discard, nil))`.
- Contexto via `.With()`: `logger.With("component", "rabbitmq")` na criação, `log.With("message_id", ...)` por mensagem. Nada de `slog.Default()`/`slog.SetDefault` fora do `main`.
- Funções puras (ex.: `Decide`) não logam.
- Pacotes priorizam **retornar erros**; logam apenas eventos que não são erro (reconexão, topologia declarada).
- Nos testes, cada caso injeta um logger sobre um `bytes.Buffer` próprio, permitindo `t.Parallel()` e asserções sobre os logs.

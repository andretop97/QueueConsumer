# TODO

Divisão de responsabilidades (ver `CLAUDE.md`): **[Claude]** escreve apenas testes; **[Você]** implementa o código de produção. Arquitetura-alvo em `docs/ARCHITECTURE.md`.

## Fase 1 — Testes sem mexer em produção

- [x] **[Claude]** `src/utils/env_test.go`: `GetEnvOrDefault` (definida/ausente/vazia), `GetEnv`, `LoadEnv` com e sem `.env`
- [x] **[Claude]** `src/utils/error_test.go`: `FailOnError` com `nil` não faz nada; com erro sai com código 1 (subprocesso)
- [x] **[Claude]** `main_test.go`: `getRabbitURLfromEnv`, `handler`; senha com `@` documentada com `t.Skip`
- [x] **[Claude]** `src/logger/logger_test.go`: `SetLoggerSettings` emite JSON em nível Debug

## Fase 2 — Integração com o RabbitMQ do docker-compose

- [x] **[Claude]** Suíte com build tag `integration` (`go test -tags=integration ./...`), pulando se o broker estiver inacessível: topologia `{svc}-*`, arg `x-dead-letter-exchange`, ack no sucesso, DLQ no erro
- [x] **[Claude]** Teste documentando o bug do `queueName` vazio (`t.Skip`)

## Fase 3 — Correções rápidas

- [x] **[Você]** Setar `consumer.queueName` em `createQueueWithDlq` → **[Claude]** remove o `Skip` correspondente
- [x] **[Você]** Não logar a URL com senha; montar URL com `url.URL` + `url.UserPassword` → **[Claude]** remove o `Skip` correspondente

## Fase 4 — Arquitetura

| # | [Você] implementa | [Claude] testa |
|---|---|---|
| A1 | Mover para `cmd/` + `internal/`, renomear arquivos para minúsculo | Ajustar testes aos novos pacotes |
| A2 | `internal/config` | Obrigatórios ausentes, defaults, URL com caracteres especiais |
| A3 | `messaging`: `Message`, `Handler`, `ErrPermanent`, `Decide` | Tabela de casos do `Decide` |
| A4 | Middlewares `Recover` e `Logging` (logger injetado) | Panic vira erro; log contém os campos esperados |
| A5 | `rabbitmq/topology.go` | Nomes derivados; integração verifica args de DLX |
| A6 | `rabbitmq/consumer.go` com `ctx`, erros retornados, logger injetado | Integração: ack, DLQ, cancelamento do ctx encerra |
| A7 | `connection.go` com `NotifyClose` | Queda simulada via API de management |
| A8 | Retry com TTL + `x-death` → `Attempts` | Reprocessa N vezes e depois DLQ |
| A9 | Workers + QoS | Processamento concorrente sem perder ack |
| A10 | Dockerfile + healthcheck no compose | — |

## Fase 5 — Limpeza

- [ ] **[Você]** Remover código morto (`createSimpleQueue`, wrappers do logger, comentário do `Reject`), corrigir typos ("successfuly", "Filed", "exemple"), renomear `IConsumer`

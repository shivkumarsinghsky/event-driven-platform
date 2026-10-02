# Event-Driven Platform — RabbitMQ, Outbox, Idempotent Consumers, Retries and DLQ

[![CI](https://github.com/shivkumarsinghsky/event-driven-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/shivkumarsinghsky/event-driven-platform/actions/workflows/ci.yml)
![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178c6)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-ff6600)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![License](https://img.shields.io/badge/license-MIT-green)

An **event-driven architecture** reference implementation by **Shiv Kumar**. Three Node.js/TypeScript services —
orders, inventory and notifications — collaborate only through events on **RabbitMQ**, each owning its own
**PostgreSQL** schema.

The repository focuses on the parts of event-driven systems that are easy to get wrong:

- publishing events reliably (transactional outbox),
- consuming them safely more than once (inbox-based idempotency),
- retrying with delays and dead-lettering poison messages,
- evolving event contracts (versioning and upcasting), and
- tracing a business flow across services (correlation and causation ids).

All of this is covered by integration tests that run against a real broker and database.

## Architecture

```mermaid
flowchart LR
    Client -->|"POST /orders"| Order["order-service<br/>API + relay + consumer"]
    Order -->|"TX: order + outbox"| ODB[("orders")]
    Order -->|"order.placed.v2"| EX{{"RabbitMQ<br/>domain.events"}}
    EX --> Inv["inventory-service<br/>consumer + relay"]
    Inv -->|"TX: inbox + reserve + outbox"| IDB[("inventory")]
    Inv -->|"inventory.reserved / rejected"| EX
    EX --> Order
    Order -->|"order.status-changed.v1"| EX
    EX --> Notif["notification-service<br/>consumer"]
    Notif --> NDB[("notification")]
    EX -.->|"failures"| Retry["retry queues<br/>TTL 1s / 5s / 30s"]
    Retry -.-> EX
    EX -.->|"permanent or exhausted"| DLQ["dead-letter queues"]
```

Full description, sequence diagram and topology: [docs/architecture.md](docs/architecture.md).

## Key Capabilities

| Capability | Where |
|---|---|
| Producers with **transactional outbox** and publisher confirms | [`src/platform/outbox`](src/platform/outbox/outbox.ts) |
| Consumers with **inbox idempotency**, manual acks, prefetch | [`src/platform/messaging/consumer.ts`](src/platform/messaging/consumer.ts) |
| **Delayed retries** (TTL queues) and **dead-letter queues** | [`retry-policy.ts`](src/platform/messaging/retry-policy.ts), [`topology.ts`](src/platform/messaging/topology.ts) |
| **Event contracts** with zod and an envelope (id, type, version, correlation/causation) | [`src/platform/contracts`](src/platform/contracts) |
| **Event versioning**: `order.placed` v1 → v2 upcasting | [`events.ts`](src/platform/contracts/events.ts) |
| **Idempotent HTTP API** (`Idempotency-Key`) | [`src/services/order/api.ts`](src/services/order/api.ts) |
| **Correlation ids** from HTTP through every event | envelope + Fastify request id |
| **Health/readiness/metrics** on every process, graceful shutdown | [`src/platform/runtime.ts`](src/platform/runtime.ts) |
| **DLQ replay** tool and runbook | [`scripts/replay-dlq.ts`](scripts/replay-dlq.ts), [runbook](docs/runbooks/dlq-replay.md) |

## How It Works

1. `POST /orders` validates input, then in **one transaction** inserts the order (`PENDING`), claims the
   idempotency key and writes `order.placed.v2` to the outbox. It returns `202 Accepted`.
2. The **outbox relay** publishes unpublished rows with publisher confirms and marks them published.
3. **inventory-service** consumes `order.placed.*`, upcasts v1 payloads, and in one transaction records the
   message in its **inbox**, reserves stock all-or-nothing (savepoint) and writes `inventory.reserved` or
   `inventory.rejected` to its outbox.
4. **order-service** consumes the result and moves the order to `CONFIRMED` or `REJECTED` — only from `PENDING`,
   so duplicates and late events are no-ops — and publishes `order.status-changed.v1`.
5. **notification-service** calls a (simulated, configurably flaky) provider using the event id as the
   provider's idempotency key, then records the notification.

### Choreography vs. orchestration

This flow is **choreographed** — no service coordinates the others. That keeps services decoupled for a short
flow; [ADR-006](docs/decisions/ADR-006-choreography-for-order-flow.md) explains when an orchestrator would be the
better choice.

### Eventual consistency and delivery semantics

| Stage | Guarantee |
|---|---|
| DB change → event | Atomic (outbox) |
| Outbox → broker → consumer | At-least-once |
| Consumer DB effects | Effectively-once (inbox in the same transaction) |
| External effects (email) | At-least-once, deduplicated via provider idempotency key |

The design does not rely on message ordering; see [Ordering](docs/architecture.md#ordering).

## Technology Stack

| Area | Choice |
|---|---|
| Language / runtime | TypeScript 5 (strict), Node.js 22 |
| HTTP | Fastify 5 |
| Messaging | RabbitMQ 3.13 via `amqplib` (topic exchange, confirm channels) |
| Database | PostgreSQL 16 via `pg`; one schema per service |
| Validation | zod |
| Logging / metrics | pino (JSON), prom-client |
| Tests | Vitest (unit + integration against real services) |
| Packaging | Docker multi-stage image, Docker Compose |

## Repository Structure

```text
event-driven-platform/
├── src/
│   ├── config.ts                 # validated environment configuration
│   ├── platform/                 # reusable messaging platform
│   │   ├── contracts/            # envelope + event schemas + upcasters
│   │   ├── messaging/            # broker, consumer, topology, retry policy, error classification
│   │   ├── outbox/               # transactional outbox + relay
│   │   ├── http/                 # health, readiness, metrics, correlation ids
│   │   ├── db.ts, logger.ts, metrics.ts
│   │   └── runtime.ts            # service bootstrap, roles, graceful shutdown
│   └── services/
│       ├── order/                # API + inventory-result handlers
│       ├── inventory/            # stock reservation handler
│       └── notification/         # notification handler + simulated provider
├── migrations/<schema>/          # SQL migrations per service
├── tests/unit/                   # pure logic: retry policy, contracts, domain, config
├── tests/integration/            # real PostgreSQL + RabbitMQ: end-to-end flow, consumer semantics
├── scripts/replay-dlq.ts         # move messages from a DLQ back to its queue
├── docker/Dockerfile, docker-compose.yml
└── docs/                         # architecture, event catalogue, runbook, ADRs
```

## Getting Started

### Option A — everything in Docker

```bash
git clone https://github.com/shivkumarsinghsky/event-driven-platform.git
cd event-driven-platform
docker compose up -d --build
```

Services: order-service `:3001`, inventory-service `:3002`, notification-service `:3003`, RabbitMQ management UI
`:15672` (user `edp`, password `edp-local-dev`, local only).

### Option B — services on the host, infrastructure in Docker

```bash
npm ci
cp .env.example .env
docker compose up -d postgres rabbitmq
npm run dev:order          # terminal 1
npm run dev:inventory      # terminal 2
npm run dev:notification   # terminal 3
```

## Configuration

All configuration comes from environment variables, validated at startup (the process exits with a clear
message if something is invalid). See [`.env.example`](.env.example):

| Variable | Purpose | Default |
|---|---|---|
| `SERVICE_NAME` | Service name in logs and metrics | — (required) |
| `DATABASE_URL`, `RABBITMQ_URL` | Connections | — (required) |
| `ROLES` | Roles this process runs: `api`, `relay`, `consumer` | all |
| `HTTP_PORT` | Health/metrics/API port | `3000` |
| `PREFETCH` | Unacked messages per consumer | `10` |
| `RETRY_DELAYS_MS` | Retry delays before dead-lettering | `1000,5000,30000` |
| `OUTBOX_POLL_INTERVAL_MS`, `OUTBOX_BATCH_SIZE` | Relay tuning | `500`, `100` |
| `NOTIFICATION_FAILURE_RATE` | Simulated provider failure probability | `0` (`0.3` in compose) |

No secrets are committed; compose and `.env.example` contain local development defaults only.

## API Examples

```http
POST /orders HTTP/1.1
Host: localhost:3001
Content-Type: application/json
Idempotency-Key: 2b7f0d2c-checkout-001
X-Correlation-Id: demo-001

{ "customerId": "cust-42", "currency": "EUR",
  "lines": [{ "sku": "SKU-PUMP-SEAL", "quantity": 2, "unitPriceMinor": 1500 }] }
```

```http
HTTP/1.1 202 Accepted
Location: /orders/8d2e4f6a-1b3c-4d5e-8f7a-9b0c1d2e3f4a
X-Correlation-Id: demo-001

{ "orderId": "8d2e4f6a-...", "status": "PENDING", "total": { "amountMinor": 3000, "currency": "EUR" }, ... }
```

```http
GET /orders/8d2e4f6a-1b3c-4d5e-8f7a-9b0c1d2e3f4a     -> { "status": "CONFIRMED", ... }
GET /stock/SKU-PUMP-SEAL  (inventory, :3002)       -> { "onHand": 50, "reserved": 2, "available": 48 }
GET /health/live | /health/ready | /metrics        (every service)
```

| Response | When |
|---|---|
| `202` | Order accepted (new) |
| `200` + `Idempotent-Replayed: true` | Same `Idempotency-Key` and body as an earlier request |
| `422` | Same `Idempotency-Key`, different body |
| `400` | Missing key or invalid body |

Seeded SKUs: `SKU-PUMP-SEAL` (50), `SKU-BEARING-6204` (200), `SKU-FILTER-HEPA` (10), `SKU-VALVE-DN50` (0 — use it
to see a rejection).

## Testing

```bash
npm test                    # unit tests, no infrastructure needed
npm run lint && npm run typecheck

# integration tests need PostgreSQL and RabbitMQ, e.g. from docker compose:
docker compose up -d postgres rabbitmq
DATABASE_URL=postgres://edp:edp-local-dev@localhost:5432/edp \
RABBITMQ_URL=amqp://edp:edp-local-dev@localhost:5672 \
npm run test:integration
```

The integration suites (skipped automatically when the variables are not set) verify:

- the full order flow to `CONFIRMED`, rejection for out-of-stock and unknown SKUs, a notification retry, and
  correlation id propagation through the outbox;
- idempotent HTTP replay and `422` on key reuse;
- a message delivered three times is processed once;
- transient failures retry with delays then dead-letter with `x-attempts`/`x-last-error`;
- permanent errors and malformed JSON dead-letter immediately;
- a retry that succeeds leaves exactly one inbox record.

CI runs both suites with PostgreSQL and RabbitMQ service containers.

## Docker

`docker/Dockerfile` builds one multi-stage image (compile → production dependencies only → non-root runtime)
used by all services; `docker-compose.yml` selects the entrypoint and roles per service and adds health checks
against `/health/ready`.

## Architecture Decisions

| ADR | Decision |
|---|---|
| [ADR-001](docs/decisions/ADR-001-rabbitmq-as-message-broker.md) | RabbitMQ rather than Kafka for this workload |
| [ADR-002](docs/decisions/ADR-002-transactional-outbox.md) | Transactional outbox for publishing |
| [ADR-003](docs/decisions/ADR-003-at-least-once-with-idempotent-consumers.md) | At-least-once delivery + inbox idempotency |
| [ADR-004](docs/decisions/ADR-004-event-versioning-and-upcasting.md) | Event versioning and upcasting |
| [ADR-005](docs/decisions/ADR-005-retry-topology-with-ttl-queues.md) | Delayed retries with TTL queues, then DLQ |
| [ADR-006](docs/decisions/ADR-006-choreography-for-order-flow.md) | Choreography for the order flow |

Also see the [event catalogue](docs/event-catalog.md).

## Scalability Considerations

- **Horizontal scaling per role:** the same image runs any combination of `api`, `relay` and `consumer`, so a busy
  consumer can scale without scaling the API.
- **Competing consumers** on each queue; `PREFETCH` bounds concurrency per instance.
- **Outbox relays** can run as several replicas thanks to `FOR UPDATE SKIP LOCKED` (trading global order).
- **Asynchronous processing** keeps the API latency independent of downstream work.
- **Database:** schema-per-service locally; separate databases in production. Inbox/outbox tables need retention.
- **Beyond this design:** for very high volume or replay requirements, Kafka partitions keyed by `subject`
  ([ADR-001](docs/decisions/ADR-001-rabbitmq-as-message-broker.md)).

## Reliability

Publisher confirms; manual acks after commit; inbox deduplication; delayed retries with an attempt counter;
dead-letter queues with diagnostic headers; failed moves to retry/DLQ requeue the original rather than drop it;
bounded broker connection retries at startup; process exit on broker loss so the orchestrator restarts cleanly;
graceful shutdown that stops consuming and drains in-flight messages.

## Security

- Input validation at the HTTP boundary and for every consumed message (envelope + payload schemas).
- Parameterised SQL everywhere; schema names escaped.
- Logs redact connection strings and authorization headers.
- Non-root container user; production dependencies only in the runtime image.
- CI runs `npm audit` and CodeQL ([security.yml](.github/workflows/security.yml)).
- **Not implemented** (reference scope): API authentication/authorization, TLS to the broker and database,
  per-service broker credentials. See [enterprise-saas-plateform](https://github.com/shivkumarsinghsky/enterprise-saas-plateform)
  for authentication and tenant-aware authorization.

## Observability

- JSON logs with `service`, `queue`, `messageId`, `type`, `correlationId`.
- `X-Correlation-Id` from the HTTP request becomes the envelope `correlationId`; `causationId` links each event
  to its cause, so a flow can be reconstructed from logs or outbox tables.
- Prometheus metrics: `messages_consumed_total{queue,type,outcome}`, `message_processing_seconds`,
  `outbox_published_total`, `outbox_pending`.
- `/health/live` and `/health/ready` on every process (used by Docker health checks).

## Future Improvements

Not implemented yet:

- OpenTelemetry tracing with `traceparent` propagated in message headers.
- Retention jobs for outbox and inbox tables.
- Consistent-hash exchange (or Kafka) for strict per-order ordering.
- Payment step with an orchestrated saga and compensations.
- Authentication on the order API; TLS and per-service credentials for RabbitMQ and PostgreSQL.
- Contract tests between producers and consumers (schema compatibility checks in CI).

## Related Projects

- [Microservices Patterns](https://github.com/shivkumarsinghsky/microservices-patterns) — pattern catalogue (outbox, saga, idempotency, retry) with focused implementations
- [Real-Time Monitoring Platform](https://github.com/shivkumarsinghsky/realtime-monitoring-platform) — high-volume telemetry events
- [Enterprise SaaS Platform](https://github.com/shivkumarsinghsky/enterprise-saas-plateform) — tenant-aware events and audit logging
- [System Design Architecture](https://github.com/shivkumarsinghsky/system-design-architecture) — e-commerce and notification system designs

## Author

**Shiv Kumar** — Senior Software Engineer / Software Architect
GitHub: [github.com/shivkumarsinghsky](https://github.com/shivkumarsinghsky)

## License

[MIT](LICENSE)

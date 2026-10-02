# Changelog

All notable changes to this repository are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-10-02

### Added

- order-service, inventory-service and notification-service communicating through RabbitMQ.
- Messaging platform: envelope with correlation/causation ids, publisher confirms, transactional outbox relay,
  inbox-based idempotent consumers, delayed retries via TTL queues, dead-letter queues, DLQ replay script.
- Event versioning with upcasting (`order.placed` v1 → v2).
- Health, readiness and Prometheus metrics endpoints on every process; graceful shutdown.
- Unit tests and integration tests against real PostgreSQL and RabbitMQ.
- Docker image and Docker Compose environment; CI with service containers.
- Architecture document, event catalogue, DLQ runbook, ADR-001 to ADR-006.

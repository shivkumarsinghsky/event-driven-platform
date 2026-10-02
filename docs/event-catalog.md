# Event Catalogue

All events use the common envelope defined in
[`src/platform/contracts/envelope.ts`](../src/platform/contracts/envelope.ts); payload schemas are in
[`src/platform/contracts/events.ts`](../src/platform/contracts/events.ts).

## Envelope

| Field | Type | Description |
|---|---|---|
| `id` | uuid | Unique message id; used for deduplication (inbox) and as AMQP `messageId` |
| `type` | string | Event name, e.g. `order.placed` |
| `version` | integer | Schema version of `data` |
| `source` | string | Producing service |
| `occurredAt` | ISO-8601 | When the change happened |
| `correlationId` | string | Shared by every message in one business flow (starts at the HTTP request) |
| `causationId` | string or null | Id of the message that caused this one |
| `subject` | string | Entity the event is about (order id); the natural partition key |
| `data` | object | Versioned payload |

Routing key: `<type>.v<version>` on the `domain.events` topic exchange.

## Events

### `order.placed` — producer: order-service

| Version | Status | Payload |
|---|---|---|
| v1 | Deprecated (still consumed via upcasting) | `orderId`, `customerId`, `lines[{sku, quantity}]`, `totalAmount` (float, implicitly EUR) |
| v2 | Current | `orderId`, `customerId`, `lines[{sku, quantity}]`, `total{amountMinor (int), currency (ISO-4217)}` |

v2 is a breaking change (money as integer minor units with explicit currency). Consumers bind to
`order.placed.*` and call `readOrderPlaced`, which upcasts v1 to v2.

```json
{
  "id": "4f1c6a3e-2b9d-4a51-9f1e-6c2d7b8a9e01",
  "type": "order.placed",
  "version": 2,
  "source": "order-service",
  "occurredAt": "2026-10-01T10:15:00.000Z",
  "correlationId": "c0a8012e-7d4f-4b1a-9c2e-3f5d6e7a8b9c",
  "causationId": null,
  "subject": "8d2e4f6a-1b3c-4d5e-8f7a-9b0c1d2e3f4a",
  "data": {
    "orderId": "8d2e4f6a-1b3c-4d5e-8f7a-9b0c1d2e3f4a",
    "customerId": "cust-42",
    "lines": [{ "sku": "SKU-PUMP-SEAL", "quantity": 2 }],
    "total": { "amountMinor": 3000, "currency": "EUR" }
  }
}
```

### `inventory.reserved` v1 — producer: inventory-service

`orderId`, `reservationId`.

### `inventory.rejected` v1 — producer: inventory-service

`orderId`, `reason` (`OUT_OF_STOCK` | `UNKNOWN_SKU`), `sku`.

### `order.status-changed` v1 — producer: order-service

`orderId`, `customerId`, `status` (`CONFIRMED` | `REJECTED`), optional `reason`.

## Compatibility rules

1. Adding an optional field: same version.
2. Removing/renaming a field, changing a type or meaning: new version.
3. Producers publish only the latest version; consumers must accept all versions still in flight.
4. A version is retired only after its producers are gone and queues/DLQs no longer contain it.

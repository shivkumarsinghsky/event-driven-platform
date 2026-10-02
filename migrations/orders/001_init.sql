CREATE TABLE orders.orders (
  id            uuid PRIMARY KEY,
  customer_id   text        NOT NULL,
  status        text        NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'REJECTED')),
  lines         jsonb       NOT NULL,
  total_minor   bigint      NOT NULL CHECK (total_minor >= 0),
  currency      char(3)     NOT NULL,
  reject_reason text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders.idempotency_keys (
  key          text PRIMARY KEY,
  request_hash text        NOT NULL,
  order_id     uuid        NOT NULL REFERENCES orders.orders (id),
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders.outbox (
  id           uuid PRIMARY KEY,
  seq          bigserial   NOT NULL,
  subject      text        NOT NULL,
  envelope     jsonb       NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON orders.outbox (seq) WHERE published_at IS NULL;

CREATE TABLE orders.inbox (
  consumer     text        NOT NULL,
  message_id   uuid        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);

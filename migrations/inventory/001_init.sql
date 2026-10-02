CREATE TABLE inventory.stock (
  sku      text PRIMARY KEY,
  on_hand  integer NOT NULL CHECK (on_hand >= 0),
  reserved integer NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  CHECK (reserved <= on_hand)
);

CREATE TABLE inventory.reservations (
  id         uuid PRIMARY KEY,
  order_id   uuid        NOT NULL UNIQUE,
  lines      jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE inventory.outbox (
  id           uuid PRIMARY KEY,
  seq          bigserial   NOT NULL,
  subject      text        NOT NULL,
  envelope     jsonb       NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON inventory.outbox (seq) WHERE published_at IS NULL;

CREATE TABLE inventory.inbox (
  consumer     text        NOT NULL,
  message_id   uuid        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);

-- Demo catalogue so the platform works out of the box.
INSERT INTO inventory.stock (sku, on_hand) VALUES
  ('SKU-PUMP-SEAL', 50),
  ('SKU-BEARING-6204', 200),
  ('SKU-FILTER-HEPA', 10),
  ('SKU-VALVE-DN50', 0);

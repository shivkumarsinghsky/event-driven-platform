CREATE TABLE notification.notifications (
  id          uuid PRIMARY KEY,
  order_id    uuid        NOT NULL,
  customer_id text        NOT NULL,
  channel     text        NOT NULL,
  template    text        NOT NULL,
  sent_at     timestamptz NOT NULL DEFAULT now(),
  -- The source event id doubles as the idempotency key passed to the email provider.
  source_message_id uuid  NOT NULL UNIQUE
);

CREATE TABLE notification.inbox (
  consumer     text        NOT NULL,
  message_id   uuid        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);

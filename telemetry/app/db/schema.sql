CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS order_metrics (
    submission_id   TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    order_id        TEXT NOT NULL,
    order_type      TEXT NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL,
    latency_ns      BIGINT NOT NULL,
    fill_correct    INTEGER NOT NULL,
    bot_id          TEXT NOT NULL,
    side            TEXT,
    price           NUMERIC,
    quantity        INTEGER,
    ack_at_ns       BIGINT,
    expected_fill_qty  INTEGER,
    actual_fill_qty INTEGER,
    expected_fill_price NUMERIC,
    actual_fill_price NUMERIC,
    status          TEXT,
    reject_reason   TEXT,
    cancel_order_id TEXT
);

SELECT create_hypertable('order_metrics', 'sent_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_submission
ON order_metrics (submission_id, sent_at DESC);
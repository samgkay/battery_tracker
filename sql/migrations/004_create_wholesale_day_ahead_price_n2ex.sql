CREATE TABLE IF NOT EXISTS wholesale_day_ahead_price_n2ex (
    ts TIMESTAMPTZ PRIMARY KEY,
    price_gbp_per_mwh NUMERIC NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wholesale_intraday_price_apx (
    ts TIMESTAMPTZ PRIMARY KEY,
    price_gbp_per_mwh NUMERIC NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

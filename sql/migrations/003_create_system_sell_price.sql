CREATE TABLE IF NOT EXISTS system_sell_price (
    ts TIMESTAMPTZ PRIMARY KEY,
    ssp_gbp_per_mwh NUMERIC NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

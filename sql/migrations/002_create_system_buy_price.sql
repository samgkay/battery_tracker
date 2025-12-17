CREATE TABLE IF NOT EXISTS system_buy_price (
    ts timestamptz PRIMARY KEY,
    sbp_gbp_per_mwh numeric NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

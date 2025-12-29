CREATE TABLE IF NOT EXISTS final_physical_notifications (
    ts TIMESTAMPTZ NOT NULL,
    bmu_id TEXT NOT NULL,
    fpn_mw NUMERIC NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts, bmu_id)
);

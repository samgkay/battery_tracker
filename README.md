# battery_tracker

Local MVP:
- Postgres in Docker
- SQL file to create test table
- Python script to fetch from an API and insert into Postgres

## System Buy Price ingestion

System Buy Price (SBP) data is fetched from the Elexon Insights/BMRS endpoint:
`https://data.elexon.co.uk/balancing/settlement/system-prices/{settlementDate}`.
The endpoint returns both SBP and SSP values, and ingestion validates that
`systemBuyPrice` is present to guard against using the wrong route.

### Preflight check

Confirm the endpoint contract and capture a sample payload before ingesting:

```powershell
python scripts\api_preflight_system_prices.py --date 2025-01-01
```

This prints the request URL, HTTP status, top-level JSON keys, and saves the
response under `tmp/system_prices_sample_2025-01-01.json`.

### Migrations

Run migrations inside the Postgres container:

```powershell
docker exec -it battery_tracker_postgres psql -U battery_tracker -d battery_tracker -f /workspace/sql/001_create_test_table.sql
docker exec -it battery_tracker_postgres psql -U battery_tracker -d battery_tracker -f /workspace/sql/migrations/002_create_system_buy_price.sql
```

### SBP backfill for 2025

Populate the `system_buy_price` table for calendar year 2025:

```powershell
python scripts\backfill_system_buy_price_2025.py
```

The script loads `DATABASE_URL` from `.env`, fetches daily SBP data for 2025,
normalizes settlement periods to UTC timestamps, and upserts rows with
`INSERT ... ON CONFLICT` semantics so reruns are idempotent.

### Verification query

After running the backfill, verify row counts:

```powershell
docker exec -it battery_tracker_postgres psql -U battery_tracker -d battery_tracker -c "select count(*) from system_buy_price;"
```

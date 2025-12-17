# battery_tracker

Local MVP:
- Postgres in Docker
- SQL file to create test table
- Python script to fetch from an API and insert into Postgres

## Running locally (PowerShell)

```powershell
docker compose up -d
python scripts\apply_migrations.py
python scripts\api_preflight_system_sell_prices.py --date 2025-01-01
python scripts\backfill_system_sell_price_2025.py
```

Verify data was ingested:

```powershell
psql -c "select count(*) from system_sell_price;"
```

## Elexon BMRS endpoints and window rules

- Base URL: `https://data.elexon.co.uk/bmrs/api/v1`
- MID (Market Index Data) day-ahead (N2EX) and intraday (APX) use `/datasets/MID` with query params `from`, `to`, and `dataProvider`.
- The API enforces a maximum 7-day window. January 2025 backfills are chunked as:
  - `2025-01-01T00:00:00Z -> 2025-01-08T00:00:00Z`
  - `2025-01-08T00:00:00Z -> 2025-01-15T00:00:00Z`
  - `2025-01-15T00:00:00Z -> 2025-01-22T00:00:00Z`
  - `2025-01-22T00:00:00Z -> 2025-01-29T00:00:00Z`
  - `2025-01-29T00:00:00Z -> 2025-02-01T00:00:00Z`
- Final Physical Notifications (PN) as an FPN proxy use `/balancing/physical` with `from`, `to`, and `bmUnit`, and follow the same 7-day chunking.

## Migrations

Apply all SQL migrations:

```powershell
python scripts\apply_migrations.py
```

## API preflight checks

Run a small window to validate connectivity and response shape:

```powershell
python scripts\api_preflight_mid_n2ex.py
python scripts\api_preflight_mid_apx.py
python scripts\api_preflight_fpn.py
```

## January 2025 backfills

Backfill using the pre-defined January 2025 windows (with 7-day chunking):

```powershell
python scripts\backfill_mid_n2ex_jan_2025.py
python scripts\backfill_mid_apx_jan_2025.py
python scripts\backfill_fpn_jan_2025.py
```

## Verification queries

```powershell
psql -c "select count(*) from wholesale_day_ahead_price_n2ex;"
psql -c "select count(*) from wholesale_intraday_price_apx;"
psql -c "select count(*) from final_physical_notifications where bmu_id='T_DRAXX-1';"
```

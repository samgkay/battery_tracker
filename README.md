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

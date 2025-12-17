# battery_tracker

Local MVP:
- Postgres in Docker
- SQL file to create test table
- Python script to fetch from an API and insert into Postgres

## Prerequisites
- Docker / Docker Compose
- Python 3 with `psycopg2-binary`, `requests`, and `python-dotenv` installed
- A `.env` file in the repo root containing:
  ```
  DATABASE_URL=postgresql://battery_tracker:tothemoon@localhost:5432/battery_tracker
  ```

## Running locally (PowerShell examples)

Start the database (background):
```powershell
docker compose up -d
```

Apply SQL migrations:
```powershell
python scripts\apply_migrations.py
```

Run the 2025 System Buy Price backfill:
```powershell
python scripts\backfill_system_buy_price_2025.py
```

Check how many SBP rows were ingested:
```powershell
docker exec -it battery_tracker_postgres psql -U battery_tracker -d battery_tracker -c "select count(*) from system_buy_price;"
```

## Notes on the System Buy Price endpoint
The System Buy Price (SBP) data is provided by the Elexon BMRS API under
`https://data.elexon.co.uk/bmrs/api/v1/balancing/system-prices`. The previously
assumed `/balancing/pricing/market-index` endpoint publishes Market Index Price
(MIP) data and does **not** contain SBP values, so this project targets the
`system-prices` endpoint for SBP ingestion.

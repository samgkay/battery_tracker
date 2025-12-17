import os
import sys
from pathlib import Path

from dotenv import load_dotenv

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

from battery_tracker.ingest.wholesale_prices import backfill_mid_to_table  # noqa: E402


START_TS = "2025-01-01T00:00:00Z"
END_TS = "2025-02-01T00:00:00Z"
PROVIDER = "APXMIDP"
TABLE_NAME = "wholesale_intraday_price_apx"


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Create a .env file in the repo root.")

    backfill_mid_to_table(database_url, PROVIDER, TABLE_NAME, START_TS, END_TS)


if __name__ == "__main__":
    main()

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Ensure local imports work when running as a script
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from battery_tracker.ingest.system_buy_price import backfill_2025  # noqa: E402


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Create a .env file with DATABASE_URL.")

    with psycopg2.connect(database_url) as conn:
        total = backfill_2025(conn)

    print("Backfill complete for 2025; rows inserted/updated:", total)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - surface the error for a non-zero exit
        print(f"Backfill failed: {exc}")
        sys.exit(1)

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Ensure src is on the path for imports
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

from battery_tracker.ingest import backfill_system_sell_price_2025  # noqa: E402


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Create a .env file in the repo root.")

    backfill_system_sell_price_2025(database_url)


if __name__ == "__main__":
    main()

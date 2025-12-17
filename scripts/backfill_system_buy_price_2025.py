import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for path in (ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from battery_tracker.ingest.system_buy_price import backfill_system_buy_price_2025  # noqa: E402


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Check your .env file in the repository root.")

    try:
        backfill_system_buy_price_2025(database_url)
    except Exception as exc:
        print(f"Backfill failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

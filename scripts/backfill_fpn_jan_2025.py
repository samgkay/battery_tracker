import os
import sys
from pathlib import Path

from dotenv import load_dotenv

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

from battery_tracker.ingest.fpn import backfill_fpn_for_bmu  # noqa: E402


START_TS = "2025-01-01T00:00:00Z"
END_TS = "2025-02-01T00:00:00Z"
DEFAULT_BM_UNIT = "T_DRAXX-1"


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Create a .env file in the repo root.")

    backfill_fpn_for_bmu(database_url, DEFAULT_BM_UNIT, START_TS, END_TS)


if __name__ == "__main__":
    main()

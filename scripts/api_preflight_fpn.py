import json
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

from battery_tracker.sources.elexon_physical import BASE_URL, PHYSICAL_PATH  # noqa: E402


START_TS = "2025-01-01T00:00:00Z"
END_TS = "2025-01-08T00:00:00Z"
BM_UNIT = "T_DRAXX-1"


def main() -> None:
    load_dotenv()
    url = BASE_URL + PHYSICAL_PATH
    params = {"from": START_TS, "to": END_TS, "bmUnit": BM_UNIT}

    response = requests.get(url, params=params, timeout=30)
    print(f"Requesting: {response.url}")
    print(f"Status code: {response.status_code}")

    if response.status_code != 200:
        sys.exit(1)

    try:
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to parse JSON: {exc}")
        sys.exit(1)

    if isinstance(payload, dict):
        top_level_keys = list(payload.keys())
        data_section = payload.get("data")
    else:
        top_level_keys = [str(type(payload))]
        data_section = None
    print(f"Top-level keys: {top_level_keys}")

    first_keys = []
    if isinstance(data_section, list) and data_section:
        first_keys = list(data_section[0].keys())
    print(f"First record keys: {json.dumps(first_keys)}")


if __name__ == "__main__":
    main()

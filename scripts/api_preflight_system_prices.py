import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests


def fetch_system_prices(settlement_date: str) -> tuple[str, requests.Response]:
    base_url = "https://data.elexon.co.uk/balancing/settlement/system-prices"
    url = f"{base_url}/{settlement_date}"
    response = requests.get(url, timeout=15)
    return url, response


def main():
    parser = argparse.ArgumentParser(description="Preflight check for Elexon system prices endpoint")
    parser.add_argument("--date", default="2025-01-01", help="Settlement date (YYYY-MM-DD)")
    args = parser.parse_args()

    url, response = fetch_system_prices(args.date)
    print(f"Request URL: {url}")
    print(f"Status code: {response.status_code}")

    if response.status_code != 200:
        sys.exit(1)

    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        print(f"Failed to decode JSON: {exc}")
        sys.exit(1)

    if isinstance(data, dict):
        top_level_keys = list(data.keys())
    else:
        top_level_keys = []
    print(f"Top-level keys: {top_level_keys}")

    tmp_dir = ROOT / "tmp"
    tmp_dir.mkdir(exist_ok=True)
    output_path = tmp_dir / f"system_prices_sample_{args.date}.json"
    output_path.write_text(json.dumps(data, indent=2))
    print(f"Saved sample JSON to {output_path}")


if __name__ == "__main__":
    main()

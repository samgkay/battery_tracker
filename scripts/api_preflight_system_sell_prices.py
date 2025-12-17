import json
import sys
from argparse import ArgumentParser
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
SYSTEM_PRICES_PATH = "/balancing/settlement/system-prices/{settlement_date}"


def build_url(settlement_date: str) -> str:
    return BASE_URL + SYSTEM_PRICES_PATH.format(settlement_date=settlement_date)


def main() -> None:
    parser = ArgumentParser(description="Preflight check for Elexon system sell prices API")
    parser.add_argument("--date", required=True, help="Settlement date (YYYY-MM-DD)")
    args = parser.parse_args()

    load_dotenv()
    url = build_url(args.date)
    print(f"Requesting: {url}")

    response = requests.get(url, timeout=30)
    print(f"Status code: {response.status_code}")

    try:
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 - broad to surface any JSON error
        print(f"Failed to parse JSON: {exc}")
        sys.exit(1)

    if isinstance(payload, dict):
        top_level_keys = list(payload.keys())
    elif isinstance(payload, list):
        top_level_keys = ["<list>"]
    else:
        top_level_keys = [str(type(payload))]
    print(f"Top-level keys: {top_level_keys}")

    output_dir = Path("tmp")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"system_sell_prices_sample_{args.date}.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Saved response to {output_path}")


if __name__ == "__main__":
    # Allow imports from the src directory if needed
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root / "src"))

    main()

import os
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv


def fetch_data() -> dict:
    """
    Fetch a small piece of data from a public API.
    Uses GitHub's public endpoint (very commonly reachable).
    """
    resp = requests.get("https://api.github.com/zen", timeout=20)
    resp.raise_for_status()
    text = resp.text.strip()

    # Store a simple integer derived from the API response
    value = len(text)

    fetched_at = datetime.now(timezone.utc)

    return {
        "source": "github_zen",
        "value": value,
        "fetched_at": fetched_at,
    }


def insert_row(database_url: str, row: dict) -> None:
    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO test_table (source, value, fetched_at)
                VALUES (%s, %s, %s)
                """,
                (row["source"], row["value"], row["fetched_at"]),
            )
        conn.commit()


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Create a .env file in the repo root.")

    row = fetch_data()
    insert_row(database_url, row)

    now = datetime.now(timezone.utc).isoformat()
    print(f"[{now}] Inserted row into test_table: {row}")


if __name__ == "__main__":
    main()

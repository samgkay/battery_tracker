from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg2
from psycopg2 import sql

from battery_tracker.sources.elexon_mid import fetch_mid

TIMESTAMP_KEYS: tuple[str, ...] = (
    "timestamp",
    "time",
    "intervalStart",
    "localTime",
    "utcTime",
)
PRICE_KEYS: tuple[str, ...] = (
    "price",
    "marketIndexPrice",
    "midPrice",
    "value",
)


def _parse_timestamp(value: str) -> datetime:
    ts = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_timestamp(record: Dict[str, object]) -> datetime:
    for key in TIMESTAMP_KEYS:
        if key in record:
            return _parse_timestamp(record[key])
    available_keys = ", ".join(sorted(record.keys()))
    raise ValueError(f"No timestamp field found in MID record. Available keys: {available_keys}")


def _get_price(record: Dict[str, object]) -> Decimal:
    for key in PRICE_KEYS:
        if key in record:
            return Decimal(str(record[key]))
    available_keys = ", ".join(sorted(record.keys()))
    raise ValueError(f"No price field found in MID record. Available keys: {available_keys}")


def normalize_mid_records(records: Iterable[Dict[str, object]]) -> List[Tuple[datetime, Decimal]]:
    normalized: List[Tuple[datetime, Decimal]] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Record is not a mapping")
        ts = _get_timestamp(record)
        price = _get_price(record)
        normalized.append((ts, price))
    return normalized


def _chunk_time_ranges(start: datetime, end: datetime) -> List[Tuple[datetime, datetime]]:
    chunks: List[Tuple[datetime, datetime]] = []
    current = start
    window = timedelta(days=7)
    while current < end:
        window_end = min(current + window, end)
        chunks.append((current, window_end))
        current = window_end
    return chunks


def upsert_mid_prices(conn, table_name: str, rows: Sequence[Tuple[datetime, Decimal]]) -> None:
    if not rows:
        return

    query = sql.SQL(
        """
        INSERT INTO {table} (ts, price_gbp_per_mwh)
        VALUES (%s, %s)
        ON CONFLICT (ts) DO UPDATE
        SET price_gbp_per_mwh = EXCLUDED.price_gbp_per_mwh,
            ingested_at = NOW()
        """
    ).format(table=sql.Identifier(table_name))

    with conn.cursor() as cur:
        cur.executemany(query, rows)
    conn.commit()


def backfill_mid_to_table(
    database_url: str,
    provider: str,
    table_name: str,
    start_ts: str,
    end_ts: str,
) -> None:
    start = _parse_timestamp(start_ts)
    end = _parse_timestamp(end_ts)

    ranges = _chunk_time_ranges(start, end)
    total_rows = 0

    with psycopg2.connect(database_url) as conn:
        for range_start, range_end in ranges:
            from_iso = range_start.isoformat().replace("+00:00", "Z")
            to_iso = range_end.isoformat().replace("+00:00", "Z")
            print(
                f"Fetching MID provider={provider} window {from_iso} -> {to_iso}",
                flush=True,
            )
            records = fetch_mid(from_iso, to_iso, provider)
            normalized = normalize_mid_records(records)
            upsert_mid_prices(conn, table_name, normalized)
            total_rows += len(normalized)
            print(
                f"Window {from_iso} -> {to_iso}: fetched {len(records)} records, upserted {len(normalized)} rows",
                flush=True,
            )

    print(f"Completed backfill into {table_name}. Total rows upserted: {total_rows}")


__all__ = [
    "backfill_mid_to_table",
    "normalize_mid_records",
    "upsert_mid_prices",
]

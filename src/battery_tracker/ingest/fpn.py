from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg2
from psycopg2 import sql

from battery_tracker.sources.elexon_physical import fetch_physical

DATASET_FILTER = "PN"


def _parse_timestamp(value: str) -> datetime:
    ts = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_record(record: Dict[str, object], bm_unit: str) -> Tuple[datetime, str, Decimal]:
    dataset_value = str(record.get("dataset", "")).upper()
    if dataset_value != DATASET_FILTER:
        raise ValueError("Record dataset is not PN")

    if "timeFrom" not in record:
        available_keys = ", ".join(sorted(record.keys()))
        raise ValueError(f"Record missing timeFrom. Available keys: {available_keys}")
    if "levelFrom" not in record:
        available_keys = ", ".join(sorted(record.keys()))
        raise ValueError(f"Record missing levelFrom. Available keys: {available_keys}")

    if "levelTo" in record and record["levelTo"] != record["levelFrom"]:
        print(
            f"Warning: levelFrom ({record['levelFrom']}) != levelTo ({record['levelTo']}) for {record.get('timeFrom')} {bm_unit}",
            flush=True,
        )

    ts = _parse_timestamp(record["timeFrom"])
    fpn_mw = Decimal(str(record["levelFrom"]))
    return ts, bm_unit, fpn_mw


def filter_and_normalize(records: Iterable[Dict[str, object]], bm_unit: str) -> List[Tuple[datetime, str, Decimal]]:
    normalized: List[Tuple[datetime, str, Decimal]] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Record is not a mapping")
        dataset_value = str(record.get("dataset", "")).upper()
        if dataset_value != DATASET_FILTER:
            continue
        normalized.append(_normalize_record(record, bm_unit))
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


def upsert_fpn(conn, rows: Sequence[Tuple[datetime, str, Decimal]]) -> None:
    if not rows:
        return

    query = sql.SQL(
        """
        INSERT INTO final_physical_notifications (ts, bmu_id, fpn_mw)
        VALUES (%s, %s, %s)
        ON CONFLICT (ts, bmu_id) DO UPDATE
        SET fpn_mw = EXCLUDED.fpn_mw,
            ingested_at = NOW()
        """
    )

    with conn.cursor() as cur:
        cur.executemany(query, rows)
    conn.commit()


def backfill_fpn_for_bmu(database_url: str, bm_unit: str, start_ts: str, end_ts: str) -> None:
    start = _parse_timestamp(start_ts)
    end = _parse_timestamp(end_ts)

    ranges = _chunk_time_ranges(start, end)
    total_rows = 0

    with psycopg2.connect(database_url) as conn:
        for range_start, range_end in ranges:
            from_iso = range_start.isoformat().replace("+00:00", "Z")
            to_iso = range_end.isoformat().replace("+00:00", "Z")
            print(
                f"Fetching FPN for {bm_unit} window {from_iso} -> {to_iso}",
                flush=True,
            )
            records = fetch_physical(from_iso, to_iso, bm_unit)
            normalized = filter_and_normalize(records, bm_unit)
            upsert_fpn(conn, normalized)
            total_rows += len(normalized)
            print(
                f"Window {from_iso} -> {to_iso}: fetched {len(records)} records, upserted {len(normalized)} rows",
                flush=True,
            )

    print(
        f"Completed FPN backfill for {bm_unit} into final_physical_notifications. Total rows upserted: {total_rows}"
    )


__all__ = [
    "backfill_fpn_for_bmu",
    "filter_and_normalize",
    "upsert_fpn",
]

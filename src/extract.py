from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Any

from .twitch_api import get_app_access_token, get_credentials, get_streams_page

DEFAULT_OUTPUT_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "raw" / "twitch_streams_snapshots.csv"
)
DEFAULT_STREAM_COLUMNS = [
    "id",
    "user_id",
    "user_login",
    "user_name",
    "game_id",
    "game_name",
    "type",
    "title",
    "viewer_count",
    "started_at",
    "language",
    "thumbnail_url",
    "tag_ids",
    "tags",
    "is_mature",
    "collected_at",
]


def collect_streams_snapshot(
    max_pages: int = 5,
    page_size: int = 100,
    sleep_sec: float = 0.5,
) -> list[dict[str, Any]]:
    client_id, client_secret = get_credentials()
    token = get_app_access_token(client_id, client_secret)

    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    collected_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for page_number in range(1, max_pages + 1):
        payload = get_streams_page(
            token=token,
            client_id=client_id,
            first=page_size,
            after=cursor,
        )

        page_rows = payload.get("data", [])
        if not page_rows:
            print(f"Page {page_number}: no more rows returned")
            break

        print(f"Page {page_number}: fetched {len(page_rows)} streams")

        for row in page_rows:
            cleaned_row = dict(row)
            cleaned_row["collected_at"] = collected_at

            if isinstance(cleaned_row.get("tags"), list):
                cleaned_row["tags"] = ", ".join(cleaned_row["tags"])
            if isinstance(cleaned_row.get("tag_ids"), list):
                cleaned_row["tag_ids"] = ", ".join(cleaned_row["tag_ids"])

            rows.append(cleaned_row)

        cursor = payload.get("pagination", {}).get("cursor")
        if not cursor:
            break

        time.sleep(sleep_sec)

    return rows


def append_rows_to_csv(
    rows: list[dict[str, Any]],
    csv_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    if not rows:
        raise ValueError("No Twitch rows were collected, so nothing was written to CSV")

    output_path = Path(csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(
        dict.fromkeys(DEFAULT_STREAM_COLUMNS + [key for row in rows for key in row.keys()])
    )
    file_exists = output_path.exists()

    with output_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction="ignore")

        if not file_exists:
            writer.writeheader()

        for row in rows:
            writer.writerow({column: row.get(column, "") for column in fieldnames})

    print(f"Saved {len(rows)} rows to {output_path}")
    return output_path


def collect_and_store_streams_snapshot(
    csv_path: str | Path = DEFAULT_OUTPUT_PATH,
    max_pages: int = 5,
    page_size: int = 100,
    sleep_sec: float = 0.5,
) -> str:
    rows = collect_streams_snapshot(
        max_pages=max_pages,
        page_size=page_size,
        sleep_sec=sleep_sec,
    )
    saved_path = append_rows_to_csv(rows, csv_path=csv_path)
    return str(saved_path)

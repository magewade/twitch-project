from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional in Airflow container
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

DEFAULT_INPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "processed"
    / "twitch_streams_enriched.csv"
)
DEFAULT_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "twitch")
DEFAULT_TABLE = "stream_snapshots_enriched"

TABLE_COLUMNS: list[tuple[str, str]] = [
    ("id", "String"),
    ("user_id", "String"),
    ("user_login", "String"),
    ("user_name", "String"),
    ("game_id", "String"),
    ("game_name", "String"),
    ("title", "String"),
    ("viewer_count", "UInt32"),
    ("started_at", "Nullable(DateTime)"),
    ("collected_at", "DateTime"),
    ("language", "String"),
    ("is_mature", "Bool"),
    ("stream_duration_minutes", "Float32"),
    ("stream_duration_hours", "Float32"),
    ("snapshot_hour_utc", "Nullable(UInt8)"),
    ("viewer_count_delta", "Float32"),
    ("viewer_count_pct_change", "Float32"),
    ("viewer_trend", "String"),
]


def _import_clickhouse_connect():
    try:
        import clickhouse_connect
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise ImportError(
            "clickhouse-connect is not installed. Install project dependencies or restart Airflow containers so _PIP_ADDITIONAL_REQUIREMENTS is applied."
        ) from exc

    return clickhouse_connect


def get_clickhouse_client(database: str | None = None):
    clickhouse_connect = _import_clickhouse_connect()
    host = resolve_clickhouse_host()
    return clickhouse_connect.get_client(
        host=host,
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=database or "default",
    )


def resolve_clickhouse_host() -> str:
    configured_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    if configured_host != "clickhouse":
        return configured_host

    try:
        socket.gethostbyname(configured_host)
        return configured_host
    except socket.gaierror:
        return "localhost"


def ensure_clickhouse_table(
    database: str = DEFAULT_DATABASE,
    table_name: str = DEFAULT_TABLE,
) -> None:
    ddl_columns = ",\n        ".join(
        f"{column_name} {column_type}" for column_name, column_type in TABLE_COLUMNS
    )

    admin_client = get_clickhouse_client()
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    admin_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}
        (
            {ddl_columns}
        )
        ENGINE = MergeTree
        PARTITION BY toDate(collected_at)
        ORDER BY (collected_at, id)
        """
    )
    admin_client.close()


def load_processed_streams(csv_path: str | Path = DEFAULT_INPUT_PATH) -> pd.DataFrame:
    input_path = Path(csv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Processed Twitch CSV was not found: {input_path}")

    df = pd.read_csv(input_path)
    if df.empty:
        raise ValueError(f"Processed Twitch CSV is empty: {input_path}")

    return df


def prepare_latest_batch(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp]:
    prepared = df.copy()
    prepared["collected_at"] = pd.to_datetime(
        prepared["collected_at"], utc=True, errors="coerce"
    )
    prepared["started_at"] = pd.to_datetime(
        prepared.get("started_at"), utc=True, errors="coerce"
    )
    prepared = prepared.dropna(subset=["collected_at"]).copy()

    if prepared.empty:
        raise ValueError("No valid collected_at values were found in the processed CSV")

    latest_collected_at = prepared["collected_at"].max()
    latest_batch = prepared.loc[
        prepared["collected_at"] == latest_collected_at,
        [column_name for column_name, _ in TABLE_COLUMNS],
    ].copy()

    string_columns = [
        "id",
        "user_id",
        "user_login",
        "user_name",
        "game_id",
        "game_name",
        "title",
        "language",
        "viewer_trend",
    ]
    for column_name in string_columns:
        latest_batch[column_name] = (
            latest_batch.get(column_name, "").fillna("").astype(str)
        )

    latest_batch["viewer_count"] = (
        pd.to_numeric(latest_batch.get("viewer_count"), errors="coerce")
        .fillna(0)
        .astype("uint32")
    )
    latest_batch["is_mature"] = (
        latest_batch.get("is_mature", False)
        .astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False})
        .fillna(False)
        .astype(bool)
    )
    latest_batch["stream_duration_minutes"] = (
        pd.to_numeric(latest_batch.get("stream_duration_minutes"), errors="coerce")
        .fillna(0.0)
        .astype("float32")
    )
    latest_batch["stream_duration_hours"] = (
        pd.to_numeric(latest_batch.get("stream_duration_hours"), errors="coerce")
        .fillna(0.0)
        .astype("float32")
    )
    latest_batch["viewer_count_delta"] = (
        pd.to_numeric(latest_batch.get("viewer_count_delta"), errors="coerce")
        .fillna(0.0)
        .astype("float32")
    )
    latest_batch["viewer_count_pct_change"] = (
        pd.to_numeric(latest_batch.get("viewer_count_pct_change"), errors="coerce")
        .fillna(0.0)
        .astype("float32")
    )
    latest_batch["snapshot_hour_utc"] = pd.to_numeric(
        latest_batch.get("snapshot_hour_utc"), errors="coerce"
    ).astype("Int64")

    latest_batch["collected_at"] = (
        latest_batch["collected_at"].dt.tz_convert("UTC").dt.tz_localize(None)
    )
    latest_batch["started_at"] = latest_batch["started_at"].apply(
        lambda value: (
            value.tz_convert("UTC").tz_localize(None) if pd.notna(value) else None
        )
    )
    latest_batch["snapshot_hour_utc"] = latest_batch["snapshot_hour_utc"].where(
        latest_batch["snapshot_hour_utc"].notna(), None
    )

    return latest_batch, latest_collected_at


def batch_already_loaded(
    latest_collected_at: pd.Timestamp,
    database: str = DEFAULT_DATABASE,
    table_name: str = DEFAULT_TABLE,
) -> bool:
    client = get_clickhouse_client(database=database)
    latest_value = latest_collected_at.tz_convert("UTC").tz_localize(None)
    result = client.query(
        f"SELECT count() FROM {table_name} WHERE collected_at = %(collected_at)s",
        parameters={"collected_at": latest_value},
    )
    client.close()
    return bool(result.result_rows[0][0])


def insert_latest_batch(
    batch_df: pd.DataFrame,
    database: str = DEFAULT_DATABASE,
    table_name: str = DEFAULT_TABLE,
) -> int:
    client = get_clickhouse_client(database=database)
    client.insert_df(table=table_name, df=batch_df)
    client.close()
    return len(batch_df)


def load_latest_batch_to_clickhouse(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    database: str = DEFAULT_DATABASE,
    table_name: str = DEFAULT_TABLE,
) -> dict[str, Any]:
    ensure_clickhouse_table(database=database, table_name=table_name)

    processed_df = load_processed_streams(input_path)
    latest_batch, latest_collected_at = prepare_latest_batch(processed_df)

    if batch_already_loaded(
        latest_collected_at=latest_collected_at,
        database=database,
        table_name=table_name,
    ):
        message = "Skipped ClickHouse load because the latest collected_at batch is already present"
        print(message)
        return {
            "status": "skipped",
            "rows": len(latest_batch),
            "collected_at": latest_collected_at.isoformat(),
            "table": f"{database}.{table_name}",
        }

    inserted_rows = insert_latest_batch(
        batch_df=latest_batch,
        database=database,
        table_name=table_name,
    )
    print(
        f"Loaded {inserted_rows} rows into ClickHouse table {database}.{table_name} for collected_at {latest_collected_at.isoformat()}"
    )
    return {
        "status": "inserted",
        "rows": inserted_rows,
        "collected_at": latest_collected_at.isoformat(),
        "table": f"{database}.{table_name}",
    }


if __name__ == "__main__":
    load_latest_batch_to_clickhouse()

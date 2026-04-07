from __future__ import annotations

from pathlib import Path

import pandas as pd

DEFAULT_INPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "twitch_streams_snapshots.csv"
)
DEFAULT_OUTPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "processed"
    / "twitch_streams_enriched.csv"
)


def load_raw_snapshots(csv_path: str | Path = DEFAULT_INPUT_PATH) -> pd.DataFrame:
    """Load raw Twitch snapshots collected from the API."""
    input_path = Path(csv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Raw Twitch CSV was not found: {input_path}")

    df = pd.read_csv(input_path)
    if df.empty:
        raise ValueError(f"Raw Twitch CSV is empty: {input_path}")

    return df


def transform_streams_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich raw Twitch snapshots with analytical features."""
    transformed = df.copy()
    current_timestamp = pd.Timestamp.now(tz="UTC").floor("s")

    if "collected_at" not in transformed.columns:
        transformed["collected_at"] = current_timestamp.isoformat()

    if "started_at" not in transformed.columns:
        transformed["started_at"] = pd.NaT

    dedupe_keys = [
        column for column in ["id", "collected_at"] if column in transformed.columns
    ]
    if dedupe_keys:
        transformed = transformed.drop_duplicates(subset=dedupe_keys).copy()

    transformed["collected_at"] = pd.to_datetime(
        transformed["collected_at"],
        utc=True,
        errors="coerce",
    )
    transformed["started_at"] = pd.to_datetime(
        transformed["started_at"],
        utc=True,
        errors="coerce",
    )
    transformed["collected_at"] = transformed["collected_at"].fillna(current_timestamp)

    transformed["viewer_count"] = pd.to_numeric(
        transformed.get("viewer_count"),
        errors="coerce",
    ).fillna(0)
    transformed["is_mature"] = (
        transformed.get("is_mature", False)
        .astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False})
        .fillna(False)
    )

    transformed["title"] = transformed.get("title", "").fillna("").astype(str)
    transformed["language"] = transformed.get("language", "unknown").fillna("unknown")
    transformed["game_name"] = transformed.get("game_name", "Unknown").fillna("Unknown")
    transformed["tags"] = transformed.get("tags", "").fillna("").astype(str)

    duration_minutes = (
        (transformed["collected_at"] - transformed["started_at"])
        .dt.total_seconds()
        .div(60)
    )
    transformed["stream_duration_minutes"] = duration_minutes.clip(lower=0).round(1)
    transformed["stream_duration_hours"] = (
        transformed["stream_duration_minutes"].div(60).round(2)
    )

    transformed["snapshot_date"] = transformed["collected_at"].dt.strftime("%Y-%m-%d")
    transformed["snapshot_hour_utc"] = transformed["collected_at"].dt.hour.astype(
        "Int64"
    )
    transformed["snapshot_weekday"] = transformed["collected_at"].dt.day_name()
    transformed["is_weekend"] = transformed["collected_at"].dt.dayofweek >= 5

    transformed["started_date"] = transformed["started_at"].dt.strftime("%Y-%m-%d")
    transformed["started_hour_utc"] = transformed["started_at"].dt.hour.astype("Int64")

    transformed["title_length"] = transformed["title"].str.len()
    transformed["tags_count"] = transformed["tags"].apply(
        lambda value: len([tag.strip() for tag in str(value).split(",") if tag.strip()])
    )
    transformed["has_tags"] = transformed["tags_count"] > 0

    transformed["viewer_rank_in_snapshot"] = (
        transformed.groupby("collected_at")["viewer_count"]
        .rank(method="dense", ascending=False)
        .astype("Int64")
    )

    if "id" in transformed.columns:
        transformed = transformed.sort_values(["id", "collected_at"]).copy()
        stream_groups = transformed.groupby("id", dropna=False)

        transformed["previous_viewer_count"] = stream_groups["viewer_count"].shift(1)
        transformed["previous_collected_at"] = stream_groups["collected_at"].shift(1)
        transformed["stream_snapshot_number"] = stream_groups.cumcount() + 1

        transformed["minutes_since_last_snapshot"] = (
            (transformed["collected_at"] - transformed["previous_collected_at"])
            .dt.total_seconds()
            .div(60)
            .round(1)
        )
        transformed["viewer_count_delta"] = (
            transformed["viewer_count"] - transformed["previous_viewer_count"]
        ).fillna(0)

        previous_non_zero = transformed["previous_viewer_count"].replace(0, pd.NA)
        transformed["viewer_count_pct_change"] = (
            (transformed["viewer_count_delta"].div(previous_non_zero).mul(100))
            .round(2)
            .fillna(0)
        )
        transformed["viewer_trend"] = transformed["viewer_count_delta"].apply(
            lambda value: "up" if value > 0 else "down" if value < 0 else "stable"
        )
    else:
        transformed["previous_viewer_count"] = pd.NA
        transformed["previous_collected_at"] = pd.NaT
        transformed["stream_snapshot_number"] = 1
        transformed["minutes_since_last_snapshot"] = pd.NA
        transformed["viewer_count_delta"] = 0
        transformed["viewer_count_pct_change"] = 0
        transformed["viewer_trend"] = "stable"

    transformed["viewer_count_bucket"] = pd.cut(
        transformed["viewer_count"],
        bins=[-1, 100, 1_000, 10_000, 50_000, float("inf")],
        labels=["0-100", "101-1K", "1K-10K", "10K-50K", "50K+"],
    )
    transformed["stream_age_bucket"] = pd.cut(
        transformed["stream_duration_minutes"],
        bins=[-1, 30, 120, 360, 720, float("inf")],
        labels=["0-30 min", "30-120 min", "2-6 hours", "6-12 hours", "12+ hours"],
    )

    transformed = transformed.sort_values(
        ["collected_at", "viewer_count"], ascending=[True, False]
    )
    return transformed


def save_transformed_snapshots(
    df: pd.DataFrame,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    """Save the processed Twitch dataset to a CSV file."""
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination, index=False)
    print(f"Saved {len(df)} transformed rows to {destination}")
    return destination


def transform_raw_snapshots(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> str:
    """Run the full transform pipeline from raw CSV to processed CSV."""
    raw_df = load_raw_snapshots(input_path)
    transformed_df = transform_streams_dataframe(raw_df)
    saved_path = save_transformed_snapshots(transformed_df, output_path)
    return str(saved_path)


if __name__ == "__main__":
    transform_raw_snapshots()

from __future__ import annotations

from pathlib import Path

import pandas as pd

DEFAULT_INPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "processed"
    / "twitch_streams_enriched.csv"
)
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "analytics"


def load_processed_streams(csv_path: str | Path = DEFAULT_INPUT_PATH) -> pd.DataFrame:
    """Load the enriched Twitch dataset produced by the transform step."""
    input_path = Path(csv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Processed Twitch CSV was not found: {input_path}")

    df = pd.read_csv(input_path)
    if df.empty:
        raise ValueError(f"Processed Twitch CSV is empty: {input_path}")

    return df


def build_analytics_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Create lightweight aggregate tables for analysis and dashboards."""
    analytics_df = df.copy()
    analytics_df["viewer_count"] = pd.to_numeric(
        analytics_df.get("viewer_count"), errors="coerce"
    ).fillna(0)
    analytics_df["stream_duration_hours"] = pd.to_numeric(
        analytics_df.get("stream_duration_hours"), errors="coerce"
    )
    analytics_df["viewer_count_delta"] = pd.to_numeric(
        analytics_df.get("viewer_count_delta"), errors="coerce"
    ).fillna(0)
    analytics_df["snapshot_hour_utc"] = pd.to_numeric(
        analytics_df.get("snapshot_hour_utc"), errors="coerce"
    )
    analytics_df["is_mature"] = (
        analytics_df.get("is_mature", False)
        .astype(str)
        .str.lower()
        .map({"true": True, "false": False})
        .fillna(False)
    )

    top_games = (
        analytics_df.groupby("game_name", dropna=False)
        .agg(
            snapshots=("id", "count"),
            avg_viewers=("viewer_count", "mean"),
            peak_viewers=("viewer_count", "max"),
            avg_stream_duration_hours=("stream_duration_hours", "mean"),
        )
        .sort_values(["avg_viewers", "peak_viewers"], ascending=False)
        .reset_index()
    )

    language_summary = (
        analytics_df.groupby("language", dropna=False)
        .agg(
            snapshots=("id", "count"),
            avg_viewers=("viewer_count", "mean"),
            peak_viewers=("viewer_count", "max"),
            avg_delta=("viewer_count_delta", "mean"),
        )
        .sort_values("avg_viewers", ascending=False)
        .reset_index()
    )

    hourly_summary = (
        analytics_df.groupby("snapshot_hour_utc", dropna=False)
        .agg(
            snapshots=("id", "count"),
            avg_viewers=("viewer_count", "mean"),
            peak_viewers=("viewer_count", "max"),
        )
        .sort_values("snapshot_hour_utc")
        .reset_index()
    )

    maturity_summary = (
        analytics_df.groupby("is_mature", dropna=False)
        .agg(
            snapshots=("id", "count"),
            avg_viewers=("viewer_count", "mean"),
            peak_viewers=("viewer_count", "max"),
            avg_delta=("viewer_count_delta", "mean"),
        )
        .reset_index()
        .sort_values("avg_viewers", ascending=False)
    )

    fastest_growing_streams = (
        analytics_df.sort_values("viewer_count_delta", ascending=False)[
            [
                "collected_at",
                "user_name",
                "game_name",
                "language",
                "viewer_count",
                "viewer_count_delta",
                "viewer_count_pct_change",
                "viewer_trend",
            ]
        ]
        .head(50)
        .reset_index(drop=True)
    )

    return {
        "top_games_summary": top_games,
        "language_summary": language_summary,
        "hourly_summary": hourly_summary,
        "maturity_summary": maturity_summary,
        "fastest_growing_streams": fastest_growing_streams,
    }


def save_analytics_tables(
    tables: dict[str, pd.DataFrame],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> list[str]:
    """Save aggregate tables as separate CSV files for reporting or dashboards."""
    destination_dir = Path(output_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)

    saved_files: list[str] = []
    for table_name, dataframe in tables.items():
        output_path = destination_dir / f"{table_name}.csv"
        dataframe.to_csv(output_path, index=False)
        print(f"Saved {table_name} to {output_path}")
        saved_files.append(str(output_path))

    return saved_files


def load_analytics_outputs(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> list[str]:
    """Run the load step: create aggregate outputs from the processed dataset."""
    processed_df = load_processed_streams(input_path)
    analytics_tables = build_analytics_tables(processed_df)
    return save_analytics_tables(analytics_tables, output_dir)


if __name__ == "__main__":
    load_analytics_outputs()

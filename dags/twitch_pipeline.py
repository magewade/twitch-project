from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.extract import collect_and_store_streams_snapshot
from src.load import load_analytics_outputs
from src.transform import transform_raw_snapshots

with DAG(
    dag_id="twitch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["twitch", "csv", "etl"],
) as dag:
    collect_streams_to_csv = PythonOperator(
        task_id="collect_streams_to_csv",
        python_callable=collect_and_store_streams_snapshot,
        op_kwargs={
            "csv_path": str(
                PROJECT_ROOT / "data" / "raw" / "twitch_streams_snapshots.csv"
            ),
            "max_pages": 10,
            "page_size": 100,
            "sleep_sec": 0.5,
        },
    )

    transform_streams_csv = PythonOperator(
        task_id="transform_streams_csv",
        python_callable=transform_raw_snapshots,
        op_kwargs={
            "input_path": str(
                PROJECT_ROOT / "data" / "raw" / "twitch_streams_snapshots.csv"
            ),
            "output_path": str(
                PROJECT_ROOT / "data" / "processed" / "twitch_streams_enriched.csv"
            ),
        },
    )

    build_analytics_outputs = PythonOperator(
        task_id="build_analytics_outputs",
        python_callable=load_analytics_outputs,
        op_kwargs={
            "input_path": str(
                PROJECT_ROOT / "data" / "processed" / "twitch_streams_enriched.csv"
            ),
            "output_dir": str(PROJECT_ROOT / "data" / "analytics"),
        },
    )

    collect_streams_to_csv >> transform_streams_csv >> build_analytics_outputs

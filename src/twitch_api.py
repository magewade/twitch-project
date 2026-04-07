from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional in Airflow container
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


def get_credentials() -> tuple[str, str]:
    client_id = os.getenv("TWITCH_CLIENT_ID")
    client_secret = os.getenv("TWITCH_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError(
            "TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set in the environment or .env file"
        )

    return client_id, client_secret


def get_app_access_token(
    client_id: str | None = None,
    client_secret: str | None = None,
) -> str:
    if client_id is None or client_secret is None:
        client_id, client_secret = get_credentials()

    response = requests.post(
        "https://id.twitch.tv/oauth2/token",
        params={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_top_games(first: int = 10) -> dict[str, Any]:
    client_id, client_secret = get_credentials()
    token = get_app_access_token(client_id, client_secret)

    response = requests.get(
        "https://api.twitch.tv/helix/games/top",
        headers={
            "Authorization": f"Bearer {token}",
            "Client-Id": client_id,
        },
        params={"first": first},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_streams_page(
    token: str,
    client_id: str,
    first: int = 100,
    after: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"first": first}
    if after:
        params["after"] = after

    response = requests.get(
        "https://api.twitch.tv/helix/streams",
        headers={
            "Authorization": f"Bearer {token}",
            "Client-Id": client_id,
        },
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def main() -> None:
    top_games = get_top_games(first=10)
    output_path = Path("top_games_raw.json")
    output_path.write_text(
        json.dumps(top_games, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(top_games, indent=2, ensure_ascii=False))
    print(f"Saved top games response to {output_path}")


if __name__ == "__main__":
    main()

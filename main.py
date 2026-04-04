import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
    raise ValueError(
        "TWITCH_CLIENT_ID и TWITCH_CLIENT_SECRET должны быть заданы в .env"
    )


def get_app_access_token() -> str:
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }

    response = requests.post(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    return data["access_token"]


def get_top_games(first: int = 10) -> dict:
    token = get_app_access_token()

    url = "https://api.twitch.tv/helix/games/top"
    headers = {
        "Authorization": f"Bearer {token}",
        "Client-Id": TWITCH_CLIENT_ID,
    }
    params = {"first": first}

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def main() -> None:
    data = get_top_games(first=10)

    print(json.dumps(data, indent=2, ensure_ascii=False))

    with open("top_games_raw.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()

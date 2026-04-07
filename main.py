from src.extract import collect_and_store_streams_snapshot
from src.twitch_api import main as api_main


if __name__ == "__main__":
    api_main()
    collect_and_store_streams_snapshot(max_pages=1, page_size=20)

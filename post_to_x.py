import logging
import os
import time

import tweepy

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

JPG_PATH = "charts/btc_usd_chart.jpg"
MAX_ATTEMPTS = 3
RETRY_SECONDS = 60
CAPTION = "Daily BTC/USD Power Law Probability Channel Chart #Bitcoin"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def post_to_x() -> None:
    api_key = _require_env("X_API_KEY")
    api_secret = _require_env("X_API_SECRET")
    access_token = _require_env("X_ACCESS_TOKEN")
    access_token_secret = _require_env("X_ACCESS_TOKEN_SECRET")

    if not os.path.exists(JPG_PATH):
        raise FileNotFoundError(f"JPG file not found: {JPG_PATH}")

    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    api.verify_credentials()
    logging.info("X API authentication successful")

    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logging.info("Posting chart to X (attempt %s/%s)", attempt, MAX_ATTEMPTS)
            media = api.media_upload(filename=JPG_PATH)
            client = tweepy.Client(
                consumer_key=api_key,
                consumer_secret=api_secret,
                access_token=access_token,
                access_token_secret=access_token_secret,
            )
            client.create_tweet(text=CAPTION, media_ids=[media.media_id])
            logging.info("Posted JPG chart to X successfully")
            return
        except Exception as exc:
            last_error = exc
            logging.warning("X post attempt %s failed: %s", attempt, exc)
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_SECONDS)

    raise RuntimeError(f"Failed to post to X after {MAX_ATTEMPTS} attempts") from last_error


if __name__ == "__main__":
    post_to_x()

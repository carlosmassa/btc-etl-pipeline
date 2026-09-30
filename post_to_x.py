import logging
import os
import time
from datetime import datetime, timezone

import tweepy

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MAX_ATTEMPTS = 3
RETRY_SECONDS = 60
DAILY_CHARTS = [
    ("charts/btc_usd_chart.jpg", "Daily BTCUSD Power Law Probability Channel #Bitcoin"),
    ("charts/btc_gold_ratio_chart.jpg", "Daily BTC/GOLD Power Law Probability Channel #Bitcoin #Gold"),
]
MONTHLY_CHARTS = [
    ("charts/btc_usd_stability.jpg", "Monthly BTCUSD Power Law Stabilization #Bitcoin"),
    ("charts/btc_gold_ratio_stability.jpg", "Monthly BTC/GOLD Power Law Stabilization #Bitcoin #Gold"),
]


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _post_one(api, client, jpg_path: str, caption: str) -> None:
    if not os.path.exists(jpg_path):
        raise FileNotFoundError(f"JPG file not found: {jpg_path}")
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logging.info("Posting %s to X (attempt %s/%s)", jpg_path, attempt, MAX_ATTEMPTS)
            media = api.media_upload(filename=jpg_path)
            client.create_tweet(text=caption, media_ids=[media.media_id])
            logging.info("Posted %s to X successfully", jpg_path)
            return
        except Exception as exc:
            last_error = exc
            logging.warning("X post attempt %s for %s failed: %s", attempt, jpg_path, exc)
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_SECONDS)
    raise RuntimeError(f"Failed to post {jpg_path} after {MAX_ATTEMPTS} attempts") from last_error


def charts_for_today(now=None):
    now = now or datetime.now(timezone.utc)
    charts = list(DAILY_CHARTS)
    if now.day == 1:
        logging.info("First of the month (%s) — also posting stability JPGs", now.date())
        charts.extend(MONTHLY_CHARTS)
    else:
        logging.info("Day %s — posting only the two probability-channel JPGs", now.day)
    return charts


def post_to_x() -> None:
    api_key = _require_env("X_API_KEY")
    api_secret = _require_env("X_API_SECRET")
    access_token = _require_env("X_ACCESS_TOKEN")
    access_token_secret = _require_env("X_ACCESS_TOKEN_SECRET")

    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    api.verify_credentials()
    logging.info("X API authentication successful")

    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )
    for jpg_path, caption in charts_for_today():
        _post_one(api, client, jpg_path, caption)


if __name__ == "__main__":
    post_to_x()

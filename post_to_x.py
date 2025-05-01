import tweepy
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def post_to_x():
    try:
        # Load X API credentials from environment variables
        api_key = os.getenv('X_API_KEY')
        api_secret = os.getenv('X_API_SECRET')
        access_token = os.getenv('X_ACCESS_TOKEN')
        access_token_secret = os.getenv('X_ACCESS_TOKEN_SECRET')

        # Authenticate with X API
        auth = tweepy.OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)

        # Verify credentials
        api.verify_credentials()
        logging.info("X API authentication successful")

        # Post JPG with caption
        jpg_path = "charts/btc_usd_chart.jpg"
        if not os.path.exists(jpg_path):
            raise FileNotFoundError(f"JPG file not found: {jpg_path}")

        caption = "Daily BTC/USD Price Chart #Bitcoin"
        media = api.media_upload(jpg_path)
        api.update_status(status=caption, media_ids=[media.media_id])
        logging.info("Posted JPG chart to X successfully")
    except Exception as e:
        logging.error(f"Failed to post to X: {str(e)}")
        raise

if __name__ == "__main__":
    post_to_x()

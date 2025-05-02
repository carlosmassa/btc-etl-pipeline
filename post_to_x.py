import tweepy
import os
import logging
import time
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def post_to_x():
    try:
        # Load X API credentials from environment variables
        api_key = os.getenv('X_API_KEY')
        api_secret = os.getenv('X_API_SECRET')
        access_token = os.getenv('X_ACCESS_TOKEN')
        access_token_secret = os.getenv('X_ACCESS_TOKEN_SECRET')
        BEARER_TOKEN = os.getenv('X_BEARER_TOKEN')

        # Authenticate with X API (v1.1)
        def create_api():
            auth = tweepy.OAuthHandler(api_key, api_secret)
            auth.set_access_token(access_token, access_token_secret)
            # Create API object
            api = tweepy.API(auth, wait_on_rate_limit=True)
            try:
                api.verify_credentials()
            except Exception as e:
                logging.info("Error creating API")
                print("Error creating API")
                raise e
            logging.info("API created")
            print("API created")
            return api

        api = create_api()
        
        # Verify credentials
        try:
            api.verify_credentials()
            logging.info("X API authentication successful!")
        except Exception as e:
            logging.info("X API authentication failed!")

        jpg_path = "charts/btc_usd_chart.jpg"
        if not os.path.exists(jpg_path):
            raise FileNotFoundError(f"JPG file not found: {jpg_path}")
        logging.info(f"JPG file found: {jpg_path}")

        MAX_RETRY_DURATION = 5 * 60 * 60  # 5 hours in seconds
        RETRY_INTERVAL = 5 * 60           # 5 minutes in seconds

        start_time = time.time()

        while True:
            try:
                # Try to post the JPG Chart to X
                logging.info("Trying to post the JPG Chart to X...")

                # Example: raise an error for testing
                # raise Exception("Simulated error")
                
                media = api.media_upload(filename=jpg_path)
                media_id = media.media_id
                logging.info(f"Media uploaded successfully, media ID: {media_id}")
        
                client = tweepy.Client(
                    consumer_key=api_key,
                    consumer_secret=api_secret,
                    access_token=access_token,
                    access_token_secret=access_token_secret)
        
                # Create a tweet
                caption = "Daily BTC/USD Power Law Probability Channel Chart #Bitcoin"
                client.create_tweet(text=caption, media_ids=[media_id])
                logging.info("Posted JPG chart to X successfully using v2 endpoint")
                break
            except Exception as e:
                elapsed = time.time() - start_time
                if elapsed >= MAX_RETRY_DURATION:
                    raise TimeoutError(f"Operation failed after {MAX_RETRY_DURATION/3600} hours") from e
        
                logging.info(f"[{datetime.datetime.now()}] Error: {e}")
                minutes_left = int((MAX_RETRY_DURATION - elapsed) // 60)
                logging.info(f"Retrying in {RETRY_INTERVAL // 60} minutes... {minutes_left} minutes left until giving up.")
                time.sleep(RETRY_INTERVAL)
    
    except Exception as e:
        logging.error(f"Failed to post to X: {str(e)}")
        raise

if __name__ == "__main__":
    post_to_x()

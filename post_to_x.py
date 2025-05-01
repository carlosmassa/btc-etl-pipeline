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
        BEARER_TOKEN = os.getenv('X_BEARER_TOKEN')

        # Using the tweepy.Client which supports API v2
        client = tweepy.Client(bearer_token=BEARER_TOKEN, 
                               consumer_key=api_key,
                               consumer_secret=api_secret,
                               access_token=access_token,
                               access_token_secret=access_token_secret)

        # Verify credentials
        try:
            client.get_me()  # This is a simple API v2 call to verify credentials
            logging.info("X API authentication successful!")
        except Exception as e:
            logging.error("X API authentication failed!")
            raise e

        # Define the tweet text
        tweet = 'This is an automated test tweet using #Python $BTC'
        print(f"Tweeting: {tweet}")

        # Post the tweet using API v2
        client.create_tweet(text=tweet)

    except Exception as e:
        logging.error(f"Failed to post to X: {str(e)}")
        raise

if __name__ == "__main__":
    post_to_x()

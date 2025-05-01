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

        client = tweepy.Client(bearer_token=BEARER_TOKEN)

        # Authenticate with X API (v1.1)
        def create_api():
            auth = tweepy.OAuthHandler(api_key, api_secret)
            auth.set_access_token(access_token, access_token_secret)
            #auth = tweepy.OAuthHandler("SwY5SACoDuOwnJ36LpmBKXZQK", "ka5oIz4uuW5uESfnJU0jA5CnVLcpwxEOtH14nGUgxPWy90peYM")
            #auth.set_access_token("94077742-CXA2QJ1niofEF91NUHtFbyhdeF3jTSP1REi3Zzek8", "kTifIRru2sv8ITYErTHa9FAj0IV7IABlv5uP8v7jTkuYr")
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

        #get my user id and follower count
        user = api.get_user(screen_name='carlesmassa')
        print(user.id)
        print(user.screen_name)  # User Name
        print(user.followers_count) #User Follower Count

        # Create a tweet
        # Define the tweet text
        tweet = 'This is an automated test tweet using #Python $BTC'
        print(tweet)

        # Generate text tweet
        api.update_status(tweet)

        # Post JPG with caption using v1.1 endpoint
        jpg_path = "charts/btc_usd_chart.jpg"
        if not os.path.exists(jpg_path):
            raise FileNotFoundError(f"JPG file not found: {jpg_path}")

        caption = "Daily BTC/USD Price Chart (2009–2025) #Bitcoin #Crypto"
        api.update_status_with_media(filename=jpg_path, status=caption)
        logging.info("Posted JPG chart to X successfully using v1.1 endpoint")
    except Exception as e:
        logging.error(f"Failed to post to X: {str(e)}")
        raise

if __name__ == "__main__":
    post_to_x()

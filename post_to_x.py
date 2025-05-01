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
        #user = api.get_user(screen_name='carlesmassa')
        #print(user.id)
        #print(user.screen_name)  # User Name
        #print(user.followers_count) #User Follower Count

        client = tweepy.Client(consumer_key=api_key, consumer_secret=api_secret, access_token=access_token, access_token_secret=access_token_secret)
        # Create a tweet
        # Define the tweet text
        tweet = 'This is an automated test tweet using #Python $BTC'
        print(tweet)

        # Generate text tweet
        client.create_tweet(text="This is an automated test tweet using Python")

    except Exception as e:
        logging.error(f"Failed to post to X: {str(e)}")
        raise

if __name__ == "__main__":
    post_to_x()

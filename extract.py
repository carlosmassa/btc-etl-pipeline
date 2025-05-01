import pandas as pd
from coinmetrics.api_client import CoinMetricsClient
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data():
    try:
        # Initialize Coin Metrics client
        client = CoinMetricsClient()
        
        # Define parameters
        asset = "btc"
        metric = "ReferenceRateUSD"
        frequency = "1d"
        end_time = datetime.now().strftime('%Y-%m-%d')
        start_time = "2009-01-03"  # Bitcoin's genesis block
        page_size = 1000  # Records per page

        logging.info(f"Fetching BTC/USD price data from {start_time} to {end_time}...")
        all_data = []
        next_page_token = None

        # Paginate through all data
        while True:
            response = client.get_asset_metrics(
                assets=asset,
                metrics=metric,
                frequency=frequency,
                start_time=start_time,
                end_time=end_time,
                page_size=page_size,
                next_page_token=next_page_token
            )
            df = response.to_dataframe()
            all_data.append(df)
            logging.info(f"Fetched {len(df)} records for page (total so far: {sum(len(d) for d in all_data)})")
            
            # Check for next page
            next_page_token = response.next_page_token
            if not next_page_token:
                break

        # Combine all pages
        if all_data:
            data = pd.concat(all_data, ignore_index=True)
            # Ensure unique records by time
            data = data.drop_duplicates(subset=['time'])
            data = data.sort_values('time')
        else:
            data = pd.DataFrame()

        # Save raw data
        data.to_csv("raw_btc_usd.csv", index=False)
        logging.info(f"Extracted {len(data)} records to raw_btc_usd.csv")
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    extract_data()

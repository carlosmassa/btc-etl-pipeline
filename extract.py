import pandas as pd
from coinmetrics.api_client import CoinMetricsClient
from datetime import datetime
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

        logging.info(f"Fetching BTC/USD price data from {start_time} to {end_time}...")
        metrics = client.get_asset_metrics(
            assets=asset,
            metrics=metric,
            frequency=frequency,
            start_time=start_time,
            end_time=end_time
        )
        data = pd.DataFrame(metrics)
        
        # Ensure correct column format
        data['time'] = pd.to_datetime(data['time'])
        data['asset'] = asset
        data['ReferenceRateUSD'] = pd.to_numeric(data['ReferenceRateUSD'], errors='coerce')
        data = data[['time', 'asset', 'ReferenceRateUSD']]
        data = data.sort_values('time')

        # Save raw data
        data.to_csv("raw_btc_usd.csv", index=False)
        logging.info(f"Extracted {len(data)} records to raw_btc_usd.csv")
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    extract_data()

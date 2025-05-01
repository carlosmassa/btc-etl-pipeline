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
        metric = "PriceUSD"
        frequency = "1d"

        logging.info(f"Fetching BTC/USD price data")
        metrics = client.get_asset_metrics(
            assets=asset,
            metrics=metric,
            frequency=frequency,
        )
        data = pd.DataFrame(metrics)

        # Ensure correct column format
        data['time'] = pd.to_datetime(data['time'])
        data['asset'] = asset
        data['PriceUSD'] = pd.to_numeric(data['PriceUSD'], errors='coerce')
        data = data[['time', 'asset', 'PriceUSD']]
        data = data.sort_values('time')

        # Save raw data
        data.to_csv("raw_btc_usd.csv", index=False)
        logging.info(f"Extracted {len(data)} records to raw_btc_usd.csv")
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    extract_data()

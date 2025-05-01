import os
import shutil
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data():
    try:
        # Create charts directory if it doesn't exist
        os.makedirs("charts", exist_ok=True)
        
        # Define destination path (fixed filename, no date)
        destination = "charts/btc_usd_chart.html"
        
        # Move btc_usd_chart.html to charts/, overwriting if exists
        shutil.move("btc_usd_chart.html", destination)
        logging.info(f"Moved chart to {destination} for commit to gh-pages branch")
    except Exception as e:
        logging.error(f"Load failed: {str(e)}")
        raise

if __name__ == "__main__":
    load_data()

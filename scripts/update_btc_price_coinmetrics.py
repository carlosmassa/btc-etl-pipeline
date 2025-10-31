import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from coinmetrics.api_client import CoinMetricsClient

# === CONFIG ===
# Local path to CSV
CSV_PATH = Path("data/BTC_Prices.csv")

# Optional: raw GitHub URL fallback
GITHUB_RAW_CSV = "https://raw.githubusercontent.com/carlosmassa/btc-etl-pipeline/main/data/BTC_Prices.csv"

ASSET = "btc"
METRIC = "PriceUSD"
FREQUENCY = "1d"


def log(msg: str):
    """Formatted logging with timestamps for GitHub Actions."""
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)


def get_btc_data(start_date: str) -> pd.DataFrame:
    """
    Extract BTC PriceUSD data from CoinMetrics starting from `start_date`.
    Returns a DataFrame with 'Date' and 'Value' columns.
    """
    client = CoinMetricsClient()
    try:
        metrics = client.get_asset_metrics(
            assets=ASSET,
            metrics=[METRIC],
            frequency=FREQUENCY,
            start_time=start_date
        )
        df = pd.DataFrame(metrics)
    except Exception as e:
        log(f"❌ Error fetching data from CoinMetrics: {e}")
        return pd.DataFrame(columns=["Date", "Value"])

    if df.empty:
        return pd.DataFrame(columns=["Date", "Value"])

    df["Date"] = pd.to_datetime(df["time"]).dt.date
    df["Value"] = pd.to_numeric(df[METRIC], errors="coerce")
    df.dropna(subset=["Value"], inplace=True)

    return df[["Date", "Value"]]


def load_existing_csv() -> pd.DataFrame:
    """Load existing CSV, or fallback to GitHub raw URL if missing."""
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH, parse_dates=["Date"])
        log(f"✅ Loaded local CSV with {len(df)} rows.")
        return df
    else:
        log("⚠️ CSV file not found locally. Trying GitHub raw URL...")
        try:
            df = pd.read_csv(GITHUB_RAW_CSV, parse_dates=["Date"])
            log(f"✅ Loaded CSV from GitHub with {len(df)} rows.")
            return df
        except Exception as e:
            log(f"⚠️ Could not fetch CSV from GitHub: {e}")
            log("ℹ️ Creating new empty DataFrame.")
            return pd.DataFrame(columns=["Date", "Value"])


def update_csv():
    """Append new BTC prices from CoinMetrics since the last CSV date."""
    log("🚀 Starting CoinMetrics BTC price update process...")

    # --- Load existing CSV ---
    df_existing = load_existing_csv()

    if df_existing.empty:
        start_date = "2010-07-17"  # Bitcoin's first available date
        log("ℹ️ Existing CSV is empty. Starting from first BTC data date.")
    else:
        # Ensure Date column is datetime
        df_existing["Date"] = pd.to_datetime(df_existing["Date"], errors="coerce")
        last_date = df_existing["Date"].max().date()
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"📊 Existing CSV has {len(df_existing)} rows (last date: {last_date}).")
        log(f"📆 Fetching data from {start_date} onwards...")

    # --- Fetch new data ---
    df_new = get_btc_data(start_date)
    if df_new.empty:
        log("ℹ️ No new data returned by CoinMetrics. Nothing to update.")
        return

    log(f"✅ Retrieved {len(df_new)} new rows from CoinMetrics.")
    log(f"🕒 New data covers {df_new['Date'].min()} → {df_new['Date'].max()}.")

    # --- Merge datasets ---
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined.drop_duplicates(subset=["Date"], inplace=True)
    df_combined.sort_values("Date", inplace=True)

    # --- Save output ---
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(CSV_PATH, index=False)

    log("💾 CSV updated successfully.")
    log(f"📈 Added {len(df_new)} new rows. Total rows now: {len(df_combined)}.")
    log(f"🗓️ Latest available date: {df_combined['Date'].max().date()}.")
    log("✅ CoinMetrics BTC price update completed successfully.\n")


if __name__ == "__main__":
    try:
        update_csv()
    except Exception as e:
        log(f"🔥 Fatal error during ETL process: {e}")
        raise
        

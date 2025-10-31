import pandas as pd
from datetime import datetime, timedelta, date
from pathlib import Path
from coinmetrics.api_client import CoinMetricsClient

# === CONFIG ===
CSV_PATH = Path("data/BTC_Prices.csv")
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
    else:
        log("⚠️ CSV file not found locally. Trying GitHub raw URL...")
        try:
            df = pd.read_csv(GITHUB_RAW_CSV, parse_dates=["Date"])
            log(f"✅ Loaded CSV from GitHub with {len(df)} rows.")
        except Exception as e:
            log(f"⚠️ Could not fetch CSV from GitHub: {e}")
            log("ℹ️ Creating new empty DataFrame.")
            df = pd.DataFrame(columns=["Date", "Value"])

    # Ensure Date column is datetime
    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    return df


def update_csv():
    """Append new BTC prices from CoinMetrics since the last CSV date."""
    log("🚀 Starting CoinMetrics BTC price update process...")

    df_existing = load_existing_csv()

    if df_existing.empty:
        start_date_obj = datetime.strptime("2010-07-17", "%Y-%m-%d").date()  # first BTC data
        log("ℹ️ Existing CSV is empty. Starting from first BTC data date.")
    else:
        last_date = df_existing["Date"].max().date()
        start_date_obj = last_date + timedelta(days=1)
        log(f"📊 Existing CSV has {len(df_existing)} rows (last date: {last_date}).")

    today = date.today()
    if start_date_obj > today:
        log(f"ℹ️ Start date {start_date_obj} is in the future. Nothing to fetch.")
        return

    start_date = start_date_obj.strftime("%Y-%m-%d")
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

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from coinmetrics.api_client import CoinMetricsClient
import sys

# === CONFIG ===
CSV_PATH = Path("data/BTC_Prices.csv")
ASSET = "btc"
METRIC = "PriceUSD"
GITHUB_RAW_CSV = "https://raw.githubusercontent.com/YOUR_GITHUB_USERNAME/YOUR_REPO/main/data/BTC_Prices.csv"

# === LOGGING ===
def log(msg: str):
    """Print timestamped log message"""
    print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S UTC}] {msg}")

# === LOAD EXISTING CSV ===
def load_existing_csv() -> pd.DataFrame:
    """Load existing CSV, or fallback to GitHub raw URL if missing."""
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH, dtype={"Date": str, "Value": float})
        log(f"✅ Loaded local CSV with {len(df)} rows.")
    else:
        log("⚠️ CSV file not found locally. Trying GitHub raw URL...")
        try:
            df = pd.read_csv(GITHUB_RAW_CSV, dtype={"Date": str, "Value": float})
            log(f"✅ Loaded CSV from GitHub with {len(df)} rows.")
        except Exception as e:
            log(f"⚠️ Could not fetch CSV from GitHub: {e}")
            log("ℹ️ Creating new empty DataFrame.")
            df = pd.DataFrame(columns=["Date", "Value"])

    # --- Enforce correct date parsing (DD/MM/YYYY) ---
    if not df.empty:
        try:
            df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
        except Exception:
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")

    return df

# === FETCH FROM COINMETRICS ===
def fetch_from_coinmetrics(start_date: str) -> pd.DataFrame:
    """Fetch new BTC daily price data from CoinMetrics."""
    client = CoinMetricsClient()
    log(f"📆 Fetching data from {start_date} onwards...")

    try:
        df_new = client.get_asset_metrics(
            assets=[ASSET],
            metrics=[METRIC],
            frequency="1d",
            start_time=start_date,
        ).to_dataframe()

        if df_new.empty:
            log("ℹ️ No new data returned by CoinMetrics.")
            return pd.DataFrame(columns=["Date", "Value"])

        df_new.reset_index(inplace=True)
        df_new = df_new.rename(columns={"time": "Date", METRIC: "Value"})
        df_new["Date"] = pd.to_datetime(df_new["Date"]).dt.date
        df_new = df_new[["Date", "Value"]]
        log(f"✅ Fetched {len(df_new)} new rows from CoinMetrics.")
        return df_new

    except Exception as e:
        log(f"❌ Error fetching data from CoinMetrics: {e}")
        return pd.DataFrame(columns=["Date", "Value"])

# === MAIN UPDATE LOGIC ===
def main():
    log("🚀 Starting CoinMetrics BTC price update process...")

    df_existing = load_existing_csv()

    if df_existing.empty:
        log("⚠️ No existing data found. Fetching full history.")
        start_date = "2010-07-17"
    else:
        last_date = df_existing["Date"].max()
        log(f"📊 Existing CSV has {len(df_existing)} rows (last date: {last_date:%Y-%m-%d}).")
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")

    today = datetime.utcnow().date()

    # Prevent future date fetches
    if datetime.strptime(start_date, "%Y-%m-%d").date() > today:
        log(f"ℹ️ Start date {start_date} is in the future. Nothing to fetch.")
        sys.exit(0)

    df_new = fetch_from_coinmetrics(start_date)

    if not df_new.empty:
        df_updated = pd.concat([df_existing, df_new]).drop_duplicates(subset="Date").sort_values("Date")
        df_updated["Date"] = df_updated["Date"].dt.strftime("%d/%m/%Y")
        df_updated.to_csv(CSV_PATH, index=False)
        log(f"💾 CSV updated successfully. Added {len(df_new)} new rows. Now {len(df_updated)} total.")
        log(f"✅ Last date updated: {df_updated['Date'].iloc[-1]}")
    else:
        log("ℹ️ No new data to append. CSV remains unchanged.")

# === RUN ===
if __name__ == "__main__":
    main()

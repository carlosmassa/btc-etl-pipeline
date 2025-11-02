import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from coinmetrics.api_client import CoinMetricsClient

# === CONFIG ===
CSV_PATH = Path("data/BTC_Prices.csv")
GITHUB_RAW_CSV = "https://raw.githubusercontent.com/carlosmassa/btc-etl-pipeline/main/data/BTC_Prices.csv"
ASSET = "btc"
METRIC = "PriceUSD"
FREQUENCY = "1d"


def log(msg: str):
    """Formatted UTC logging for GitHub Actions."""
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)


def load_existing_csv() -> pd.DataFrame:
    """Load BTC price CSV, ensuring DD/MM/YYYY date format is correctly parsed."""
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

    # --- Enforce proper date parsing (DD/MM/YYYY) ---
    if not df.empty:
        try:
            df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
        except Exception:
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")

    return df


def get_btc_data(start_date: str) -> pd.DataFrame:
    """Fetch BTC PriceUSD data from CoinMetrics starting from `start_date`."""
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

    # --- Normalize data ---
    df["Date"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
    df["Value"] = pd.to_numeric(df[METRIC], errors="coerce")
    df.dropna(subset=["Value"], inplace=True)

    return df[["Date", "Value"]]


def main():
    log("🚀 Starting CoinMetrics BTC price update process...")

    df_existing = load_existing_csv()

    if df_existing.empty:
        start_date = "2010-07-17"  # first available BTC data
        log("ℹ️ No existing data. Starting from 2010-07-17.")
    else:
        last_date = df_existing["Date"].max()
        log(f"📊 Existing CSV has {len(df_existing)} rows (last date: {last_date.date()}).")

        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")

        # Avoid requesting future dates
        if datetime.strptime(start_date, "%Y-%m-%d").date() > datetime.utcnow().date():
            log(f"ℹ️ Start date {start_date} is in the future. Nothing to fetch.")
            return

        log(f"📆 Fetching data from {start_date} onwards...")

    # --- Fetch new data ---
    df_new = get_btc_data(start_date)

    if df_new.empty:
        log("ℹ️ No new data returned by CoinMetrics. Nothing to update.")
        return

    log(f"✅ Fetched {len(df_new)} new rows from CoinMetrics.")
    log(f"🕒 New data covers {df_new['Date'].min().date()} → {df_new['Date'].max().date()}.")

    # --- Normalize Date types before merging ---
    df_existing["Date"] = pd.to_datetime(df_existing["Date"], errors="coerce")
    df_new["Date"] = pd.to_datetime(df_new["Date"], errors="coerce")

    # --- Merge and sort ---
    df_updated = (
        pd.concat([df_existing, df_new])
        .drop_duplicates(subset="Date")
        .sort_values("Date")
    )

    # --- Format before saving ---
    # --- Save in DD/MM/YYYY format ---
    df_updated["Date"] = df_updated["Date"].dt.strftime("%d/%m/%Y")
    # --- Limit to two decimals ---
    df_updated["Value"] = df_updated["Value"].round(2)  # ✅ round to 2 decimals

    # --- Save to CSV ---
    df_updated.to_csv(CSV_PATH, index=False)


    log(f"💾 CSV updated successfully. Added {len(df_new)} new rows. Now {len(df_updated)} total.")
    log(f"✅ Last date updated: {df_updated['Date'].iloc[-1]}")
    log("🎉 CoinMetrics BTC price update completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"🔥 Fatal error during ETL process: {e}")
        raise

import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
import requests
import os

# === CONFIG ===
CSV_PATH = Path("data/LBMA-gold_D-gold_D_USD_PM.csv")
GITHUB_RAW_CSV = "https://raw.githubusercontent.com/carlosmassa/btc-etl-pipeline/main/data/LBMA-gold_D-gold_D_USD_PM.csv"
SYMBOL = "lbma_gold_pm"  # Metals.Dev symbol for LBMA Gold PM USD
MAX_DAYS_PER_CALL = 30

# Load API key
API_KEY = os.getenv("METALS_DEV_API_KEY")
if not API_KEY:
    raise ValueError("❌ METALS_DEV_API_KEY environment variable not found. Set it before running.")
else:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] 🔑 METALS_DEV_API_KEY loaded successfully (length: {len(API_KEY)} chars)")

def log(msg: str):
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)

def load_existing_csv() -> pd.DataFrame:
    """Load CSV from local path or GitHub raw URL."""
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH, dtype={"Date": str, "Value": float})
        log(f"✅ Loaded local CSV with {len(df)} rows.")
    else:
        log("⚠️ CSV not found locally. Trying GitHub raw URL...")
        try:
            df = pd.read_csv(GITHUB_RAW_CSV, dtype={"Date": str, "Value": float})
            log(f"✅ Loaded CSV from GitHub with {len(df)} rows.")
        except Exception as e:
            log(f"⚠️ Could not fetch CSV from GitHub: {e}")
            df = pd.DataFrame(columns=["Date", "Value"])
    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])
    return df

def fetch_timeseries(start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch timeseries data from Metals.Dev API."""
    url = (
        f"https://api.metals.dev/v1/timeseries"
        f"?api_key={API_KEY}"
        f"&symbols={SYMBOL}"
        f"&start_date={start_date.isoformat()}"
        f"&end_date={end_date.isoformat()}"
    )
    try:
        resp = requests.get(url)
        data = resp.json()
        rows = []
        if data.get("rates"):
            for d_str, day_data in data["rates"].items():
                if "metals" in day_data and "gold" in day_data["metals"]:
                    rows.append({"Date": pd.to_datetime(d_str), "Value": day_data["metals"]["gold"]})
        if not rows:
            log(f"⚠️ No values returned for {start_date} → {end_date}.")
        return pd.DataFrame(rows)
    except Exception as e:
        log(f"❌ Error fetching timeseries: {e}")
        return pd.DataFrame(columns=["Date", "Value"])

def fill_weekends(df: pd.DataFrame) -> pd.DataFrame:
    """Fill Saturdays and Sundays by carrying forward the last Friday's value."""
    full_dates = pd.date_range(start=df["Date"].min(), end=df["Date"].max(), freq="D")
    df_full = pd.DataFrame({"Date": full_dates})
    df_full = df_full.merge(df, on="Date", how="left")
    df_full["Value"] = df_full["Value"].ffill()
    log(f"✅ Weekends filled by carrying forward last Friday's value. Total rows now: {len(df_full)}")
    return df_full

def main():
    log("🚀 Starting LBMA Gold PM USD ETL process...")

    # Load existing CSV
    df_existing = load_existing_csv()

    # Fill missing weekends
    df_existing = fill_weekends(df_existing)

    # Write CSV immediately (fix historical weekends)
    df_existing["Value"] = df_existing["Value"].round(2)
    df_existing["Date"] = df_existing["Date"].dt.strftime("%Y-%m-%d")
    df_existing.to_csv(CSV_PATH, index=False)
    log("💾 Historical CSV updated successfully with weekends filled.")

    # Fetch new data from last date to today
    last_date = pd.to_datetime(df_existing["Date"]).max().date()
    today = date.today()

    if last_date >= today:
        log("ℹ️ CSV is already up-to-date. No API calls needed.")
        return

    log(f"📅 Fetching missing dates: {last_date + timedelta(days=1)} → {today}")
    df_new = fetch_timeseries(last_date + timedelta(days=1), today)
    if df_new.empty:
        log("ℹ️ No new data fetched from API. CSV remains as-is.")
        return

    df_updated = pd.concat([pd.to_datetime(df_existing.assign(Date=df_existing["Date"])), df_new]).drop_duplicates(subset="Date").sort_values("Date")
    df_updated["Value"] = df_updated["Value"].round(2)
    df_updated["Date"] = df_updated["Date"].dt.strftime("%Y-%m-%d")
    df_updated.to_csv(CSV_PATH, index=False)
    log(f"💾 CSV updated successfully with new API data. Total rows: {len(df_updated)}")
    log(f"✅ Last date updated: {df_updated['Date'].iloc[-1]}")
    log("🎉 LBMA Gold PM USD ETL completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"🔥 Fatal error during ETL: {e}")
        raise


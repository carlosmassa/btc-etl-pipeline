import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
import requests
import os

# === CONFIG ===
CSV_PATH = Path("data/LBMA-gold_D-gold_D_USD_PM.csv")
SYMBOL = "lbma_gold_pm"  # Metals.Dev symbol for LBMA Gold PM USD

# Load API key
API_KEY = os.getenv("METALS_DEV_API_KEY")
if not API_KEY:
    raise ValueError("❌ METALS_DEV_API_KEY environment variable not found. Set it before running.")
else:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] 🔑 METALS_DEV_API_KEY loaded successfully (length: {len(API_KEY)} chars)")

def log(msg: str):
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)

def load_existing_csv() -> pd.DataFrame:
    """Load local CSV."""
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH, dtype={"Date": str, "Value": float})
        log(f"✅ Loaded local CSV with {len(df)} rows.")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])
    else:
        log("⚠️ CSV not found locally. Starting with empty DataFrame.")
        df = pd.DataFrame(columns=["Date", "Value"])
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

def fill_missing_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing dates (like weekends) by carrying forward last known value."""
    full_dates = pd.date_range(start=df["Date"].min(), end=df["Date"].max(), freq="D")
    df_full = pd.DataFrame({"Date": full_dates})
    df_full = df_full.merge(df, on="Date", how="left")
    df_full["Value"] = df_full["Value"].ffill()
    return df_full

def main():
    log("🚀 Starting LBMA Gold PM USD ETL process...")

    df_existing = load_existing_csv()
    last_date = df_existing["Date"].max().date() if not df_existing.empty else date.today() - timedelta(days=1)
    today = date.today()

    if last_date < today:
        log(f"📅 Fetching missing dates: {last_date + timedelta(days=1)} → {today}")
        df_new = fetch_timeseries(last_date + timedelta(days=1), today)
        if not df_new.empty:
            df_existing = pd.concat([df_existing, df_new]).drop_duplicates(subset="Date").sort_values("Date")
            log(f"💾 Fetched {len(df_new)} new rows from API.")
        else:
            log("ℹ️ No new data fetched from API.")

    # Fill any missing dates (weekends or gaps)
    df_existing = fill_missing_dates(df_existing)
    df_existing["Value"] = df_existing["Value"].round(2)
    df_existing["Date"] = df_existing["Date"].dt.strftime("%Y-%m-%d")
    df_existing.to_csv(CSV_PATH, index=False)

    log(f"💾 CSV updated successfully. Total rows: {len(df_existing)}")
    log(f"✅ Last date in CSV: {df_existing['Date'].iloc[-1]}")
    log("🎉 LBMA Gold PM USD ETL completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"🔥 Fatal error during ETL: {e}")
        raise



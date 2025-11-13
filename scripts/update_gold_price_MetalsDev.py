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

# Load API key from environment variable
API_KEY = os.getenv("METALS_DEV_API_KEY")
if not API_KEY:
    raise ValueError("❌ METALS_DEV_API_KEY environment variable not found. Set it before running.")
else:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] 🔑 METALS_DEV_API_KEY loaded successfully (length: {len(API_KEY)} chars)")


def log(msg: str):
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)


def load_existing_csv() -> pd.DataFrame:
    """Load CSV from local path or GitHub raw URL, ensuring clean dates."""
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
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
        before_drop = len(df)
        df = df.dropna(subset=["Date"])
        dropped = before_drop - len(df)
        if dropped > 0:
            log(f"⚠️ Dropped {dropped} rows with invalid or missing dates.")
    return df


def calculate_effective_pm_date(target_date: date) -> date:
    """Return the previous business day's PM fix date for a given target date."""
    if target_date.weekday() == 0:  # Monday → Friday PM
        return target_date - timedelta(days=3)
    elif target_date.weekday() == 6:  # Sunday → skip
        return None
    elif target_date.weekday() == 5:  # Saturday → Friday PM
        return target_date - timedelta(days=1)
    else:  # Tue-Fri → previous day
        return target_date - timedelta(days=1)


def generate_missing_dates(last_date: date, today: date, existing_dates: set):
    """Generate all missing effective PM dates between last_date and today."""
    dates = []
    current = last_date + timedelta(days=1)
    while current <= today:
        effective_date = calculate_effective_pm_date(current)
        if effective_date is not None and effective_date not in existing_dates:
            dates.append(effective_date)
        current += timedelta(days=1)
    return sorted(list(set(dates)))


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

        # Determine correct container key
        rows = []
        container_key = None
        if "timeseries" in data and isinstance(data["timeseries"], dict):
            container_key = "timeseries"
        elif "rates" in data and isinstance(data["rates"], dict):
            container_key = "rates"

        if container_key:
            for d_str, prices in data[container_key].items():
                if isinstance(prices, dict) and SYMBOL in prices:
                    rows.append({"Date": pd.to_datetime(d_str), "Value": prices[SYMBOL]})

            if "quota" in data:
                log(f"ℹ️ API quota used: {data['quota'].get('used','N/A')}, remaining: {data['quota'].get('remaining','N/A')}")

            if not rows:
                log(f"⚠️ No values returned in '{container_key}' for {start_date} → {end_date}.")
            return pd.DataFrame(rows)

        else:
            log(f"⚠️ No data found for {start_date} → {end_date}. Response keys: {list(data.keys())}")
            return pd.DataFrame(columns=["Date", "Value"])

    except Exception as e:
        log(f"❌ Error fetching timeseries: {e}")
        return pd.DataFrame(columns=["Date", "Value"])


def main():
    log("🚀 Starting LBMA Gold PM USD ETL process...")

    if not API_KEY:
        raise ValueError("❌ METALS_DEV_API_KEY environment variable not found. Set it before running.")
    else:
        log("🔑 METALS_DEV_API_KEY loaded successfully (length: %d chars)" % len(API_KEY))

    df_existing = load_existing_csv()

    # --- Determine last valid date ---
    if not df_existing.empty:
        valid_dates = df_existing["Date"].dropna()
        if valid_dates.empty:
            last_date = date.today() - timedelta(days=1)
            log("⚠️ All existing dates are NaT. Using yesterday as last_date.")
        else:
            last_date = valid_dates.max().date()
    else:
        last_date = date.today() - timedelta(days=1)

    existing_effective_dates = set(df_existing["Date"].dropna().dt.date) if not df_existing.empty else set()
    today = date.today()

    missing_dates = generate_missing_dates(last_date, today, existing_effective_dates)
    if not missing_dates:
        log("ℹ️ CSV is already up-to-date. No API calls needed.")
        return

    log(f"📅 Total effective PM dates missing: {len(missing_dates)}")

    # --- Batch missing dates in 30-day windows for timeseries API ---
    df_new_list = []
    batch_start_idx = 0
    while batch_start_idx < len(missing_dates):
        batch_end_idx = min(batch_start_idx + MAX_DAYS_PER_CALL - 1, len(missing_dates) - 1)
        start_date = missing_dates[batch_start_idx]
        end_date = missing_dates[batch_end_idx]
        log(f"📌 Fetching timeseries {start_date} → {end_date}")
        df_batch = fetch_timeseries(start_date, end_date)
        df_new_list.append(df_batch)
        batch_start_idx = batch_end_idx + 1

    df_new = pd.concat(df_new_list).drop_duplicates(subset="Date").sort_values("Date")

    if df_new.empty:
        log("ℹ️ No new data fetched. CSV remains unchanged.")
        return

    # --- Merge, clean, and save ---
    df_updated = pd.concat([df_existing, df_new]).drop_duplicates(subset="Date").sort_values("Date")
    df_updated["Value"] = df_updated["Value"].round(2)
    df_updated["Date"] = df_updated["Date"].dt.strftime("%d/%m/%Y")
    df_updated.to_csv(CSV_PATH, index=False)

    log(f"💾 CSV updated successfully. Added {len(df_new)} new rows. Total rows: {len(df_updated)}")
    log(f"✅ Last date updated: {df_updated['Date'].iloc[-1]}")
    log("🎉 LBMA Gold PM USD ETL completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"🔥 Fatal error during ETL: {e}")
        raise
        

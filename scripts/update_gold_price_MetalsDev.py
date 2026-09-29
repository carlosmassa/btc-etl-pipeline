from datetime import datetime, date, timedelta, timezone
from pathlib import Path
import os

import pandas as pd
import requests

CSV_PATH = Path("data/LBMA-gold_D-gold_D_USD_PM.csv")
GITHUB_RAW_CSV = "https://raw.githubusercontent.com/carlosmassa/btc-etl-pipeline/main/data/LBMA-gold_D-gold_D_USD_PM.csv"
SYMBOL = "lbma_gold_pm"
MAX_DAYS_PER_CALL = 30
API_KEY = os.getenv("METALS_DEV_API_KEY")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log(msg: str) -> None:
    print(f"[{utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)


def persist_csv(df: pd.DataFrame) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    out["Value"] = pd.to_numeric(out["Value"], errors="coerce").round(2)
    out = out.dropna(subset=["Date", "Value"]).drop_duplicates(subset="Date", keep="last").sort_values("Date")
    out.to_csv(CSV_PATH, index=False)


def load_existing_csv() -> pd.DataFrame:
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH)
        log(f"Loaded local gold CSV with {len(df)} rows.")
    else:
        log("Gold CSV not found locally. Trying GitHub raw URL...")
        try:
            df = pd.read_csv(GITHUB_RAW_CSV)
            log(f"Loaded gold CSV from GitHub with {len(df)} rows.")
        except Exception as exc:
            log(f"Could not fetch gold CSV from GitHub: {exc}")
            df = pd.DataFrame(columns=["Date", "Value"])

    if df.empty:
        return pd.DataFrame(columns=["Date", "Value"])

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df.dropna(subset=["Date", "Value"])
    df = df.drop_duplicates(subset="Date", keep="last").sort_values("Date")
    return df.reset_index(drop=True)


def fill_calendar(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    full = pd.DataFrame({"Date": pd.date_range(df["Date"].min(), df["Date"].max(), freq="D")})
    filled = full.merge(df, on="Date", how="left")
    filled["Value"] = filled["Value"].ffill()
    return filled.dropna(subset=["Value"])


def fetch_timeseries(start_date: date, end_date: date) -> pd.DataFrame:
    if not API_KEY:
        raise ValueError("METALS_DEV_API_KEY environment variable not found.")
    resp = requests.get(
        "https://api.metals.dev/v1/timeseries",
        params={
            "api_key": API_KEY,
            "symbols": SYMBOL,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for d_str, day_data in (data.get("rates") or {}).items():
        metals = day_data.get("metals") or {}
        if "gold" in metals:
            rows.append({"Date": pd.to_datetime(d_str), "Value": metals["gold"]})
    if not rows:
        log(f"No gold rates returned for {start_date} → {end_date}.")
    return pd.DataFrame(rows, columns=["Date", "Value"])


def main() -> None:
    log("Starting LBMA Gold PM USD update...")
    existing = fill_calendar(load_existing_csv())
    if existing.empty:
        raise ValueError("Gold CSV is empty; cannot update incrementally.")

    last_date = existing["Date"].max().date()
    today = utc_now().date()
    if last_date >= today:
        persist_csv(existing)
        log("CSV already covers today. Rewrote ISO-8601 file with weekend fill.")
        return

    fetch_end = min(today - timedelta(days=1), last_date + timedelta(days=MAX_DAYS_PER_CALL))
    if fetch_end < last_date + timedelta(days=1):
        persist_csv(existing)
        log("No new past dates to fetch yet. Rewrote ISO-8601 file with weekend fill.")
        return

    log(f"Fetching missing dates: {last_date + timedelta(days=1)} → {fetch_end}")
    new = fetch_timeseries(last_date + timedelta(days=1), fetch_end)
    if new.empty:
        persist_csv(existing)
        log("No new API rows. Rewrote existing gold CSV.")
        return

    updated = fill_calendar(
        pd.concat([existing, new], ignore_index=True)
        .drop_duplicates(subset="Date", keep="last")
        .sort_values("Date")
    )
    persist_csv(updated)
    last = pd.to_datetime(updated["Date"]).dt.strftime("%Y-%m-%d").iloc[-1]
    log(f"Gold CSV updated. Total rows: {len(updated)}. Last date: {last}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"Fatal error during gold ETL: {exc}")
        raise

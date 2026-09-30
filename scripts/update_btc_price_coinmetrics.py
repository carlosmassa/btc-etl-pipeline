from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from coinmetrics.api_client import CoinMetricsClient

CSV_PATH = Path("data/BTC_Prices.csv")
GITHUB_RAW_CSV = "https://raw.githubusercontent.com/carlosmassa/btc-etl-pipeline/main/data/BTC_Prices.csv"
ASSET = "btc"
METRIC = "PriceUSD"
FREQUENCY = "1d"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log(msg: str) -> None:
    print(f"[{utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}", flush=True)


def parse_price_dates(series: pd.Series) -> pd.Series:
    iso = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    if iso.notna().mean() >= 0.5:
        return iso
    dmy = pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")
    if dmy.notna().mean() >= 0.5:
        return dmy
    return pd.to_datetime(series, dayfirst=True, errors="coerce")


def persist_csv(df: pd.DataFrame) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    out.to_csv(CSV_PATH, index=False)


def load_existing_csv() -> pd.DataFrame:
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH)
        log(f"Loaded local CSV with {len(df)} rows.")
    else:
        log("CSV file not found locally. Trying GitHub raw URL...")
        try:
            df = pd.read_csv(GITHUB_RAW_CSV)
            log(f"Loaded CSV from GitHub with {len(df)} rows.")
        except Exception as exc:
            log(f"Could not fetch CSV from GitHub: {exc}")
            df = pd.DataFrame(columns=["Date", "Value"])

    if df.empty:
        return pd.DataFrame(columns=["Date", "Value"])

    df["Date"] = parse_price_dates(df["Date"])
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df.dropna(subset=["Date", "Value"])
    df = df.drop_duplicates(subset="Date", keep="last").sort_values("Date")
    return df.reset_index(drop=True)


def get_btc_data(start_date: str) -> pd.DataFrame:
    client = CoinMetricsClient()
    try:
        metrics = client.get_asset_metrics(
            assets=ASSET,
            metrics=[METRIC],
            frequency=FREQUENCY,
            start_time=start_date,
        )
        raw = pd.DataFrame(metrics)
    except Exception as exc:
        log(f"Error fetching data from CoinMetrics: {exc}")
        return pd.DataFrame(columns=["Date", "Value"])

    if raw.empty:
        return pd.DataFrame(columns=["Date", "Value"])

    raw["Date"] = pd.to_datetime(raw["time"]).dt.tz_localize(None).dt.normalize()
    raw["Value"] = pd.to_numeric(raw[METRIC], errors="coerce")
    raw = raw.dropna(subset=["Date", "Value"])
    return raw[["Date", "Value"]]


def main() -> None:
    log("Starting CoinMetrics BTC price update process...")
    existing = load_existing_csv()

    if existing.empty:
        start_date = "2010-07-17"
        log("No existing data. Starting from 2010-07-17.")
    else:
        last_date = existing["Date"].max()
        log(f"Existing CSV has {len(existing)} rows (last date: {last_date.date()}).")
        start = (last_date + timedelta(days=1)).date()
        if start > utc_now().date():
            persist_csv(existing)
            log(f"Start date {start.isoformat()} is in the future. Rewrote CSV in ISO-8601.")
            return
        start_date = start.isoformat()
        log(f"Fetching data from {start_date} onwards...")

    new = get_btc_data(start_date)
    if new.empty:
        persist_csv(existing)
        log("No new CoinMetrics rows. Rewrote CSV in ISO-8601.")
        return

    log(f"Fetched {len(new)} new rows from CoinMetrics.")
    new["Value"] = new["Value"].round(2)

    updated = (
        pd.concat([existing, new], ignore_index=True)
        .drop_duplicates(subset="Date", keep="last")
        .sort_values("Date")
    )
    persist_csv(updated)
    last = pd.to_datetime(updated["Date"]).dt.strftime("%Y-%m-%d").iloc[-1]
    log(f"CSV updated successfully. Now {len(updated)} total rows. Last date: {last}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"Fatal error during ETL process: {exc}")
        raise

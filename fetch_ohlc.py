"""Fetch daily BTC OHLC from CryptoDataDownload (Bitstamp) and update data/BTC_OHLC.csv."""
import logging
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
URL = "https://www.cryptodatadownload.com/cdd/Bitstamp_BTCUSD_d.csv"
OUT = Path("data/BTC_OHLC.csv")


def fetch() -> pd.DataFrame:
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(pd.io.common.StringIO(r.text), skiprows=1)
    df["Date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"})
    return df[["Date", "Open", "High", "Low", "Close"]].sort_values("Date").drop_duplicates("Date", keep="last")


def main() -> None:
    fresh = fetch()
    if OUT.exists():
        old = pd.read_csv(OUT)
        merged = pd.concat([old, fresh]).drop_duplicates("Date", keep="last").sort_values("Date")
    else:
        merged = fresh
    merged.to_csv(OUT, index=False)
    logging.info("Wrote %s rows (%s → %s)", len(merged), merged["Date"].iloc[0], merged["Date"].iloc[-1])


if __name__ == "__main__":
    main()

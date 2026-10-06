import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CHARTS_DIR = Path("charts")
REQUIRED = [
    "btc_usd_chart.html",
    "btc_usd_chart.jpg",
    "btc_gold_ratio_chart.html",
    "btc_gold_ratio_chart.jpg",
    "btc_usd_stability.html",
    "btc_usd_stability.jpg",
    "btc_gold_ratio_stability.html",
    "btc_gold_ratio_stability.jpg",
    "btc_usd_health.html",
    "btc_usd_health.jpg",
    "btc_gold_ratio_health.html",
    "btc_gold_ratio_health.jpg",
    "btc_usd_nlb.html",
    "btc_usd_nlb.jpg",
    "btc_usd_nlb_regression.html",
    "btc_usd_nlb_regression.jpg",
    "index.html",
]


def load_data() -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    missing = [str(CHARTS_DIR / name) for name in REQUIRED if not (CHARTS_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(f"Expected chart files missing: {', '.join(missing)}")
    logging.info("Chart artifacts ready in %s: %s", CHARTS_DIR, ", ".join(REQUIRED))


if __name__ == "__main__":
    try:
        load_data()
    except Exception as exc:
        logging.error("Load failed: %s", exc)
        raise

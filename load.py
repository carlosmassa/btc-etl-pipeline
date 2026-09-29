import logging
import shutil
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CHARTS_DIR = Path("charts")
HTML_NAME = "btc_usd_chart.html"
JPG_NAME = "btc_usd_chart.jpg"


def load_data() -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    dest_html = CHARTS_DIR / HTML_NAME
    dest_jpg = CHARTS_DIR / JPG_NAME

    root_html = Path(HTML_NAME)
    if root_html.exists() and root_html.resolve() != dest_html.resolve():
        shutil.copy2(root_html, dest_html)
        logging.info("Copied %s → %s", root_html, dest_html)

    missing = [str(p) for p in (dest_html, dest_jpg) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Expected chart files missing: {', '.join(missing)}")

    logging.info("Chart artifacts ready: %s, %s", dest_html, dest_jpg)


if __name__ == "__main__":
    try:
        load_data()
    except Exception as exc:
        logging.error("Load failed: %s", exc)
        raise

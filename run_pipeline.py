import logging

import transform as t
from stability import compute_power_law_stability, render_stability_chart

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def write_full_index() -> None:
    t.CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    index_path = t.CHARTS_DIR / "index.html"
    index_path.write_text(
        Path_INDEX,
        encoding="utf-8",
    )

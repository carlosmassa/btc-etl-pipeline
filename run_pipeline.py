import logging
from pathlib import Path

import transform as t
from model_health import compute_model_health, render_health_chart
from nlb import compute_nlb, render_nlb_basic, render_nlb_regression
from stability import compute_power_law_stability, render_stability_chart

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def write_full_index() -> None:
    t.CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html = Path("index_template.html").read_text(encoding="utf-8")
    index_path = t.CHARTS_DIR / "index.html"
    index_path.write_text(html, encoding="utf-8")
    logging.info("Wrote %s", index_path)


def main() -> None:
    btc = t.clean_btc()
    t.render_chart(
        t.fit_power_law(btc, series_label="BTC/USD"),
        title="BTCUSD Power Law Probability Channel",
        yaxis_title="Price (USD)",
        series_name="Price",
        html_name="btc_usd_chart.html",
        jpg_name="btc_usd_chart.jpg",
        value_style="usd",
        fair_value_prefix="$",
        line_color="orange",
        show_forecast_table=True,
    )
    render_stability_chart(
        compute_power_law_stability(btc),
        title="BTCUSD Power Law Stabilization",
        html_name="btc_usd_stability.html",
        jpg_name="btc_usd_stability.jpg",
        series_label="BTCUSD price",
        show_extrema_table=True,
    )

    render_health_chart(
        compute_model_health(btc),
        title="BTCUSD Power Law Health",
        html_name="btc_usd_health.html",
        jpg_name="btc_usd_health.jpg",
        series_label="BTCUSD",
    )

    ratio = t.clean_btc_gold_ratio()
    t.render_chart(
        t.fit_power_law(ratio, series_label="BTC/Gold"),
        title="BTC/GOLD Power Law Probability Channel",
        yaxis_title="Ounces of gold per BTC",
        series_name="BTC/Gold",
        html_name="btc_gold_ratio_chart.html",
        jpg_name="btc_gold_ratio_chart.jpg",
        value_style="ratio",
        fair_value_prefix="",
        line_color="#FFD700",
        show_forecast_table=True,
    )
    render_stability_chart(
        compute_power_law_stability(ratio),
        title="BTC/GOLD Power Law Stabilization",
        html_name="btc_gold_ratio_stability.html",
        jpg_name="btc_gold_ratio_stability.jpg",
        series_label="BTC/GOLD ratio",
        show_extrema_table=True,
    )

    render_health_chart(
        compute_model_health(ratio),
        title="BTC/GOLD Power Law Health",
        html_name="btc_gold_ratio_health.html",
        jpg_name="btc_gold_ratio_health.jpg",
        series_label="BTC/GOLD",
    )
    nlb = compute_nlb(btc)
    render_nlb_basic(nlb)
    render_nlb_regression(nlb)
    write_full_index()


if __name__ == "__main__":
    main()

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import statsmodels.api as sm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BTC_CSV = Path("data/BTC_Prices.csv")
GOLD_CSV = Path("data/LBMA-gold_D-gold_D_USD_PM.csv")
GENESIS = pd.Timestamp("2009-01-03")
USEFUL_FROM = pd.Timestamp("2010-07-18")
CHART_QUANTILES = [0.001, 0.05, 0.50, 0.90, 0.98, 0.999]
QUANTILE_LABELS = {
    0.001: "0.1%",
    0.05: "5%",
    0.50: "50%",
    0.90: "90%",
    0.98: "98%",
    0.999: "99.9%",
}
FUTURE_DAYS = 5 * 365
CHARTS_DIR = Path("charts")
FORECAST_HORIZONS = [
    ("Today", 0),
    ("3 mo", 91),
    ("6 mo", 182),
    ("1 yr", 365),
    ("2 yr", 730),
    ("3 yr", 1095),
    ("4 yr", 1461),
    ("5 yr", 1825),
]
TABLE_ROW_COLORS = {
    "99.9%": "#B33A12",
    "98%": "#3E6F8C",
    "90%": "#0B6E94",
    "50%": "#0C6C78",
    "5%": "#1A7A45",
    "0.1%": "#5A7A10",
}
TABLE_HEADER_COLOR = "#2A2A2A"
TABLE_LABEL_COLOR = "#1C1C1C"


def parse_price_dates(series: pd.Series) -> pd.Series:
    iso = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    if iso.notna().mean() >= 0.5:
        return iso
    dmy = pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")
    if dmy.notna().mean() >= 0.5:
        return dmy
    return pd.to_datetime(series, dayfirst=True, errors="coerce")


def _coef(fit) -> tuple[float, float]:
    params = fit.params
    if hasattr(params, "iloc"):
        return float(params.iloc[0]), float(params.iloc[1])
    return float(params[0]), float(params[1])


def _load_price_csv(csv_path: Path, label: str) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"{label} file not found: {csv_path}")
    df = pd.read_csv(csv_path)
    if "Date" not in df.columns or "Value" not in df.columns:
        raise ValueError(f"{csv_path} must have Date and Value columns")
    df["Date"] = parse_price_dates(df["Date"])
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df.dropna(subset=["Date", "Value"])
    df = df[df["Value"] > 0]
    df = df.drop_duplicates(subset="Date", keep="last").sort_values("Date")
    return df.reset_index(drop=True)


def _finalize_series(df: pd.DataFrame, label: str) -> pd.DataFrame:
    df = df[df["Date"] >= USEFUL_FROM].copy()
    df["ind"] = (df["Date"] - GENESIS).dt.days
    df = df[df["ind"] > 0].sort_values("Date").reset_index(drop=True)
    if df.empty:
        raise ValueError(f"No usable {label} rows after cleaning")
    logging.info("Cleaned %s %s rows (%s → %s)", len(df), label, df["Date"].min().date(), df["Date"].max().date())
    return df


def clean_btc(csv_path: Path = BTC_CSV) -> pd.DataFrame:
    return _finalize_series(_load_price_csv(csv_path, "BTC"), "BTC")


def clean_gold(csv_path: Path = GOLD_CSV) -> pd.DataFrame:
    gold = _load_price_csv(csv_path, "gold")
    full = pd.DataFrame({"Date": pd.date_range(gold["Date"].min(), gold["Date"].max(), freq="D")})
    gold = full.merge(gold, on="Date", how="left")
    gold["Value"] = gold["Value"].ffill()
    gold = gold.dropna(subset=["Value"])
    return _finalize_series(gold, "gold")


def clean_btc_gold_ratio() -> pd.DataFrame:
    btc = _load_price_csv(BTC_CSV, "BTC")
    gold = clean_gold()[["Date", "Value"]].rename(columns={"Value": "Gold"})
    merged = btc.merge(gold, on="Date", how="left").sort_values("Date")
    merged["Gold"] = merged["Gold"].ffill()
    merged = merged.dropna(subset=["Value", "Gold"])
    merged = merged[merged["Gold"] > 0]
    merged["Value"] = merged["Value"] / merged["Gold"]
    ratio = merged[["Date", "Value"]].copy()
    return _finalize_series(ratio, "BTC/Gold ratio")


def fit_power_law(df: pd.DataFrame, series_label: str = "series") -> dict:
    X = np.log(df["ind"].astype(float))
    y = np.log(df["Value"].astype(float))
    X_with_const = sm.add_constant(X)
    fits = {}
    for q in CHART_QUANTILES:
        fits[q] = sm.QuantReg(y, X_with_const).fit(q=q)
        intercept, slope = _coef(fits[q])
        logging.info("%s quantile %s: intercept=%.4f slope=%.4f prsquared=%.3f", series_label, QUANTILE_LABELS[q], intercept, slope, fits[q].prsquared)
    work = df.copy()
    for q, label in QUANTILE_LABELS.items():
        work[f"QuantRegPredict_{label}"] = fits[q].predict(X_with_const)
        work[f"LinearReg_{label}"] = np.exp(work[f"QuantRegPredict_{label}"])
    future_dates = pd.date_range(work["Date"].max() + pd.Timedelta(days=1), periods=FUTURE_DAYS, freq="D")
    future = pd.DataFrame({"Date": future_dates})
    future["ind"] = (future["Date"] - GENESIS).dt.days
    X_future = sm.add_constant(np.log(future["ind"].astype(float)))
    for q, label in QUANTILE_LABELS.items():
        future[f"QuantRegPredict_{label}"] = fits[q].predict(X_future)
        future[f"LinearReg_{label}"] = np.exp(future[f"QuantRegPredict_{label}"])
    combined = pd.concat([work, future], ignore_index=True)
    latest = work.iloc[-1]
    latest_price = float(latest["Value"])
    fair_value = float(latest["LinearReg_50%"])
    percent_change = ((latest_price - fair_value) / fair_value) * 100
    price_quantile = estimate_price_quantile(y, X_with_const, latest_price)
    intercept, slope = _coef(fits[0.50])
    logging.info("%s 50%% equation: Value = exp(%.6f * ln(days since genesis) + %.6f)", series_label, slope, intercept)
    logging.info("%s latest %.4f is %.2f%% %s 50%% fair value (q≈%s)", series_label, latest_price, abs(percent_change), "above" if percent_change >= 0 else "below", f"{price_quantile * 100:.2f}%")
    return {"df": work, "combined": combined, "fits": fits, "latest_date": pd.Timestamp(latest["Date"]), "latest_price": latest_price, "fair_value": fair_value, "percent_change": percent_change, "price_quantile": price_quantile, "band": {"q001": float(latest["LinearReg_0.1%"]), "q05": float(latest["LinearReg_5%"]), "q50": fair_value, "q90": float(latest["LinearReg_90%"]), "q98": float(latest["LinearReg_98%"]), "q999": float(latest["LinearReg_99.9%"])}}


def estimate_price_quantile(y, X_with_const, latest_price, lo=0.001, hi=0.999, max_iter=16):
    target = np.log(latest_price)
    x_last = X_with_const.iloc[[-1]] if hasattr(X_with_const, "iloc") else X_with_const[-1:]
    def pred_at(q):
        pred = sm.QuantReg(y, X_with_const).fit(q=q, max_iter=5000).predict(x_last)
        return float(pred.iloc[0] if hasattr(pred, "iloc") else pred[0])
    low_pred, high_pred = pred_at(lo), pred_at(hi)
    if target <= low_pred:
        return lo
    if target >= high_pred:
        return hi
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        if pred_at(mid) < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _predict_price(fit, days_since_genesis):
    x = sm.add_constant(np.log(np.array([float(days_since_genesis)])), has_constant="add")
    pred = fit.predict(x)
    value = float(pred.iloc[0] if hasattr(pred, "iloc") else pred[0])
    return float(np.exp(value))


def _table_cell(value, value_style):
    if value_style == "usd":
        return f"{value / 1000:,.0f}"
    return f"{value:,.2f}"


def forecast_table_values(model, value_style="usd", unit="Projection"):
    latest_date = model["latest_date"]
    fits = model["fits"]
    header = [unit] + [name for name, _ in FORECAST_HORIZONS]
    row_labels, row_prices, row_colors = [], [], []
    for q in reversed(CHART_QUANTILES):
        label = QUANTILE_LABELS[q]
        prices = []
        for _, offset in FORECAST_HORIZONS:
            target = latest_date + pd.Timedelta(days=offset)
            prices.append(_predict_price(fits[q], (target - GENESIS).days))
        row_labels.append(label)
        row_prices.append(prices)
        row_colors.append(TABLE_ROW_COLORS[label])
    columns = [row_labels]
    fills = [[TABLE_LABEL_COLOR] * len(row_labels)]
    for col_idx in range(len(FORECAST_HORIZONS)):
        columns.append([_table_cell(row_prices[r][col_idx], value_style) for r in range(len(row_labels))])
        fills.append(row_colors)
    return header, columns, fills


def add_forecast_table(fig, model, value_style="usd"):
    header, columns, fills = forecast_table_values(model, value_style=value_style)
    fig.add_trace(go.Table(
        header=dict(values=header, fill_color=TABLE_HEADER_COLOR, font=dict(family="Arial", color="white", size=14), align="center", line=dict(color="#111111", width=1), height=30),
        cells=dict(values=columns, fill_color=fills, font=dict(family="Arial", color="white", size=14), align="center", line=dict(color="#111111", width=1), height=28),
        domain=dict(x=[0.50, 0.995], y=[0.02, 0.44]),
        name="Forecast table",
        visible=True,
    ))


def inject_html_table_controls(html_path, model, value_style):
    header, columns, fills = forecast_table_values(model, value_style=value_style)
    n_rows = len(columns[0])
    thead = "<tr>" + "".join(f"<th>{h}</th>" for h in header) + "</tr>"
    body_rows = []
    for r in range(n_rows):
        cells = []
        for c in range(len(header)):
            bg = TABLE_LABEL_COLOR if c == 0 else fills[1][r]
            cells.append(f'<td style="background:{bg}">{columns[c][r]}</td>')
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    table_markup = f'<table id="forecast-table"><thead>{thead}</thead><tbody>{chr(10).join(body_rows)}</tbody></table>'
    snippet = f"""
<style>
  .plotly-graph-div {{ position: relative; }}
  #forecast-table-toggle {{
    position: absolute; right: 12px; bottom: 8px; top: auto; z-index: 30;
    background: #2A2A2A; color: #fff; border: 1px solid #555;
    padding: 6px 10px; font: 12px Arial, sans-serif; cursor: pointer;
  }}
  #forecast-table {{
    position: absolute; right: 12px; bottom: 44px; z-index: 20;
    border-collapse: collapse; font: 14px Arial, sans-serif; color: #fff;
  }}
  #forecast-table th, #forecast-table td {{
    padding: 5px 10px; text-align: center; border: 1px solid #111;
  }}
  #forecast-table th {{ background: {TABLE_HEADER_COLOR}; font-weight: normal; }}
</style>
<script>
document.addEventListener("DOMContentLoaded", function () {{
  const gd = document.querySelector(".plotly-graph-div");
  if (!gd) return;
  const box = gd.querySelector(".plot-container") || gd;
  box.style.position = "relative";
  const btn = document.createElement("button");
  btn.id = "forecast-table-toggle";
  btn.textContent = "Hide table";
  const holder = document.createElement("div");
  holder.innerHTML = `{table_markup}`;
  box.appendChild(btn);
  box.appendChild(holder.firstElementChild);
  btn.addEventListener("click", function () {{
    const tbl = document.getElementById("forecast-table");
    const hidden = tbl.style.display === "none";
    tbl.style.display = hidden ? "table" : "none";
    btn.textContent = hidden ? "Hide table" : "Show table";
  }});
}});
</script>
"""
    text = html_path.read_text(encoding="utf-8")
    html_path.write_text(text.replace("</body>", snippet + "\n</body>", 1) if "</body>" in text else text + snippet, encoding="utf-8")


def _fmt(value, style):
    return f"{value:,.0f}" if style == "usd" else f"{value:,.2f}"


def render_chart(model, *, title, yaxis_title, series_name, html_name, jpg_name, value_style="usd", fair_value_prefix="$", line_color="orange", show_forecast_table=False):
    df, combined = model["df"], model["combined"]
    latest_date, latest_price, percent_change, band, price_quantile = model["latest_date"], model["latest_price"], model["percent_change"], model["band"], model["price_quantile"]
    change_type = "above" if percent_change >= 0 else "below"
    n_points = f"{len(df):,.0f}"
    date_label = latest_date.strftime("%d %B %Y")
    q_label = f"{price_quantile * 100:.2f}%"
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Value"], mode="lines", name=series_name, line=dict(color=line_color, width=3)))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_50%"], mode="lines", name="50% Quantile", line=dict(color="cyan", width=2)))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_0.1%"], fill=None, mode="lines", line=dict(color="#A8D800", width=1), name="0.1% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_5%"], fill="tonexty", mode="lines", line=dict(color="#00FF7F", width=0), name="5% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_90%"], fill=None, mode="lines", line=dict(color="#00BFFF", width=1), name="90% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_98%"], fill="tonexty", mode="lines", line=dict(color="#87CEFA", width=1), name="98% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_99.9%"], fill="tonexty", mode="lines", line=dict(color="#FF4500", width=0), name="99.9% Quantile", showlegend=False))
    annotations = [
        dict(xref="paper", yref="paper", x=0.40, y=-0.05, xanchor="center", yanchor="top", text=(f"Chart Date: {date_label} ({n_points} Data Points)<br>Latest: {_fmt(latest_price, value_style)} ({abs(percent_change):,.2f}% {change_type} Fair Value); {q_label} Quantile<br>Fair Value: {fair_value_prefix}{_fmt(band['q50'], value_style)} (50% Quantile)"), font=dict(family="Arial", size=12, color="rgb(150,150,150)"), align="left", showarrow=False),
        dict(xref="paper", yref="paper", x=0.65, y=-0.05, xanchor="center", yanchor="top", text=(f"98% to 99.9% Quantile ({_fmt(band['q98'], value_style)} - {_fmt(band['q999'], value_style)})<br>90% to 98% Quantile ({_fmt(band['q90'], value_style)} - {_fmt(band['q98'], value_style)})<br>0.1% to 5% Quantile ({_fmt(band['q001'], value_style)} - {_fmt(band['q05'], value_style)})"), font=dict(family="Arial", size=12, color="rgb(150,150,150)"), align="left", showarrow=False),
        dict(x=1, y=-0.12, text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>', showarrow=False, xref="paper", yref="paper", xanchor="right", yanchor="auto", xshift=0, yshift=0),
    ]
    fig.update_layout(title=title, xaxis_title="Date", yaxis_title=yaxis_title, yaxis_type="log", hovermode="closest", annotations=annotations, showlegend=True, legend_orientation="h", template="plotly_dark")
    fig.update_yaxes(showgrid=False)
    config = {"modeBarButtonsToAdd": ["drawline", "drawopenpath", "drawcircle", "drawrect", "eraseshape", "v1hovermode", "togglespikelines"]}
    if show_forecast_table:
        add_forecast_table(fig, model, value_style=value_style)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = CHARTS_DIR / html_name
    jpg_path = CHARTS_DIR / jpg_name
    fig.write_image(str(jpg_path), width=1600, height=900, scale=2)
    if show_forecast_table:
        fig.data = tuple(tr for tr in fig.data if getattr(tr, "type", None) != "table")
    fig.write_html(str(html_path), auto_open=False, config=config)
    if show_forecast_table:
        inject_html_table_controls(html_path, model, value_style)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path


def write_chart_index():
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    index_path = CHARTS_DIR / "index.html"
    index_path.write_text("""<!doctype html>
<html lang=\"en\">
<head><meta charset=\"utf-8\"><title>BTC Power Law Charts</title>
<style>body{font-family:Arial,sans-serif;background:#111;color:#eee;max-width:720px;margin:3rem auto;padding:0 1rem}a{color:#7dd3fc}li{margin:.6rem 0}</style>
</head><body>
<h1>Power Law Probability Channels</h1>
<ul><li><a href=\"btc_usd_chart.html\">BTC/USD</a></li><li><a href=\"btc_gold_ratio_chart.html\">BTCUSD / GOLD</a></li></ul>
</body></html>
""", encoding="utf-8")
    logging.info("Wrote %s", index_path)
    return index_path


def transform_data():
    btc_model = fit_power_law(clean_btc(), series_label="BTC/USD")
    render_chart(btc_model, title="Power Law Probability Channel", yaxis_title="Price (USD)", series_name="Price", html_name="btc_usd_chart.html", jpg_name="btc_usd_chart.jpg", value_style="usd", fair_value_prefix="$", line_color="orange", show_forecast_table=True)
    ratio_model = fit_power_law(clean_btc_gold_ratio(), series_label="BTC/Gold")
    render_chart(ratio_model, title="BTCUSD / GOLD Power Law Probability Channel", yaxis_title="Ounces of gold per BTC", series_name="BTC/Gold", html_name="btc_gold_ratio_chart.html", jpg_name="btc_gold_ratio_chart.jpg", value_style="ratio", fair_value_prefix="", line_color="#FFD700", show_forecast_table=True)
    write_chart_index()


if __name__ == "__main__":
    try:
        transform_data()
    except Exception as exc:
        logging.error("Transformation failed: %s", exc)
        raise

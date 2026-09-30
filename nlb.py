import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

CHARTS_DIR = Path("charts")
NLB_COLOR = "#7dd3fc"
PRICE_COLOR = "orange"
FIT_COLOR = "#f5a24a"
TABLE_HEADER_COLOR = "#2A2A2A"
TABLE_LABEL_COLOR = "#1C1C1C"
ROW_COLORS = {"+2σ": "#B33A12", "Fit": "#0C6C78", "-2σ": "#1A7A45"}
HORIZONS = [("Today", 0), ("3 mo", 91), ("6 mo", 182), ("1 yr", 365), ("2 yr", 730), ("3 yr", 1095), ("4 yr", 1461), ("5 yr", 1825)]
FUTURE_DAYS = 5 * 365
HTML_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "responsive": True,
    "scrollZoom": True,
    "modeBarButtonsToAdd": ["drawline", "drawopenpath", "drawclosedpath", "drawcircle", "drawrect", "eraseshape", "v1hovermode", "togglespikelines"],
}


def compute_nlb(df):
    work = df.dropna(subset=["Date", "Value"]).sort_values("Date").reset_index(drop=True)
    work["nlb"] = work["Value"][::-1].cummin()[::-1].astype(float)
    first = work["Date"].iloc[0]
    work["days"] = (work["Date"] - first).dt.days.astype(float).clip(lower=1)
    work["sqrt_days"] = np.sqrt(work["days"])
    logging.info("NLB series %s points (%s → %s); latest NLB=$%.0f", len(work), work["Date"].min().date(), work["Date"].max().date(), float(work["nlb"].iloc[-1]))
    return work


def _fit_nlb(nlb):
    x = nlb["sqrt_days"].to_numpy(dtype=float)
    y = np.log(nlb["nlb"].astype(float).to_numpy())
    design = np.column_stack((np.ones(len(x)), x))
    coef, *_ = np.linalg.lstsq(design, y, rcond=None)
    intercept, slope = float(coef[0]), float(coef[1])
    pred = design @ coef
    rmse = float(np.sqrt(np.mean((y - pred) ** 2)))
    return {"intercept": intercept, "slope": slope, "rmse": rmse, "first": pd.Timestamp(nlb["Date"].iloc[0]), "last": pd.Timestamp(nlb["Date"].iloc[-1]), "nlb_now": float(nlb["nlb"].iloc[-1])}


def _predict(model, date):
    days = max((pd.Timestamp(date) - model["first"]).days, 1)
    logp = model["intercept"] + model["slope"] * np.sqrt(days)
    fit = float(np.exp(logp))
    return float(np.exp(logp + 2 * model["rmse"])), fit, float(np.exp(logp - 2 * model["rmse"]))


def _cell(value):
    return f"${value / 1000:,.0f}k"


def _credit():
    return dict(x=1, y=-0.12, xref="paper", yref="paper", xanchor="right", yanchor="auto", text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>', showarrow=False)


def _write(fig, html_name, jpg_name, width=1600, height=900):
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = CHARTS_DIR / html_name
    jpg_path = CHARTS_DIR / jpg_name
    try:
        fig.write_image(str(jpg_path), width=width, height=height, scale=2)
    except Exception as exc:
        logging.warning("Could not write %s (%s)", jpg_path, exc)
    fig.write_html(str(html_path), auto_open=False, config=HTML_CONFIG)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path


def forecast_table_values(model):
    header = ["Projection"] + [name for name, _ in HORIZONS]
    labels = ["+2σ", "Fit", "-2σ"]
    rows = []
    for label in labels:
        prices = []
        for _, offset in HORIZONS:
            up, fit, lo = _predict(model, model["last"] + pd.Timedelta(days=offset))
            prices.append(_cell({"+2σ": up, "Fit": fit, "-2σ": lo}[label]))
        rows.append((label, prices, ROW_COLORS[label]))
    columns = [[label for label, _, _ in rows]]
    fills = [[TABLE_LABEL_COLOR] * len(rows)]
    for col_idx in range(len(HORIZONS)):
        columns.append([rows[r][1][col_idx] for r in range(len(rows))])
        fills.append([rows[r][2] for r in range(len(rows))])
    return header, columns, fills


def add_forecast_table(fig, model):
    header, columns, fills = forecast_table_values(model)
    fig.add_trace(go.Table(header=dict(values=header, fill_color=TABLE_HEADER_COLOR, font=dict(family="Arial", color="white", size=11), align="center", line=dict(color="#111111", width=1), height=24), cells=dict(values=columns, fill_color=fills, font=dict(family="Arial", color="white", size=11), align="center", line=dict(color="#111111", width=1), height=22), domain=dict(x=[0.42, 0.995], y=[0.02, 0.28]), name="Forecast table"))


def inject_html_table_controls(html_path, model):
    header, columns, fills = forecast_table_values(model)
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
  #forecast-table {{ position: absolute; right: 3%; bottom: 16%; z-index: 20; border-collapse: collapse; font: 14px Arial, sans-serif; color: #fff; }}
  #forecast-table th, #forecast-table td {{ padding: 5px 10px; text-align: center; border: 1px solid #111; }}
  #forecast-table th {{ background: {TABLE_HEADER_COLOR}; font-weight: normal; }}
  #chart-footer {{ position: absolute; right: 12px; bottom: 9%; z-index: 30; display: flex; flex-direction: column; align-items: flex-end; gap: 6px; }}
  #forecast-table-toggle {{ background: #2A2A2A; color: #fff; border: 1px solid #555; padding: 6px 10px; font: 12px Arial, sans-serif; cursor: pointer; }}
</style>
<script>
document.addEventListener("DOMContentLoaded", function () {{
  const gd = document.querySelector(".plotly-graph-div");
  if (!gd) return;
  const box = gd.querySelector(".plot-container") || gd;
  box.style.position = "relative";
  box.insertAdjacentHTML("beforeend", `{table_markup}`);
  const footer = document.createElement("div");
  footer.id = "chart-footer";
  footer.innerHTML = '<button id="forecast-table-toggle">Hide table</button>';
  box.appendChild(footer);
  document.getElementById("forecast-table-toggle").addEventListener("click", function () {{
    const tbl = document.getElementById("forecast-table");
    const hidden = tbl.style.display === "none";
    tbl.style.display = hidden ? "table" : "none";
    this.textContent = hidden ? "Hide table" : "Show table";
  }});
}});
</script>
"""
    text = html_path.read_text(encoding="utf-8")
    html_path.write_text(text.replace("</body>", snippet + "\n</body>", 1) if "</body>" in text else text + snippet, encoding="utf-8")


def render_nlb_basic(nlb):
    model = _fit_nlb(nlb)
    latest = nlb.iloc[-1]
    date_label = pd.Timestamp(latest["Date"]).strftime("%d %B %Y")
    future_dates = pd.date_range(model["last"], model["last"] + pd.Timedelta(days=FUTURE_DAYS), freq="D")
    hist_up, hist_fit, hist_lo = zip(*[_predict(model, d) for d in nlb["Date"]])
    fut_up, fut_fit, fut_lo = zip(*[_predict(model, d) for d in future_dates])
    band_x = list(nlb["Date"]) + list(future_dates)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=band_x, y=list(hist_up) + list(fut_up), mode="lines", name="+2σ", line=dict(color="rgba(245,162,74,0.55)", width=1.5), hovertemplate="Date=%{x|%d %b %Y}<br>+2σ=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=band_x, y=list(hist_lo) + list(fut_lo), mode="lines", name="-2σ", line=dict(color="rgba(245,162,74,0.55)", width=1.5), fill="tonexty", fillcolor="rgba(245,162,74,0.10)", hovertemplate="Date=%{x|%d %b %Y}<br>-2σ=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=band_x, y=list(hist_fit) + list(fut_fit), mode="lines", name="Fit", line=dict(color=FIT_COLOR, width=2), hovertemplate="Date=%{x|%d %b %Y}<br>Fit=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=nlb["Date"], y=nlb["Value"], mode="lines", name="Price", line=dict(color=PRICE_COLOR, width=1.6), hovertemplate="Date=%{x|%d %b %Y}<br>Price=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=nlb["Date"], y=nlb["nlb"], mode="lines", name="NLB floor", line=dict(color=NLB_COLOR, width=2.4), hovertemplate="Date=%{x|%d %b %Y}<br>NLB=$%{y:,.0f}<extra></extra>"))
    fig.update_yaxes(title_text="Price (USD)", type="log", showgrid=True, gridcolor="#333")
    fig.update_xaxes(title_text="Date", showgrid=True, gridcolor="#333")
    fig.update_layout(title=dict(text=f"BTCUSD Never Look Back Price<br><sup>{date_label}  ·  Price ${float(latest['Value']):,.0f}  ·  NLB ${float(latest['nlb']):,.0f}</sup>", x=0.5, xanchor="center"), template="plotly_dark", legend=dict(orientation="h", y=-0.16, x=0), hovermode="closest", margin=dict(t=80, b=110, l=70, r=40), annotations=[_credit()])
    add_forecast_table(fig, model)
    html_path, jpg_path = _write(fig, "btc_usd_nlb.html", "btc_usd_nlb.jpg")
    fig.data = tuple(tr for tr in fig.data if getattr(tr, "type", None) != "table")
    fig.write_html(str(html_path), auto_open=False, config=HTML_CONFIG)
    inject_html_table_controls(html_path, model)
    return html_path, jpg_path


def render_nlb_regression(nlb):
    model = _fit_nlb(nlb)
    future_dates = pd.date_range(model["last"], model["last"] + pd.Timedelta(days=FUTURE_DAYS), freq="D")
    future_days = np.maximum((future_dates - model["first"]).days.astype(float), 1.0)
    future_sqrt = np.sqrt(future_days)
    log_fit = model["intercept"] + model["slope"] * future_sqrt
    fit = np.exp(log_fit)
    upper = np.exp(log_fit + 2 * model["rmse"])
    lower = np.exp(log_fit - 2 * model["rmse"])
    hist_fit = np.exp(model["intercept"] + model["slope"] * nlb["sqrt_days"].to_numpy())
    hist_dates = nlb["Date"].dt.strftime("%d %b %Y").to_numpy()
    all_dates = np.concatenate([hist_dates, pd.DatetimeIndex(future_dates).strftime("%d %b %Y").to_numpy()])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=np.concatenate([nlb["sqrt_days"], future_sqrt]), y=np.concatenate([np.exp(np.log(hist_fit) + 2 * model["rmse"]), upper]), mode="lines", name="+2σ", line=dict(color="rgba(245,162,74,0.45)", width=1.4), customdata=all_dates, hovertemplate="Date=%{customdata}<br>+2σ=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=np.concatenate([nlb["sqrt_days"], future_sqrt]), y=np.concatenate([np.exp(np.log(hist_fit) - 2 * model["rmse"]), lower]), mode="lines", name="-2σ", line=dict(color="rgba(245,162,74,0.45)", width=1.4), fill="tonexty", fillcolor="rgba(245,162,74,0.10)", customdata=all_dates, hovertemplate="Date=%{customdata}<br>-2σ=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=np.concatenate([nlb["sqrt_days"], future_sqrt]), y=np.concatenate([hist_fit, fit]), mode="lines", name="Fit", line=dict(color=FIT_COLOR, width=2), customdata=all_dates, hovertemplate="Date=%{customdata}<br>Fit=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=nlb["sqrt_days"], y=nlb["nlb"], mode="lines", name="NLB floor", line=dict(color=NLB_COLOR, width=2.4), customdata=hist_dates, hovertemplate="Date=%{customdata}<br>NLB=$%{y:,.0f}<extra></extra>"))
    year_ticks, year_labels = [], []
    for year in range(model["first"].year, model["last"].year + 6):
        mark = max(pd.Timestamp(year=year, month=1, day=1), model["first"])
        if year % 2 == 1 or year >= model["last"].year:
            year_ticks.append(float(np.sqrt(max((mark - model["first"]).days, 1))))
            year_labels.append(f"'{str(year)[2:]}")
    fig.update_yaxes(title_text="NLB price (USD)", type="log", showgrid=True, gridcolor="#333")
    fig.update_xaxes(title_text="Square-root time (year marks)", tickmode="array", tickvals=year_ticks, ticktext=year_labels, showgrid=True, gridcolor="#333")
    fig.update_layout(title=dict(text=f"BTCUSD Never Look Back Regression<br><sup>{model['last'].strftime('%d %B %Y')}  ·  NLB ${model['nlb_now']:,.0f}  ·  Fit ${_predict(model, model['last'])[1]:,.0f}</sup>", x=0.5, xanchor="center"), template="plotly_dark", legend=dict(orientation="h", y=-0.16, x=0), hovermode="closest", margin=dict(t=80, b=110, l=70, r=40), annotations=[_credit()])
    add_forecast_table(fig, model)
    html_path, jpg_path = _write(fig, "btc_usd_nlb_regression.html", "btc_usd_nlb_regression.jpg")
    fig.data = tuple(tr for tr in fig.data if getattr(tr, "type", None) != "table")
    fig.write_html(str(html_path), auto_open=False, config=HTML_CONFIG)
    inject_html_table_controls(html_path, model)
    return html_path, jpg_path

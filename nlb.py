import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

CHARTS_DIR = Path("charts")
NLB_COLOR = "#7dd3fc"
PRICE_COLOR = "orange"
FIT_COLOR = "#f5a24a"


def compute_nlb(df):
    work = df.dropna(subset=["Date", "Value"]).sort_values("Date").reset_index(drop=True)
    work["nlb"] = work["Value"][::-1].cummin()[::-1].astype(float)
    first = work["Date"].iloc[0]
    work["days"] = (work["Date"] - first).dt.days.astype(float).clip(lower=1)
    work["sqrt_days"] = np.sqrt(work["days"])
    logging.info("NLB series %s points (%s → %s); latest NLB=$%.0f", len(work), work["Date"].min().date(), work["Date"].max().date(), float(work["nlb"].iloc[-1]))
    return work


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
    fig.write_html(str(html_path), auto_open=False)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path


def render_nlb_basic(nlb):
    latest = nlb.iloc[-1]
    date_label = pd.Timestamp(latest["Date"]).strftime("%d %B %Y")
    nlb_now = float(latest["nlb"])
    price_now = float(latest["Value"])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=nlb["Date"], y=nlb["Value"], mode="lines", name="Price", line=dict(color=PRICE_COLOR, width=1.6), hovertemplate="Date=%{x|%Y-%m-%d}<br>Price=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=nlb["Date"], y=nlb["nlb"], mode="lines", name="NLB floor", line=dict(color=NLB_COLOR, width=2.4), hovertemplate="Date=%{x|%Y-%m-%d}<br>NLB=$%{y:,.0f}<extra></extra>"))
    fig.update_yaxes(title_text="Price (USD)", type="log", showgrid=True, gridcolor="#333")
    fig.update_xaxes(title_text="Date", showgrid=True, gridcolor="#333")
    fig.update_layout(title=dict(text=f"BTCUSD Never Look Back Price<br><sup>{date_label}  ·  Price ${price_now:,.0f}  ·  NLB ${nlb_now:,.0f}</sup>", x=0.5, xanchor="center"), template="plotly_dark", legend=dict(orientation="h", y=-0.16, x=0), hovermode="closest", margin=dict(t=80, b=110, l=70, r=40), annotations=[_credit()])
    return _write(fig, "btc_usd_nlb.html", "btc_usd_nlb.jpg")


def render_nlb_regression(nlb):
    x = nlb["sqrt_days"].to_numpy(dtype=float)
    y = np.log(nlb["nlb"].astype(float).to_numpy())
    X = np.column_stack((np.ones(len(x)), x))
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    pred = X @ coef
    rmse = float(np.sqrt(np.mean((y - pred) ** 2)))
    fit = np.exp(pred)
    upper = np.exp(pred + 2 * rmse)
    lower = np.exp(pred - 2 * rmse)
    latest = nlb.iloc[-1]
    date_label = pd.Timestamp(latest["Date"]).strftime("%d %B %Y")
    nlb_now = float(latest["nlb"])
    fit_now = float(fit[-1])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=nlb["sqrt_days"], y=upper, mode="lines", name="+2σ", line=dict(color="rgba(245,162,74,0.35)", width=1), hovertemplate="√days=%{x:.1f}<br>+2σ=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=nlb["sqrt_days"], y=lower, mode="lines", name="-2σ", line=dict(color="rgba(245,162,74,0.35)", width=1), fill="tonexty", fillcolor="rgba(245,162,74,0.08)", hovertemplate="√days=%{x:.1f}<br>-2σ=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=nlb["sqrt_days"], y=fit, mode="lines", name="Regression", line=dict(color=FIT_COLOR, width=2), hovertemplate="√days=%{x:.1f}<br>Fit=$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=nlb["sqrt_days"], y=nlb["nlb"], mode="lines", name="NLB floor", line=dict(color=NLB_COLOR, width=2.4), hovertemplate="√days=%{x:.1f}<br>NLB=$%{y:,.0f}<extra></extra>"))
    year_ticks, year_labels = [], []
    for year in range(int(nlb["Date"].dt.year.min()), int(nlb["Date"].dt.year.max()) + 1):
        row = nlb[nlb["Date"].dt.year == year].iloc[0]
        if year % 2 == 1:
            year_ticks.append(float(row["sqrt_days"]))
            year_labels.append(f"'{str(year)[2:]}")
    fig.update_yaxes(title_text="NLB price (USD)", type="log", showgrid=True, gridcolor="#333")
    fig.update_xaxes(title_text="Square-root time (year marks)", tickmode="array", tickvals=year_ticks, ticktext=year_labels, showgrid=True, gridcolor="#333")
    fig.update_layout(title=dict(text=f"BTCUSD Never Look Back Regression<br><sup>{date_label}  ·  NLB ${nlb_now:,.0f}  ·  Fit ${fit_now:,.0f}</sup>", x=0.5, xanchor="center"), template="plotly_dark", legend=dict(orientation="h", y=-0.16, x=0), hovermode="closest", margin=dict(t=80, b=110, l=70, r=40), annotations=[_credit()])
    return _write(fig, "btc_usd_nlb_regression.html", "btc_usd_nlb_regression.jpg")

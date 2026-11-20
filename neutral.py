"""Bitcoin Neutral Price — pure walk-forward expanding-window median valuation.

At each date the Neutral Price is the fitted value of a log-log power-law
regression on all prior data, shifted so the median residual is zero.
This makes the line the level that price has spent roughly half its history
above and half below, using only information available up to that date.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

CHARTS_DIR = Path("charts")
HTML_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "responsive": True,
    "scrollZoom": True,
    "modeBarButtons": [[
        "toImage", "zoom2d", "pan2d", "select2d", "lasso2d",
        "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d",
        "hoverClosestCartesian", "hoverCompareCartesian", "toggleSpikelines",
        "drawline", "drawopenpath", "drawclosedpath", "drawcircle", "drawrect",
        "eraseshape",
    ]],
}


def _credit():
    return dict(
        x=1, y=-0.08, xref="paper", yref="paper", xanchor="right",
        text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>',
        showarrow=False,
    )


def compute_neutral(df, start="2010-07-18", min_points=365):
    work = df.dropna(subset=["Date", "Value"]).sort_values("Date")
    work = work[work["Value"] > 0]
    close = pd.Series(work["Value"].to_numpy(float), index=pd.to_datetime(work["Date"]))
    close = close[~close.index.duplicated(keep="last")].sort_index()
    close = close[close.index >= pd.Timestamp(start)]
    dates = close.index
    prices = close.to_numpy()
    days = np.maximum((dates - dates[0]).days.to_numpy().astype(float), 1.0)
    log_days = np.log(days)
    log_p = np.log(prices)
    n = len(prices)
    neutral = np.full(n, np.nan)
    for i in range(min_points, n):
        x = log_days[: i + 1]
        y = log_p[: i + 1]
        x_mean = x.mean()
        slope = ((x - x_mean) * (y - y.mean())).sum() / ((x - x_mean) ** 2).sum()
        intercept = y.mean() - slope * x_mean
        fitted = intercept + slope * x
        med_resid = np.median(y - fitted)
        neutral[i] = np.exp(intercept + med_resid + slope * log_days[i])
    out = pd.DataFrame({"close": prices, "neutral": neutral}, index=dates)
    out["ratio"] = out["close"] / out["neutral"]
    return out


def render_neutral(model, html_name="btc_neutral_price.html", jpg_name="btc_neutral_price.jpg"):
    dates = model.index
    close = model["close"]
    neutral = model["neutral"]
    ratio = model["ratio"]
    latest = dates[-1]
    lp = close.iloc[-1]
    ln = neutral.iloc[-1]
    lr = ratio.iloc[-1]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.65, 0.35])

    fig.add_trace(go.Scatter(
        x=dates, y=close, mode="lines", name="BTC close",
        line=dict(color="#e5e7eb", width=1.4),
        hovertemplate="%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=dates, y=neutral, mode="lines", name="Neutral Price",
        line=dict(color="#9ca3af", width=1.2, dash="dot"),
        hovertemplate="%{x|%d %b %Y}<br>Neutral $%{y:,.0f}<extra></extra>",
    ), row=1, col=1)

    above = close.where(close >= neutral)
    below = close.where(close < neutral)
    fig.add_trace(go.Scatter(
        x=dates, y=above, mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(236,72,153,0.30)",
        name="Price above Neutral", hoverinfo="skip",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=dates, y=below, mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(6,182,212,0.30)",
        name="Price below Neutral", hoverinfo="skip",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=dates, y=ratio, mode="lines", name="Price / Neutral",
        line=dict(color="#e5e7eb", width=1.2),
        hovertemplate="%{x|%d %b %Y}<br>×%{y:.2f}<extra></extra>",
    ), row=2, col=1)
    fig.add_hline(y=1, line=dict(color="#6b7280", width=1, dash="dot"), row=2, col=1)

    fig.add_annotation(
        x=latest, y=lp, text=f"${lp:,.0f}", showarrow=False, xanchor="left",
        font=dict(size=10, color="white"), bgcolor="rgba(15,23,42,0.85)",
    )
    fig.add_annotation(
        x=latest, y=lr, text=f"×{lr:.2f}", showarrow=False, xanchor="left",
        font=dict(size=10, color="white"), bgcolor="rgba(15,23,42,0.85)",
    )

    fig.update_yaxes(title_text="BTC / USD · log scale", type="log", row=1, col=1, gridcolor="#2a2a2a")
    fig.update_yaxes(title_text="Price / Neutral · log scale", type="log", row=2, col=1, gridcolor="#2a2a2a")
    fig.update_xaxes(gridcolor="#2a2a2a")

    fig.update_layout(
        title=dict(
            text=f"Bitcoin Neutral Price<br><sup>Pure walk-forward from 18 Jul 2010 · data to {latest:%d %b %Y} · ${lp:,.0f}</sup>",
            x=0.5, xanchor="center",
        ),
        template="plotly_dark",
        hovermode="closest",
        legend=dict(orientation="h", y=-0.08, x=0),
        margin=dict(t=80, b=70, l=70, r=90),
        annotations=[_credit()],
    )

    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(CHARTS_DIR / html_name), auto_open=False, config=HTML_CONFIG)
    try:
        fig.write_image(str(CHARTS_DIR / jpg_name), width=1600, height=1000, scale=2)
    except Exception as exc:
        logging.warning("Could not write %s (%s)", jpg_name, exc)
    logging.info("Wrote %s and %s", html_name, jpg_name)
    return fig

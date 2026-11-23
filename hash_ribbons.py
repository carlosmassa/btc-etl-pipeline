"""Hash Ribbons — 30d/60d hashrate MAs for miner capitulation and recovery."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from eqm import HTML_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_hashrate(path="data/BTC_Hashrate.csv"):
    df = pd.read_csv(path, parse_dates=["Date"]).set_index("Date").sort_index()
    return df["hashrate"]


def compute_ribbons(hashrate, price):
    """30d and 60d SMA; flag capitulation (30 < 60) and recovery crosses."""
    hr = hashrate.reindex(price.index).interpolate()
    ma30 = hr.rolling(30, min_periods=20).mean()
    ma60 = hr.rolling(60, min_periods=40).mean()
    cap = ma30 < ma60
    # Recovery: was below, now above
    recovery = (~cap) & cap.shift(1).fillna(False)
    # Capitulation start: was above, now below
    cap_start = cap & (~cap.shift(1).fillna(False))
    out = pd.DataFrame({
        "Hashrate": hr,
        "MA30": ma30,
        "MA60": ma60,
        "Capitulation": cap,
        "Recovery": recovery,
        "CapStart": cap_start,
        "Price": price,
    }).dropna(subset=["MA60"])
    return out


def render_hash_ribbons(df=None, html_name="btc_hash_ribbons.html", jpg_name="btc_hash_ribbons.jpg"):
    if df is None:
        hr = load_hashrate()
        price = pd.read_csv("data/BTC_Prices.csv", parse_dates=["Date"]).set_index("Date")["Value"]
        df = compute_ribbons(hr, price)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.55, 0.45], vertical_spacing=0.05,
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
    )
    # Top: price
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Price"], mode="lines", name="BTC Price",
        line=dict(color="#f8fafc", width=1.4),
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    # Shade capitulation periods on price panel
    cap_periods = df["Capitulation"]
    in_cap = False
    start = None
    for dt, is_cap in cap_periods.items():
        if is_cap and not in_cap:
            start = dt
            in_cap = True
        elif not is_cap and in_cap:
            fig.add_vrect(x0=start, x1=dt, fillcolor="rgba(239,68,68,0.12)",
                          line_width=0, row=1, col=1)
            in_cap = False
    if in_cap:
        fig.add_vrect(x0=start, x1=df.index[-1], fillcolor="rgba(239,68,68,0.12)",
                      line_width=0, row=1, col=1)
    # Recovery markers on price
    rec = df[df["Recovery"]]
    if len(rec):
        fig.add_trace(go.Scatter(
            x=rec.index, y=rec["Price"], mode="markers", name="Recovery (buy)",
            marker=dict(symbol="triangle-up", size=10, color="#22c55e", line=dict(width=1, color="#166534")),
            hovertemplate="Recovery<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
        ), row=1, col=1)
    # Bottom: hashrate + MAs
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Hashrate"] / 1e9, mode="lines", name="Hashrate",
        line=dict(color="#94a3b8", width=0.8), opacity=0.6,
        hovertemplate="Hashrate<br>%{x|%d %b %Y}<br>%{y:.2f} ZH/s<extra></extra>",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA30"] / 1e9, mode="lines", name="30d MA",
        line=dict(color="#3b82f6", width=1.6),
        hovertemplate="30d<br>%{x|%d %b %Y}<br>%{y:.2f} ZH/s<extra></extra>",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA60"] / 1e9, mode="lines", name="60d MA",
        line=dict(color="#a855f7", width=1.6),
        hovertemplate="60d<br>%{x|%d %b %Y}<br>%{y:.2f} ZH/s<extra></extra>",
    ), row=2, col=1)
    # Shade capitulation on bottom too
    in_cap = False
    start = None
    for dt, is_cap in cap_periods.items():
        if is_cap and not in_cap:
            start = dt
            in_cap = True
        elif not is_cap and in_cap:
            fig.add_vrect(x0=start, x1=dt, fillcolor="rgba(239,68,68,0.15)",
                          line_width=0, row=2, col=1)
            in_cap = False
    if in_cap:
        fig.add_vrect(x0=start, x1=df.index[-1], fillcolor="rgba(239,68,68,0.15)",
                      line_width=0, row=2, col=1)
    # Recovery markers on ribbons
    if len(rec):
        fig.add_trace(go.Scatter(
            x=rec.index, y=rec["MA30"] / 1e9, mode="markers", name="Cross up",
            marker=dict(symbol="circle", size=8, color="#22c55e", line=dict(width=1, color="#166534")),
            hovertemplate="Cross up<br>%{x|%d %b %Y}<extra></extra>",
            showlegend=False,
        ), row=2, col=1)

    latest = df.iloc[-1]
    status = "Capitulation" if latest["Capitulation"] else "Expansion"
    color = "#ef4444" if latest["Capitulation"] else "#22c55e"
    title = (
        f"Bitcoin Hash Ribbons<br>"
        f"<sup>30d / 60d hashrate MA · {status} · "
        f"30d {latest['MA30']/1e9:.2f} ZH/s · 60d {latest['MA60']/1e9:.2f} ZH/s</sup>"
    )
    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        height=720, margin=dict(l=60, r=40, t=80, b=60),
        annotations=[
            dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper",
                 x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"),
                 xanchor="right"),
            dict(text="Red shade = 30d below 60d (miner capitulation). Green markers = recovery cross.",
                 xref="paper", yref="paper", x=0, y=-0.08, showarrow=False,
                 font=dict(size=10, color="#64748b"), xanchor="left"),
        ],
    )
    fig.update_yaxes(title_text="BTC Price (USD)", type="log", row=1, col=1)
    fig.update_yaxes(title_text="Hashrate (ZH/s)", row=2, col=1)

    out = Path("charts")
    out.mkdir(exist_ok=True)
    fig.write_html(out / html_name, config=HTML_CONFIG)
    fig.write_image(out / jpg_name, width=1400, height=900, scale=2)
    logging.info("Wrote %s and %s", html_name, jpg_name)
    return fig


if __name__ == "__main__":
    render_hash_ribbons()

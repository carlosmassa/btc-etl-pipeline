"""Bitcoin monthly RSI (14) with colored points, PlanB-style."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from eqm import HTML_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def monthly_rsi(price, period=14):
    """14-period RSI on monthly closes."""
    monthly = price.resample("ME").last().dropna()
    delta = monthly.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.dropna()


def render_rsi(html_name="btc_rsi.html", jpg_name="btc_rsi.jpg"):
    price = pd.read_csv("data/BTC_Prices.csv", parse_dates=["Date"]).set_index("Date")["Value"]
    rsi = monthly_rsi(price)
    # Color scale: blue (low) → green → yellow → red (high)
    colors = []
    for v in rsi:
        if v >= 85:
            colors.append("#ef4444")
        elif v >= 70:
            colors.append("#f97316")
        elif v >= 55:
            colors.append("#eab308")
        elif v >= 45:
            colors.append("#22c55e")
        else:
            colors.append("#3b82f6")

    fig = go.Figure()
    # RSI line
    fig.add_trace(go.Scatter(
        x=rsi.index, y=rsi, mode="lines", name="RSI (14m)",
        line=dict(color="#f8fafc", width=1.8),
        hovertemplate="RSI<br>%{x|%b %Y}<br>%{y:.1f}<extra></extra>",
    ))
    # Colored monthly points
    fig.add_trace(go.Scatter(
        x=rsi.index, y=rsi, mode="markers", name="Monthly RSI",
        marker=dict(size=8, color=colors, line=dict(width=0.5, color="#1e293b")),
        hovertemplate="%{x|%b %Y}<br>RSI %{y:.1f}<extra></extra>",
        showlegend=False,
    ))
    # Reference lines
    for level, color, label in [(30, "#3b82f6", "30"), (50, "#64748b", "50"), (70, "#ef4444", "70")]:
        fig.add_hline(y=level, line_width=0.6, line_dash="dot", line_color=color,
                      annotation_text=label, annotation_position="right",
                      annotation_font_size=10, annotation_font_color=color)

    latest = rsi.iloc[-1]
    title = (
        f"Bitcoin Relative Strength Index (RSI)<br>"
        f"<sup>14-month · Latest {latest:.1f} · {rsi.index[-1]:%b %Y}</sup>"
    )
    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        height=640, margin=dict(l=60, r=50, t=80, b=60),
        yaxis=dict(title="RSI", range=[25, 105], tickvals=[30, 40, 50, 60, 70, 80, 90, 100]),
        xaxis=dict(title="Month"),
        annotations=[
            dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper",
                 x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"),
                 xanchor="right"),
            dict(text="Colored points: blue <45 · green 45–55 · yellow 55–70 · orange 70–85 · red ≥85",
                 xref="paper", yref="paper", x=0, y=-0.08, showarrow=False,
                 font=dict(size=10, color="#64748b"), xanchor="left"),
        ],
        showlegend=False,
    )
    out = Path("charts")
    out.mkdir(exist_ok=True)
    fig.write_html(out / html_name, config=HTML_CONFIG)
    fig.write_image(out / jpg_name, width=1400, height=800, scale=2)
    logging.info("Wrote %s and %s", html_name, jpg_name)
    return fig


if __name__ == "__main__":
    render_rsi()

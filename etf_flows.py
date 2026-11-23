"""Bitcoin ETF weekly flows — contrarian indicator of local tops/bottoms."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from eqm import HTML_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Fund colors matching the attached chart style
FUND_COLORS = {
    "IBIT": "#f97316",   # orange
    "FBTC": "#22c55e",   # green
    "GBTC": "#ef4444",   # red
    "BTC": "#a855f7",    # purple
    "ARKB": "#3b82f6",   # blue
    "BITB": "#eab308",   # yellow
    "HODL": "#14b8a6",   # teal
    "MSBT": "#ec4899",   # pink
    "BRRR": "#84cc16",   # lime
    "EZBC": "#06b6d4",   # cyan
    "BTCO": "#f43f5e",   # rose
    "BTCW": "#8b5cf6",   # violet
    "Other": "#64748b",  # slate
}


def load_flows(path="data/BTC_ETF_Flows.csv"):
    df = pd.read_csv(path, parse_dates=["Date"]).sort_values("Date")
    return df.set_index("Date")


def weekly_aggregate(df):
    """Sum daily flows into ISO weeks; keep the Friday (or last day) of each week."""
    w = df.resample("W-FRI").agg({
        "NetFlowUSD": "sum",
        "BTCClose": "last",
        "AUM": "last",
    }).dropna(subset=["NetFlowUSD"])
    return w


def render_etf_flows(df=None, html_name="btc_etf_flows.html", jpg_name="btc_etf_flows.jpg"):
    if df is None:
        df = load_flows()
    weekly = weekly_aggregate(df)
    # Extreme thresholds: top/bottom 10% of weekly flows
    q10 = weekly["NetFlowUSD"].quantile(0.10)
    q90 = weekly["NetFlowUSD"].quantile(0.90)
    # 4-week rolling sum for smoother signal
    weekly["Roll4"] = weekly["NetFlowUSD"].rolling(4, min_periods=1).sum()

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.55, 0.45], vertical_spacing=0.06,
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]],
    )
    # Top: BTC price
    fig.add_trace(go.Scatter(
        x=weekly.index, y=weekly["BTCClose"], mode="lines", name="BTC Price",
        line=dict(color="#f8fafc", width=1.6),
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    # Mark local extremes near extreme flow weeks
    extremes = weekly[(weekly["NetFlowUSD"] <= q10) | (weekly["NetFlowUSD"] >= q90)]
    for _, row in extremes.iterrows():
        color = "#22c55e" if row["NetFlowUSD"] < 0 else "#ef4444"
        fig.add_annotation(
            x=row.name, y=row["BTCClose"],
            text=f"${abs(row['NetFlowUSD'])/1e9:.1f}b",
            showarrow=True, arrowhead=2, arrowcolor=color,
            font=dict(size=10, color=color), ax=0, ay=-25 if row["NetFlowUSD"] > 0 else 25,
            row=1, col=1,
        )
    # Bottom: weekly flow bars
    colors = np.where(weekly["NetFlowUSD"] >= 0, "#22c55e", "#ef4444")
    fig.add_trace(go.Bar(
        x=weekly.index, y=weekly["NetFlowUSD"] / 1e9, name="Weekly Net Flow",
        marker_color=colors, opacity=0.85,
        hovertemplate="Flow<br>%{x|%d %b %Y}<br>$%{y:.2f}b<extra></extra>",
    ), row=2, col=1)
    # 4-week rolling
    fig.add_trace(go.Scatter(
        x=weekly.index, y=weekly["Roll4"] / 1e9, mode="lines", name="4-week sum",
        line=dict(color="#94a3b8", width=1.2, dash="dot"),
        hovertemplate="4w sum<br>%{x|%d %b %Y}<br>$%{y:.2f}b<extra></extra>",
    ), row=2, col=1)
    # Zero line
    fig.add_hline(y=0, line_width=0.6, line_color="#475569", row=2, col=1)

    latest = weekly.iloc[-1]
    title = (
        f"Bitcoin ETF Weekly Flows<br>"
        f"<sup>Week of {latest.name:%d %b %Y} · "
        f"{'$+' if latest['NetFlowUSD']>=0 else ''}${latest['NetFlowUSD']/1e9:.2f}b · "
        f"BTC ${latest['BTCClose']:,.0f}</sup>"
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
            dict(text="Source: TFTC (CC BY 4.0) · Extreme weeks (top/bottom 10%) marked",
                 xref="paper", yref="paper", x=0, y=-0.08, showarrow=False,
                 font=dict(size=10, color="#64748b"), xanchor="left"),
        ],
    )
    fig.update_yaxes(title_text="BTC Price (USD)", type="log", row=1, col=1)
    fig.update_yaxes(title_text="Weekly Net Flow ($b)", row=2, col=1)
    fig.update_xaxes(title_text="Week ending", row=2, col=1)

    out = Path("charts")
    out.mkdir(exist_ok=True)
    fig.write_html(out / html_name, config=HTML_CONFIG)
    fig.write_image(out / jpg_name, width=1400, height=900, scale=2)
    logging.info("Wrote %s and %s", html_name, jpg_name)
    return fig


if __name__ == "__main__":
    render_etf_flows()

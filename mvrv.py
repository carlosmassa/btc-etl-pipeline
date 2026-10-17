"""MVRV Quantile Bands — cycle-adjusted, walk-forward."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BANDS = [
    (0.05, 0.25, "rgba(34,197,94,0.18)"),
    (0.25, 0.50, "rgba(250,204,21,0.12)"),
    (0.50, 0.75, "rgba(251,146,60,0.12)"),
    (0.75, 0.95, "rgba(239,68,68,0.18)"),
]
QS = [0.05, 0.25, 0.50, 0.75, 0.95]


def load_mvrv(path="data/BTC_MVRV.csv"):
    df = pd.read_csv(path, parse_dates=["Date"]).set_index("Date").sort_index()
    return df.dropna(subset=["Price", "MVRV", "Realized"])


def cycle_adjust(mvrv):
    """Normalize MVRV to a contracting envelope of cycle peaks and troughs."""
    # Major cycle peaks and troughs (local extremes with 1y window)
    peaks = mvrv[(mvrv == mvrv.rolling(365, center=True).max()) & (mvrv > 2.5)]
    troughs = mvrv[(mvrv == mvrv.rolling(365, center=True).min()) & (mvrv < 1.2)]
    if len(peaks) < 3 or len(troughs) < 3:
        return mvrv
    # Fit declining line through peaks, rising through troughs (on days since start)
    start = mvrv.index[0]
    peak_days = (peaks.index - start).days.to_numpy()
    trough_days = (troughs.index - start).days.to_numpy()
    peak_fit = np.polyfit(peak_days, np.log(peaks.to_numpy()), 1)
    trough_fit = np.polyfit(trough_days, np.log(troughs.to_numpy()), 1)
    days = (mvrv.index - start).days.to_numpy()
    peak_env = np.exp(np.polyval(peak_fit, days))
    trough_env = np.exp(np.polyval(trough_fit, days))
    peak_env = np.maximum(peak_env, trough_env * 1.05)
    norm = (mvrv.to_numpy() - trough_env) / (peak_env - trough_env)
    return pd.Series(np.clip(norm, -0.5, 1.5), index=mvrv.index)


def compute_bands(df):
    mvrv = df["MVRV"]
    adj = cycle_adjust(mvrv)
    n = len(df)
    pct = np.full(n, np.nan)
    bands = {q: np.full(n, np.nan) for q in QS}
    mvrv_np = mvrv.to_numpy()
    adj_np = adj.to_numpy()
    realized = df["Realized"].to_numpy()
    for s in range(180, n):
        sample = mvrv_np[: s + 1]
        sample = sample[~np.isnan(sample)]
        if sample.size < 100:
            continue
        ordered = np.sort(sample)
        for q in QS:
            bands[q][s] = realized[s] * np.quantile(ordered, q)
        # Cycle-adjusted percentile for the oscillator
        adj_sample = adj_np[: s + 1]
        adj_sample = adj_sample[~np.isnan(adj_sample)]
        if adj_sample.size > 50 and not np.isnan(adj_np[s]):
            pct[s] = np.searchsorted(np.sort(adj_sample), adj_np[s], side="right") / adj_sample.size
    out = df.copy()
    out["pct"] = pct * 100
    for q in QS:
        out[f"q{int(q*100)}"] = bands[q]
    return out


def render(df, *, html_name, jpg_name):
    d = df.dropna(subset=["pct"])
    last = d.iloc[-1]
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.68, 0.32], vertical_spacing=0.04)
    # Top: tight bands around realized price
    colors = {
        (5, 25): "rgba(34,197,94,0.15)",
        (25, 50): "rgba(250,204,21,0.10)",
        (50, 75): "rgba(251,146,60,0.10)",
        (75, 95): "rgba(239,68,68,0.15)",
    }
    for (q0, q1), color in colors.items():
        fig.add_trace(go.Scatter(
            x=d.index, y=d[f"q{q1}"], mode="lines", line=dict(width=0),
            showlegend=False, hoverinfo="skip",
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=d.index, y=d[f"q{q0}"], mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor=color, name=f"{q0}–{q1}%",
            hovertemplate=f"{q0}%<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=d.index, y=d["Price"], mode="lines", name="BTC Price",
        line=dict(color="#f8fafc", width=1.6),
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=d.index, y=d["Realized"], mode="lines", name="Realized Price",
        line=dict(color="#f59e0b", width=1.4),
        hovertemplate="Realized<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    # Bottom: percentile with filled band traces
    band_colors = [(0, 25, "rgba(34,197,94,0.35)"), (25, 50, "rgba(250,204,21,0.30)"),
                   (50, 75, "rgba(251,146,60,0.30)"), (75, 100, "rgba(239,68,68,0.35)")]
    for y0, y1, color in band_colors:
        fig.add_trace(go.Scatter(
            x=[d.index[0], d.index[-1], d.index[-1], d.index[0]],
            y=[y0, y0, y1, y1], mode="lines", fill="toself",
            fillcolor=color, line=dict(width=0), showlegend=False, hoverinfo="skip",
        ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=d.index, y=d["pct"], mode="lines", name="Cycle-adjusted percentile",
        line=dict(color="#f8fafc", width=1.4),
        hovertemplate="Percentile<br>%{x|%d %b %Y}<br>%{y:.0f}%<extra></extra>",
    ), row=2, col=1)
    fig.update_yaxes(type="log", title_text="Price (USD)", row=1, col=1, gridcolor="#333")
    fig.update_yaxes(title_text="Percentile", row=2, col=1, gridcolor="#333", range=[0, 100])
    fig.update_layout(
        title=dict(
            text=(
                "Bitcoin: MVRV Quantile Bands<br><sup>"
                f"{d.index[-1]:%d %B %Y}  ·  ${last['Price']:,.0f}  ·  "
                f"MVRV {last['MVRV']:.2f}  ·  percentile {last['pct']:.0f}%</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
        annotations=[
            dict(
                text="Percentile of cycle-adjusted MVRV<br>(0 = cheapest day in history, 100 = most expensive)",
                xref="paper", yref="paper", x=0.02, y=0.28, showarrow=False,
                font=dict(size=10, color="#9aa6b2"), xanchor="left", yanchor="top",
            ),
            dict(
                text="Chart by: @CarlesMassa", xref="paper", yref="paper",
                x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"),
                xanchor="right",
            ),
        ],
    )
    html_path = Path("charts") / html_name
    jpg_path = Path("charts") / jpg_name
    html_path.parent.mkdir(exist_ok=True)
    fig.write_html(str(html_path), auto_open=False)
    fig.write_image(str(jpg_path), width=1600, height=900, scale=2)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path

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
    """Normalize MVRV by a contracting envelope of peaks and troughs."""
    peaks = mvrv.rolling(365, min_periods=90).max()
    troughs = mvrv.rolling(365, min_periods=90).min()
    # Fit declining peak line and rising trough line on log-time
    days = np.arange(len(mvrv))
    valid = ~(peaks.isna() | troughs.isna())
    if valid.sum() < 100:
        return mvrv
    log_t = np.log1p(days[valid])
    peak_fit = np.polyfit(log_t, np.log(peaks[valid]), 1)
    trough_fit = np.polyfit(log_t, np.log(troughs[valid]), 1)
    peak_env = np.exp(np.polyval(peak_fit, np.log1p(days)))
    trough_env = np.exp(np.polyval(trough_fit, np.log1p(days)))
    # Ensure peak > trough
    peak_env = np.maximum(peak_env, trough_env * 1.1)
    norm = (mvrv.to_numpy() - trough_env) / (peak_env - trough_env)
    return pd.Series(norm, index=mvrv.index)


def compute_bands(df):
    mvrv = df["MVRV"]
    adj = cycle_adjust(mvrv)
    n = len(df)
    pct = np.full(n, np.nan)
    bands = {q: np.full(n, np.nan) for q in QS}
    adj_np = adj.to_numpy()
    mvrv_np = mvrv.to_numpy()
    realized = df["Realized"].to_numpy()
    for s in range(180, n):
        sample = adj_np[: s + 1]
        sample = sample[~np.isnan(sample)]
        if sample.size < 100:
            continue
        ordered = np.sort(sample)
        pct[s] = np.searchsorted(ordered, adj_np[s], side="right") / ordered.size
        for q in QS:
            q_val = np.quantile(ordered, q)
            # Map back: price = realized * mvrv_at_that_quantile
            # Approximate by scaling the current mvrv by the quantile ratio
            ratio = q_val / max(adj_np[s], 0.01) if adj_np[s] > 0 else 1.0
            bands[q][s] = realized[s] * mvrv_np[s] * ratio
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
        line=dict(color="#111", width=1.6),
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=d.index, y=d["Realized"], mode="lines", name="Realized Price",
        line=dict(color="#f59e0b", width=1.4),
        hovertemplate="Realized<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    # Bottom: percentile with horizontal bands
    fig.add_hrect(y0=0, y1=25, fillcolor="rgba(34,197,94,0.25)", line_width=0, row=2, col=1)
    fig.add_hrect(y0=25, y1=50, fillcolor="rgba(250,204,21,0.18)", line_width=0, row=2, col=1)
    fig.add_hrect(y0=50, y1=75, fillcolor="rgba(251,146,60,0.18)", line_width=0, row=2, col=1)
    fig.add_hrect(y0=75, y1=100, fillcolor="rgba(239,68,68,0.25)", line_width=0, row=2, col=1)
    fig.add_trace(go.Scatter(
        x=d.index, y=d["pct"], mode="lines", name="Cycle-adjusted percentile",
        line=dict(color="#111", width=1.4),
        hovertemplate="Percentile<br>%{x|%d %b %Y}<br>%{y:.0f}%<extra></extra>",
    ), row=2, col=1)
    fig.update_yaxes(type="log", title_text="Price (USD)", row=1, col=1, gridcolor="#eee")
    fig.update_yaxes(title_text="Percentile", row=2, col=1, gridcolor="#eee", range=[0, 100])
    fig.update_layout(
        title=dict(
            text=(
                "Bitcoin: MVRV Quantile Bands<br><sup>"
                f"{d.index[-1]:%d %B %Y}  ·  ${last['Price']:,.0f}  ·  "
                f"MVRV {last['MVRV']:.2f}  ·  percentile {last['pct']:.0f}%</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_white", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
    )
    html_path = Path("charts") / html_name
    jpg_path = Path("charts") / jpg_name
    html_path.parent.mkdir(exist_ok=True)
    fig.write_html(str(html_path), auto_open=False)
    fig.write_image(str(jpg_path), width=1600, height=900, scale=2)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path

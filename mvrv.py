"""MVRV Quantile Bands — cycle-adjusted, walk-forward."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
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


def render_momentum_chart(df):
    """MVRV momentum oscillator."""
    d = df.loc["2010-01-01":].copy()
    d["sma365"] = d["MVRV"].rolling(365).mean()
    d["momentum"] = d["MVRV"] - d["sma365"]
    d = d.dropna(subset=["momentum"])
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=d.index, y=d["Price"], mode="lines", name="Price", line=dict(color="#f8fafc", width=1.2)), secondary_y=False)
    pos = d[d["momentum"] >= 0]
    neg = d[d["momentum"] < 0]
    fig.add_trace(go.Bar(x=pos.index, y=pos["momentum"], name="Momentum +", marker_color="#22c55e", marker_line_width=0), secondary_y=True)
    fig.add_trace(go.Bar(x=neg.index, y=neg["momentum"], name="Momentum −", marker_color="#ef4444", marker_line_width=0), secondary_y=True)
    fig.update_yaxes(type="log", title_text="Price (USD)", secondary_y=False)
    fig.update_yaxes(title_text="Momentum", secondary_y=True, zeroline=True, zerolinecolor="#64748b")
    fig.update_layout(
        title=dict(text="Bitcoin MVRV Momentum Oscillator<br><sup>MVRV minus its 365-day average</sup>", x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest", barmode="relative",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=70),
        annotations=[dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper", x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"), xanchor="right")],
    )
    fig.write_html("charts/btc_mvrv_momentum.html")
    fig.write_image("charts/btc_mvrv_momentum.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote MVRV momentum chart")
    """STH realized price as support and resistance."""
    sth = pd.read_csv("data/BTC_STH.csv", parse_dates=["Date"]).set_index("Date")
    price = pd.read_csv("data/BTC_Prices.csv", parse_dates=["Date"]).set_index("Date")
    df = price.join(sth, how="inner").dropna(subset=["Value", "STH"])
    df["ratio"] = df["Value"] / df["STH"]
    df["touch"] = (df["ratio"] - 1).abs() < 0.03
    touches = df[df["touch"]]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df["Value"], mode="lines", name="BTC Price", line=dict(color="#f8fafc", width=1.6)))
    fig.add_trace(go.Scatter(x=df.index, y=df["STH"], mode="lines", name="STH Realized Price", line=dict(color="#f5a24a", width=1.4)))
    fig.add_trace(go.Scatter(x=touches.index, y=touches["Value"], mode="markers", name="Touch (±3%)",
                             marker=dict(color="#22c55e", size=6, symbol="circle"),
                             hovertemplate="Touch<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>"))
    fig.update_yaxes(type="log", title_text="Price (USD)")
    fig.update_layout(
        title=dict(text="STH Realized Price as support and resistance<br><sup>Coins moved in the last 155 days</sup>", x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
        annotations=[dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper", x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"), xanchor="right")],
    )
    fig.write_html("charts/btc_sth_support.html")
    fig.write_image("charts/btc_sth_support.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote STH support chart")
    """MVRV SMA, raw signals, and percentile signals. From 2012."""
    d = df.loc["2012-01-01":].copy()
    d["sma365"] = d["MVRV"].rolling(365).mean()
    d["above"] = d["MVRV"] > d["sma365"]
    d["cross_up"] = (d["above"] & ~d["above"].shift(1).fillna(False))
    d["cross_down"] = (~d["above"] & d["above"].shift(1).fillna(False))
    sells_raw = d[d["cross_down"] & (d["MVRV"] > 2.5)]
    buys_raw = d[d["cross_up"] & (d["MVRV"] < 1.2)]
    # SMA chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d.index, y=d["MVRV"], mode="lines", name="MVRV", line=dict(color="#7dd3fc", width=1.4)))
    fig.add_trace(go.Scatter(x=d.index, y=d["sma365"], mode="lines", name="365-day SMA", line=dict(color="#f5a24a", width=1.4)))
    fig.add_hline(y=2.5, line_dash="dot", line_color="#ef4444", annotation_text="2.5")
    fig.add_hline(y=1.2, line_dash="dot", line_color="#22c55e", annotation_text="1.2")
    fig.update_layout(title=dict(text="MVRV and 365-day SMA<br><sup>From 2012</sup>", x=0.5, xanchor="center"),
                      template="plotly_dark", hovermode="closest", legend=dict(orientation="h", y=-0.12, x=0),
                      margin=dict(t=90, b=80, l=70, r=40), yaxis_title="MVRV")
    fig.write_html("charts/btc_mvrv_sma.html")
    fig.write_image("charts/btc_mvrv_sma.jpg", width=1600, height=700, scale=2)
    # Raw signals
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=d.index, y=d["Price"], mode="lines", name="BTC Price", line=dict(color="#f8fafc", width=1.4)))
    fig2.add_trace(go.Scatter(x=buys_raw.index, y=buys_raw["Price"], mode="markers", name="Buy", marker=dict(color="#22c55e", size=10, symbol="triangle-up")))
    fig2.add_trace(go.Scatter(x=sells_raw.index, y=sells_raw["Price"], mode="markers", name="Sell", marker=dict(color="#ef4444", size=10, symbol="triangle-down")))
    fig2.update_yaxes(type="log", title_text="Price (USD)")
    fig2.update_layout(title=dict(text="BTC Price — raw MVRV SMA crosses<br><sup>Buy: cross up when MVRV < 1.2 · Sell: cross down when MVRV > 2.5</sup>", x=0.5, xanchor="center"),
                       template="plotly_dark", hovermode="closest", legend=dict(orientation="h", y=-0.12, x=0),
                       margin=dict(t=90, b=80, l=70, r=40))
    fig2.write_html("charts/btc_mvrv_signals.html")
    fig2.write_image("charts/btc_mvrv_signals.jpg", width=1600, height=700, scale=2)
    # Percentile signals
    adj = cycle_adjust(d["MVRV"])
    d["pct"] = adj.expanding().rank(pct=True)
    d["pct_sma"] = d["pct"].rolling(365).mean()
    d["pct_above"] = d["pct"] > d["pct_sma"]
    d["pct_cross_up"] = (d["pct_above"] & ~d["pct_above"].shift(1).fillna(False))
    d["pct_cross_down"] = (~d["pct_above"] & d["pct_above"].shift(1).fillna(False))
    sells_pct = d[d["pct_cross_down"] & (d["pct"] > 0.7)]
    buys_pct = d[d["pct_cross_up"] & (d["pct"] < 0.3)]
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=d.index, y=d["Price"], mode="lines", name="BTC Price", line=dict(color="#f8fafc", width=1.4)))
    fig3.add_trace(go.Scatter(x=buys_pct.index, y=buys_pct["Price"], mode="markers", name="Buy", marker=dict(color="#22c55e", size=10, symbol="triangle-up")))
    fig3.add_trace(go.Scatter(x=sells_pct.index, y=sells_pct["Price"], mode="markers", name="Sell", marker=dict(color="#ef4444", size=10, symbol="triangle-down")))
    fig3.update_yaxes(type="log", title_text="Price (USD)")
    fig3.update_layout(title=dict(text="BTC Price — cycle-adjusted percentile crosses<br><sup>Buy: cross up when percentile < 30% · Sell: cross down when percentile > 70%</sup>", x=0.5, xanchor="center"),
                       template="plotly_dark", hovermode="closest", legend=dict(orientation="h", y=-0.12, x=0),
                       margin=dict(t=90, b=80, l=70, r=40))
    fig3.write_html("charts/btc_mvrv_pct_signals.html")
    fig3.write_image("charts/btc_mvrv_pct_signals.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote MVRV SMA charts")
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
        margin=dict(t=90, b=110, l=70, r=40),
        annotations=[
            dict(
                text="Percentile of cycle-adjusted MVRV (0 = cheapest day in history, 100 = most expensive)",
                xref="paper", yref="paper", x=0.5, y=-0.04, showarrow=False,
                font=dict(size=10, color="#9aa6b2"), xanchor="center", yanchor="top",
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

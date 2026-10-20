"""MVRV Quantile Bands — cycle-adjusted, walk-forward."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from eqm import HTML_CONFIG

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
    fig.write_html(str(html_path), auto_open=False, config=HTML_CONFIG)
    fig.write_image(str(jpg_path), width=1600, height=900, scale=2)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path


def render_sma_charts(df):
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
    fig.write_html("charts/btc_mvrv_sma.html", config=HTML_CONFIG)
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
    fig2.write_html("charts/btc_mvrv_signals.html", config=HTML_CONFIG)
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
    fig3.write_html("charts/btc_mvrv_pct_signals.html", config=HTML_CONFIG)
    fig3.write_image("charts/btc_mvrv_pct_signals.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote MVRV SMA charts")


def render_sth_chart():
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
    fig.write_html("charts/btc_sth_support.html", config=HTML_CONFIG)
    fig.write_image("charts/btc_sth_support.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote STH support chart")




def render_mvrv_envelope():
    """MVRV cycle envelope: declining log line through peaks, rising through troughs."""
    mvrv = pd.read_csv("data/BTC_MVRV.csv", parse_dates=["Date"]).set_index("Date")
    df = mvrv.loc["2010-08-01":].copy()
    peaks = [("2011-06-08", 5.8), ("2013-12-04", 5.9), ("2017-12-17", 4.1), ("2021-04-14", 3.9)]
    troughs = [("2011-11-18", 0.6), ("2015-01-14", 0.7), ("2018-12-15", 0.7), ("2022-11-21", 0.8)]
    peak_dates = pd.to_datetime([p[0] for p in peaks])
    peak_vals = np.array([p[1] for p in peaks])
    trough_dates = pd.to_datetime([t[0] for t in troughs])
    trough_vals = np.array([t[1] for t in troughs])
    peak_days = (peak_dates - df.index[0]).days
    trough_days = (trough_dates - df.index[0]).days
    peak_fit = np.polyfit(peak_days, np.log(peak_vals), 1)
    trough_fit = np.polyfit(trough_days, np.log(trough_vals), 1)
    all_days = (df.index - df.index[0]).days
    peak_line = np.exp(np.polyval(peak_fit, all_days))
    trough_line = np.exp(np.polyval(trough_fit, all_days))
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df["MVRV"], mode="lines", name="MVRV", line=dict(color="#7dd3fc", width=1.2)))
    fig.add_trace(go.Scatter(x=df.index, y=peak_line, mode="lines", name="Peak line (declining)", line=dict(color="#ef4444", width=1.4, dash="dot")))
    fig.add_trace(go.Scatter(x=df.index, y=trough_line, mode="lines", name="Trough line (rising)", line=dict(color="#22c55e", width=1.4, dash="dot")))
    fig.add_trace(go.Scatter(x=peak_dates, y=peak_vals, mode="markers", name="Confirmed peaks",
                             marker=dict(color="#ef4444", size=10, symbol="triangle-down")))
    fig.add_trace(go.Scatter(x=trough_dates, y=trough_vals, mode="markers", name="Confirmed troughs",
                             marker=dict(color="#22c55e", size=10, symbol="triangle-up")))
    fig.update_layout(
        title=dict(text="MVRV Cycle Envelope<br><sup>Declining log line through peaks, rising log line through troughs</sup>", x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
        yaxis_title="MVRV",
        annotations=[dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper", x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"), xanchor="right")],
    )
    fig.write_html("charts/btc_mvrv_envelope.html", config=HTML_CONFIG)
    fig.write_image("charts/btc_mvrv_envelope.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote MVRV envelope chart")


def render_lth_chart():
    """LTH realized price chart."""
    lth = pd.read_csv("data/BTC_LTH.csv", parse_dates=["Date"]).set_index("Date")
    price = pd.read_csv("data/BTC_Prices.csv", parse_dates=["Date"]).set_index("Date")
    df = price.join(lth, how="inner").dropna()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df["Value"], mode="lines", name="BTC Price", line=dict(color="#f8fafc", width=1.4)))
    fig.add_trace(go.Scatter(x=df.index, y=df["LTH"], mode="lines", name="LTH Realized Price", line=dict(color="#7dd3fc", width=1.6)))
    fig.update_yaxes(type="log", title_text="Price (USD)")
    fig.update_layout(
        title=dict(text="Bitcoin Long-Term Holder Realized Price<br><sup>Coins unmoved for 155+ days</sup>", x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
        annotations=[dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper", x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"), xanchor="right")],
    )
    fig.write_html("charts/btc_lth_realized.html", config=HTML_CONFIG)
    fig.write_image("charts/btc_lth_realized.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote LTH realized price chart")

def render_sth_lth_chart():
    """Combined STH and LTH realized prices."""
    sth = pd.read_csv("data/BTC_STH.csv", parse_dates=["Date"]).set_index("Date")
    lth = pd.read_csv("data/BTC_LTH.csv", parse_dates=["Date"]).set_index("Date")
    price = pd.read_csv("data/BTC_Prices.csv", parse_dates=["Date"]).set_index("Date")
    df = price.join(sth, how="inner").join(lth, how="inner").dropna()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df["Value"], mode="lines", name="BTC Price", line=dict(color="#f8fafc", width=1.4)))
    fig.add_trace(go.Scatter(x=df.index, y=df["STH"], mode="lines", name="STH Realized Price", line=dict(color="#f5a24a", width=1.4)))
    fig.add_trace(go.Scatter(x=df.index, y=df["LTH"], mode="lines", name="LTH Realized Price", line=dict(color="#7dd3fc", width=1.4)))
    fig.update_yaxes(type="log", title_text="Price (USD)")
    fig.update_layout(
        title=dict(text="Bitcoin STH and LTH Realized Prices<br><sup>STH: coins moved in last 155 days · LTH: coins unmoved for 155+ days</sup>", x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
        annotations=[dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper", x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"), xanchor="right")],
    )
    fig.write_html("charts/btc_sth_lth.html", config=HTML_CONFIG)
    fig.write_image("charts/btc_sth_lth.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote STH and LTH chart")

def render_sth_inflections():
    """STH major inflections: 30-day prior trend + 20-day confirmation."""
    from scipy.signal import savgol_filter
    sth = pd.read_csv("data/BTC_STH.csv", parse_dates=["Date"]).set_index("Date")
    price = pd.read_csv("data/BTC_Prices.csv", parse_dates=["Date"]).set_index("Date")
    df = price.join(sth, how="inner").dropna(subset=["Value", "STH"]).sort_index()
    window = 121
    slope = savgol_filter(df["STH"].values, window_length=window, polyorder=3, deriv=1, delta=1)
    df["slope"] = slope
    lookback, persist, min_slope = 30, 20, 0.001
    ups, downs = [], []
    for i in range(window + lookback, len(df) - persist):
        if (df["slope"].iloc[i-lookback:i] < 0).all() and df["slope"].iloc[i] > min_slope:
            if (df["slope"].iloc[i:i+persist] > 0).all():
                ups.append(df.index[i])
        if (df["slope"].iloc[i-lookback:i] > 0).all() and df["slope"].iloc[i] < -min_slope:
            if (df["slope"].iloc[i:i+persist] < 0).all():
                downs.append(df.index[i])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df["Value"], mode="lines", name="BTC Price", line=dict(color="#f8fafc", width=1.2)))
    fig.add_trace(go.Scatter(x=df.index, y=df["STH"], mode="lines", name="STH Realized Price", line=dict(color="#f5a24a", width=1.4)))
    if ups:
        fig.add_trace(go.Scatter(x=ups, y=df.loc[ups, "STH"], mode="markers", name="To uptrend",
                                 marker=dict(color="#22c55e", size=12, symbol="triangle-up"),
                                 hovertemplate="To uptrend<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>"))
    if downs:
        fig.add_trace(go.Scatter(x=downs, y=df.loc[downs, "STH"], mode="markers", name="To downtrend",
                                 marker=dict(color="#ef4444", size=12, symbol="triangle-down"),
                                 hovertemplate="To downtrend<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>"))
    fig.update_yaxes(type="log", title_text="Price (USD)")
    fig.update_layout(
        title=dict(text="STH Realized Price — major inflections<br><sup>Large turns only: 30-day prior trend + 20-day confirmation</sup>", x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
        annotations=[dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper", x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"), xanchor="right")],
    )
    fig.write_html("charts/btc_sth_inflections.html", config=HTML_CONFIG)
    fig.write_image("charts/btc_sth_inflections.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote STH inflections chart")

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
    fig.write_html("charts/btc_mvrv_momentum.html", config=HTML_CONFIG)
    fig.write_image("charts/btc_mvrv_momentum.jpg", width=1600, height=700, scale=2)
    logging.info("Wrote MVRV momentum chart")

"""Satoshi Compass — composite accumulation score from price-derived metrics."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from eqm import HTML_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def compute_compass(price):
    """Build 0-100 accumulation score from several price-derived metrics.
    Higher score = cheaper relative to history (more accumulation opportunity).
    """
    p = price.sort_index()
    # Mayer Multiple: price / 200d SMA. Low = cheap.
    sma200d = p.rolling(200, min_periods=100).mean()
    mayer = p / sma200d
    # 200-week SMA ratio
    sma200w = p.rolling(200 * 7, min_periods=100 * 7).mean()
    ratio_200w = p / sma200w
    # Power-law fair value (log-log fit on days since start)
    start = p.index[0]
    days = (p.index - start).days.to_numpy()
    log_p = np.log(p.to_numpy())
    log_d = np.log(np.maximum(days, 1))
    # Expanding window fit would be ideal; use full-history for speed, then residual
    valid = ~np.isnan(log_p)
    if valid.sum() > 100:
        coef = np.polyfit(log_d[valid], log_p[valid], 1)
        fair = np.exp(np.polyval(coef, log_d))
        resid = log_p - np.log(fair)
        # Percentile of residual (low residual = cheap)
        resid_pct = pd.Series(resid, index=p.index).expanding(min_periods=200).rank(pct=True)
    else:
        fair = p.to_numpy()
        resid_pct = pd.Series(0.5, index=p.index)
    # Weekly RSI
    weekly = p.resample("W").last()
    delta = weekly.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    rsi = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    rsi = rsi.reindex(p.index, method="ffill")
    # Drawdown from ATH
    ath = p.cummax()
    dd = (p - ath) / ath
    # Days since ATH
    days_ath = pd.Series(0, index=p.index)
    last_ath = p.index[0]
    for i, (dt, px) in enumerate(p.items()):
        if px >= ath.loc[dt]:
            last_ath = dt
        days_ath.loc[dt] = (dt - last_ath).days
    # Normalize each to 0-1 where 1 = cheapest / most accumulation
    def inv_pct(s, low, high):
        """Map s to 0-1, inverted so low values score high."""
        clipped = s.clip(low, high)
        return 1 - (clipped - low) / (high - low)

    score_mayer = inv_pct(mayer, 0.4, 2.5)
    score_200w = inv_pct(ratio_200w, 0.5, 3.0)
    score_pl = 1 - resid_pct  # low residual = high score
    score_rsi = inv_pct(rsi, 20, 80)
    score_dd = inv_pct(dd, -0.85, 0)
    score_days = (days_ath / 400).clip(0, 1)  # longer since ATH = higher
    # Weighted average
    weights = {"mayer": 0.20, "200w": 0.15, "pl": 0.20, "rsi": 0.15, "dd": 0.15, "days": 0.15}
    score = (
        score_mayer * weights["mayer"] +
        score_200w * weights["200w"] +
        score_pl * weights["pl"] +
        score_rsi * weights["rsi"] +
        score_dd * weights["dd"] +
        score_days * weights["days"]
    ) * 100
    out = pd.DataFrame({
        "Price": p,
        "Score": score,
        "Fair": fair,
        "Floor": fair * np.exp(np.percentile(resid[valid], 5)),
        "Mayer": mayer,
        "Ratio200w": ratio_200w,
        "RSI": rsi,
        "Drawdown": dd,
        "DaysATH": days_ath,
        "PL_pct": resid_pct,
    }).dropna(subset=["Score"])
    return out


def render_compass(html_name="btc_compass.html", jpg_name="btc_compass.jpg"):
    price = pd.read_csv("data/BTC_Prices.csv", parse_dates=["Date"]).set_index("Date")["Value"]
    df = compute_compass(price)
    # Color price by score: cyan (high score / cheap) → white → purple (low score / expensive)
    colors = []
    for s in df["Score"]:
        if s >= 70:
            colors.append("#22d3ee")
        elif s >= 50:
            colors.append("#a5f3fc")
        elif s >= 30:
            colors.append("#e2e8f0")
        else:
            colors.append("#c084fc")
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.65, 0.35], vertical_spacing=0.04,
    )
    # Top: price colored by score
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Price"], mode="lines", name="BTC Price",
        line=dict(color="#f8fafc", width=1.2),
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Fair"], mode="lines", name="Fair value",
        line=dict(color="#94a3b8", width=1.0, dash="dot"),
        hovertemplate="Fair<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Floor"], mode="lines", name="Floor",
        line=dict(color="#38bdf8", width=1.0),
        hovertemplate="Floor<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=1, col=1)
    # Bottom: score
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Score"], mode="lines", name="Score",
        line=dict(color="#22d3ee", width=1.4),
        hovertemplate="Score<br>%{x|%d %b %Y}<br>%{y:.0f}<extra></extra>",
    ), row=2, col=1)
    fig.add_hrect(y0=70, y1=100, fillcolor="rgba(34,211,238,0.08)", line_width=0, row=2, col=1)
    fig.add_hrect(y0=0, y1=30, fillcolor="rgba(192,132,252,0.08)", line_width=0, row=2, col=1)

    latest = df.iloc[-1]
    title = (
        f"Satoshi Compass — Accumulation Radar<br>"
        f"<sup>Score {latest['Score']:.0f}/100 · "
        f"Mayer {latest['Mayer']:.2f}× · 200W {latest['Ratio200w']:.2f}× · "
        f"RSI {latest['RSI']:.0f} · DD {latest['Drawdown']:.0%} · "
        f"{latest['DaysATH']:.0f}d since ATH</sup>"
    )
    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center"),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.10, x=0),
        height=720, margin=dict(l=60, r=40, t=90, b=60),
        annotations=[
            dict(text="Chart by: @CarlesMassa", xref="paper", yref="paper",
                 x=1, y=-0.08, showarrow=False, font=dict(size=11, color="#9aa6b2"),
                 xanchor="right"),
            dict(text="Score 0–100: higher = cheaper vs history. Cyan ≥70 (buy zone), purple ≤30 (hot).",
                 xref="paper", yref="paper", x=0, y=-0.08, showarrow=False,
                 font=dict(size=10, color="#64748b"), xanchor="left"),
        ],
    )
    fig.update_yaxes(title_text="BTC Price (USD)", type="log", row=1, col=1)
    fig.update_yaxes(title_text="Score", range=[0, 100], row=2, col=1)
    out = Path("charts")
    out.mkdir(exist_ok=True)
    fig.write_html(out / html_name, config=HTML_CONFIG)
    fig.write_image(out / jpg_name, width=1400, height=900, scale=2)
    logging.info("Wrote %s and %s", html_name, jpg_name)
    return fig


if __name__ == "__main__":
    render_compass()

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

CHARTS_DIR = Path("charts")
STABILITY_MIN_POINTS = 400
STABILITY_CMIN = 0.70
STABILITY_CMAX = 1.00
STABILITY_COLORSCALE = [
    [0.00, "#D62728"],
    [0.35, "#FF7F0E"],
    [0.55, "#FFDD00"],
    [0.80, "#90EE90"],
    [1.00, "#2CA02C"],
]


def compute_power_law_stability(df, min_points=STABILITY_MIN_POINTS):
    work = df.dropna(subset=["Date", "Value", "ind"]).sort_values("Date").reset_index(drop=True)
    x = np.log(work["ind"].astype(float).to_numpy())
    y = np.log(work["Value"].astype(float).to_numpy())
    n = len(work)
    if n <= min_points:
        raise ValueError("Not enough rows for a stability series")
    slopes = np.full(n, np.nan)
    r2s = np.full(n, np.nan)
    for i in range(min_points - 1, n):
        xi, yi = x[: i + 1], y[: i + 1]
        X = np.column_stack((np.ones(i + 1), xi))
        coef, *_ = np.linalg.lstsq(X, yi, rcond=None)
        pred = X @ coef
        ss_res = float(np.sum((yi - pred) ** 2))
        ss_tot = float(np.sum((yi - yi.mean()) ** 2))
        slopes[i] = float(coef[1])
        r2s[i] = (1.0 - ss_res / ss_tot) if ss_tot > 0 else np.nan
    out = pd.DataFrame({"Date": work["Date"], "slope": slopes, "r2": r2s}).dropna()
    logging.info("Stability series %s points (%s → %s); latest b=%.4f r²=%.4f", len(out), out["Date"].min().date(), out["Date"].max().date(), float(out["slope"].iloc[-1]), float(out["r2"].iloc[-1]))
    return out


def render_stability_chart(stability, *, title, html_name, jpg_name, series_label):
    from plotly.subplots import make_subplots
    latest = stability.iloc[-1]
    date_label = pd.Timestamp(latest["Date"]).strftime("%d %B %Y")
    latest_b = float(latest["slope"])
    latest_r2 = float(latest["r2"])
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, row_heights=[0.58, 0.42], subplot_titles=("Scale Coefficient (b)", "Coefficient of Determination (r²)"))
    fig.add_trace(go.Scatter(x=stability["Date"], y=stability["slope"], mode="markers", name="b", marker=dict(color=stability["r2"], colorscale=STABILITY_COLORSCALE, cmin=STABILITY_CMIN, cmax=STABILITY_CMAX, size=5, colorbar=dict(title="r²", len=0.45, y=0.78, thickness=14)), hovertemplate="Date=%{x|%Y-%m-%d}<br>b=%{y:.4f}<br>r²=%{marker.color:.4f}<extra></extra>"), row=1, col=1)
    fig.add_trace(go.Scatter(x=stability["Date"], y=stability["r2"], mode="markers", name="r²", marker=dict(color=stability["r2"], colorscale=STABILITY_COLORSCALE, cmin=STABILITY_CMIN, cmax=STABILITY_CMAX, size=5, showscale=False), hovertemplate="Date=%{x|%Y-%m-%d}<br>r²=%{y:.4f}<extra></extra>"), row=2, col=1)
    fig.update_yaxes(title_text="Scale Coefficient (b)", row=1, col=1, showgrid=True, gridcolor="#333")
    fig.update_yaxes(title_text="r²", row=2, col=1, showgrid=True, gridcolor="#333", range=[0.60, 1.02])
    fig.update_xaxes(title_text="Date", row=2, col=1, showgrid=True, gridcolor="#333")
    fig.update_layout(title=dict(text=f"{title}<br><sup>{date_label}  ·  b = {latest_b:.4f}  ·  r² = {latest_r2:.4f}</sup>", x=0.5, xanchor="center"), template="plotly_dark", showlegend=False, hovermode="closest", annotations=[dict(xref="paper", yref="paper", x=0.98, y=0.28, xanchor="right", yanchor="top", align="left", bgcolor="rgba(20,20,20,0.75)", bordercolor="#888", borderwidth=1, font=dict(family="Arial", size=12, color="#ddd"), showarrow=False, text=(f"Scale Coefficient (b):<br>How fast is the {series_label} rising?  (y = a · x<sup>b</sup>)<br><br>Coefficient of Determination (r²):<br>What % of the series is explained by the power law?")), dict(x=1, y=-0.12, xref="paper", yref="paper", xanchor="right", text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>', showarrow=False)])
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = CHARTS_DIR / html_name
    jpg_path = CHARTS_DIR / jpg_name
    fig.write_image(str(jpg_path), width=1600, height=1100, scale=2)
    fig.write_html(str(html_path), auto_open=False)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path

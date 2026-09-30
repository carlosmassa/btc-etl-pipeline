import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

CHARTS_DIR = Path("charts")
STABILITY_MIN_POINTS = 400
STABILITY_CMIN = 0.70
STABILITY_CMAX = 1.00
STABILITY_COLORSCALE = [[0.00, "#D62728"], [0.35, "#FF7F0E"], [0.55, "#FFDD00"], [0.80, "#90EE90"], [1.00, "#2CA02C"]]


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


def _extrema_rows(stability):
    def row(metric, series, fmt):
        imax, imin = series.idxmax(), series.idxmin()
        dmax = pd.Timestamp(stability.loc[imax, "Date"]).strftime("%d %b %Y")
        dmin = pd.Timestamp(stability.loc[imin, "Date"]).strftime("%d %b %Y")
        return [metric, fmt(series.loc[imax]), dmax, fmt(series.loc[imin]), dmin]
    return [row("b", stability["slope"], lambda v: f"{float(v):.4f}"), row("r²", stability["r2"], lambda v: f"{float(v):.4f}")]


def inject_extrema_table(html_path, rows, series_label):
    thead = "<tr><th></th><th>Max</th><th>Date</th><th>Min</th><th>Date</th></tr>"
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows)
    snippet = f"""
<style>
  .plotly-graph-div {{ position: relative; }}
  #stability-note {{ position: absolute; left: 50%; bottom: 10px; transform: translateX(-50%); z-index: 20; text-align: center; background: none; border: none; padding: 0; font: 12px Arial, sans-serif; color: #b9b9b9; line-height: 1.4; }}
  #stability-note b {{ color: #ddd; font-weight: bold; }}
  #stability-credit {{ position: absolute; right: 12px; bottom: 10px; z-index: 20; font: 12px Arial, sans-serif; color: #fff; white-space: nowrap; }}
  #extrema-footer {{ position: absolute; right: 18px; bottom: 20%; z-index: 30; display: flex; flex-direction: column; align-items: flex-end; gap: 8px; }}
  #extrema-table {{ border-collapse: collapse; font: 13px Arial, sans-serif; color: #fff; }}
  #extrema-table th, #extrema-table td {{ padding: 6px 10px; text-align: center; border: 1px solid #111; background: #1C1C1C; }}
  #extrema-table th {{ background: #2A2A2A; font-weight: normal; }}
  #extrema-table td:first-child {{ background: #2A2A2A; font-weight: bold; }}
  #extrema-toggle {{ background: #2A2A2A; color: #fff; border: 1px solid #555; padding: 6px 10px; font: 12px Arial, sans-serif; cursor: pointer; }}
</style>
<script>
document.addEventListener("DOMContentLoaded", function () {{
  const gd = document.querySelector(".plotly-graph-div");
  if (!gd) return;
  const box = gd.querySelector(".plot-container") || gd;
  box.style.position = "relative";
  const footer = document.createElement("div");
  footer.id = "extrema-footer";
  footer.innerHTML = '<table id="extrema-table"><thead>{thead}</thead><tbody>{body}</tbody></table><button id="extrema-toggle">Hide table</button>';
  box.appendChild(footer);
  const note = document.createElement("div");
  note.id = "stability-note";
  note.innerHTML = '<b>Scale coefficient (b)</b> — how fast the {series_label} rises (y = a · x<sup>b</sup>)<br><b>r²</b> — share of the series explained by the power law';
  box.appendChild(note);
  const credit = document.createElement("div");
  credit.id = "stability-credit";
  credit.innerHTML = 'Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color:#fff">@CarlesMassa</a>';
  box.appendChild(credit);
  document.getElementById("extrema-toggle").addEventListener("click", function () {{
    const tbl = document.getElementById("extrema-table");
    const hidden = tbl.style.display === "none";
    tbl.style.display = hidden ? "table" : "none";
    this.textContent = hidden ? "Hide table" : "Show table";
  }});
}});
</script>
"""
    text = html_path.read_text(encoding="utf-8")
    html_path.write_text(text.replace("</body>", snippet + "\n</body>", 1) if "</body>" in text else text + snippet, encoding="utf-8")


def render_stability_chart(stability, *, title, html_name, jpg_name, series_label, show_extrema_table=False):
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
    fig.update_layout(title=dict(text=f"{title}<br><sup>{date_label}  ·  b = {latest_b:.4f}  ·  r² = {latest_r2:.4f}</sup>", x=0.5, xanchor="center"), template="plotly_dark", showlegend=False, hovermode="closest", margin=dict(t=80, b=170, l=70, r=90), annotations=[dict(xref="paper", yref="paper", x=0.5, y=-0.22, xanchor="center", yanchor="top", align="center", font=dict(family="Arial", size=12, color="#b9b9b9"), showarrow=False, text=(f"Scale coefficient (b) — how fast the {series_label} rises (y = a · x<sup>b</sup>)<br>r² — share of the series explained by the power law")), dict(x=1, y=-0.12, xref="paper", yref="paper", xanchor="right", yanchor="auto", text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>', showarrow=False)])
    extrema = _extrema_rows(stability) if show_extrema_table else []
    if show_extrema_table:
        fig.add_trace(go.Table(header=dict(values=["", "Max", "Date", "Min", "Date"], fill_color="#2A2A2A", font=dict(family="Arial", color="white", size=11), align="center", height=24), cells=dict(values=list(map(list, zip(*extrema))), fill_color="#1C1C1C", font=dict(family="Arial", color="white", size=11), align="center", height=22), domain=dict(x=[0.70, 0.995], y=[0.06, 0.20])))
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = CHARTS_DIR / html_name
    jpg_path = CHARTS_DIR / jpg_name
    fig.write_image(str(jpg_path), width=1600, height=1100, scale=2)
    if show_extrema_table:
        fig.data = tuple(tr for tr in fig.data if getattr(tr, "type", None) != "table")
    fig.update_layout(annotations=[])
    fig.write_html(str(html_path), auto_open=False)
    if show_extrema_table:
        inject_extrema_table(html_path, extrema, series_label)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path

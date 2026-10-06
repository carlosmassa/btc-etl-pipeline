"""Autocorrelation-robust health checks for the log-log power law.

R² is not used. Each date is scored on:
- expanding and 4-year rolling OLS exponents with Newey–West standard errors
- an AR(1) Cochrane–Orcutt / FGLS exponent
- the expanding 1% quantile slope (the historical floor)
- the residual process after the AR(1) component is removed
- a 30-day walk-forward pinball loss against a persisted-deviation benchmark,
  plus the hit rate of the claimed 1% floor
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import statsmodels.api as sm
from plotly.subplots import make_subplots

CHARTS_DIR = Path("charts")
MIN_POINTS = 4 * 365
STEP_DAYS = 30
ROLL_DAYS = 4 * 365
HORIZON = 30
FLOOR_Q = 0.01
HTML_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "responsive": True,
    "scrollZoom": True,
    "modeBarButtonsToAdd": [
        "drawline", "drawopenpath", "drawclosedpath", "drawcircle", "drawrect",
        "eraseshape", "v1hovermode", "togglespikelines",
    ],
}


def _hac_lags(n: int) -> int:
    return max(1, int(np.floor(4 * (n / 100.0) ** (2.0 / 9.0))))


def _pinball(y, pred, q):
    gap = np.asarray(y, dtype=float) - np.asarray(pred, dtype=float)
    return float(np.mean(np.where(gap >= 0, q * gap, (q - 1.0) * gap)))


def _ols_hac(x, y):
    design = np.column_stack((np.ones(len(x)), x))
    fit = sm.OLS(y, design).fit(cov_type="HAC", cov_kwds={"maxlags": _hac_lags(len(x))})
    return float(fit.params[0]), float(fit.params[1]), float(fit.bse[1]), fit.resid


def _fgls_ar1(x, y, resid):
    if len(resid) < 10:
        return np.nan, np.nan, np.nan, np.nan
    phi = float(np.corrcoef(resid[1:], resid[:-1])[0, 1])
    phi = float(np.clip(phi, -0.999, 0.999))
    y2 = y[1:] - phi * y[:-1]
    x2 = x[1:] - phi * x[:-1]
    design = np.column_stack((np.full(len(x2), 1.0 - phi), x2))
    beta, *_ = np.linalg.lstsq(design, y2, rcond=None)
    innov = y2 - design @ beta
    innov_sigma = float(np.std(innov, ddof=1))
    half_life = float(np.log(0.5) / np.log(abs(phi))) if 0.05 < abs(phi) < 0.999 else np.nan
    return float(beta[1]), phi, innov_sigma, half_life


def _floor_label(q):
    pct = q * 100
    return f"{pct:.1f}%" if pct < 1 else f"{pct:.0f}%"


def _quantile_slope(x, y, q):
    design = sm.add_constant(x)
    fit = sm.QuantReg(y, design).fit(q=q, max_iter=5000)
    params = fit.params
    intercept = float(params.iloc[0] if hasattr(params, "iloc") else params[0])
    slope = float(params.iloc[1] if hasattr(params, "iloc") else params[1])
    return intercept, slope


def compute_model_health(df, min_points=MIN_POINTS, step=STEP_DAYS, roll_days=ROLL_DAYS, horizon=HORIZON, floor_q=FLOOR_Q):
    work = df.dropna(subset=["Date", "Value", "ind"]).sort_values("Date").reset_index(drop=True)
    x = np.log(work["ind"].astype(float).to_numpy())
    y = np.log(work["Value"].astype(float).to_numpy())
    dates = pd.to_datetime(work["Date"])
    n = len(work)
    if n <= min_points + horizon:
        raise ValueError("Not enough rows for a model-health series")

    rows = []
    for end in range(min_points, n - horizon, step):
        x_fit, y_fit = x[:end], y[:end]
        intercept, slope, se, resid = _ols_hac(x_fit, y_fit)
        fgls_slope, phi, innov_sigma, half_life = _fgls_ar1(x_fit, y_fit, resid)
        roll_start = max(0, end - roll_days)
        _, roll_slope, roll_se, _ = _ols_hac(x[roll_start:end], y[roll_start:end])
        q_intercept, q_slope = _quantile_slope(x_fit, y_fit, floor_q)

        future = slice(end, end + horizon)
        x_fut, y_fut = x[future], y[future]
        mean_pred = intercept + slope * x_fut
        floor_pred = q_intercept + q_slope * x_fut
        last_mean_gap = float(y_fit[-1] - (intercept + slope * x_fit[-1]))
        last_floor_gap = float(y_fit[-1] - (q_intercept + q_slope * x_fit[-1]))
        naive_mean = mean_pred + last_mean_gap
        naive_floor = floor_pred + last_floor_gap

        mean_loss = _pinball(y_fut, mean_pred, 0.50)
        naive_mean_loss = _pinball(y_fut, naive_mean, 0.50)
        floor_loss = _pinball(y_fut, floor_pred, floor_q)
        naive_floor_loss = _pinball(y_fut, naive_floor, floor_q)
        breaches = y_fut < floor_pred
        breach_rate = float(np.mean(breaches))
        # longest run of closes under the claimed floor, inside this horizon
        longest = current = 0
        for flag in breaches:
            current = current + 1 if flag else 0
            longest = max(longest, current)
        resid_band = np.quantile(resid, [0.05, 0.95])
        covered = (y_fut >= mean_pred + resid_band[0]) & (y_fut <= mean_pred + resid_band[1])
        rows.append({
            "Date": dates.iloc[end - 1],
            "n": end,
            "b": slope,
            "b_se": se,
            "b_lo": slope - 1.96 * se,
            "b_hi": slope + 1.96 * se,
            "b_fgls": fgls_slope,
            "b_roll": roll_slope,
            "b_roll_se": roll_se,
            "b_floor": q_slope,
            "floor_q": floor_q,
            "phi": phi,
            "innov_sigma": innov_sigma,
            "half_life": half_life,
            "resid_mean_1y": float(np.mean(resid[-365:])) if len(resid) >= 365 else float(np.mean(resid)),
            "pinball_50": mean_loss,
            "pinball_50_naive": naive_mean_loss,
            "pinball_50_ratio": mean_loss / naive_mean_loss if naive_mean_loss > 0 else np.nan,
            "pinball_floor": floor_loss,
            "pinball_floor_naive": naive_floor_loss,
            "pinball_floor_ratio": floor_loss / naive_floor_loss if naive_floor_loss > 0 else np.nan,
            "breach_rate": breach_rate,
            "breach_run": longest,
            "coverage_90": float(np.mean(covered)),
        })
        logging.info(
            "Health %s  b=%.3f±%.3f  floor b=%.3f  φ=%.2f  floor pinball ratio=%.2f",
            dates.iloc[end - 1].date(), slope, 1.96 * se, q_slope, phi,
            rows[-1]["pinball_floor_ratio"],
        )

    out = pd.DataFrame(rows)
    out["excess_floor_loss"] = (out["pinball_floor"] - out["pinball_floor_naive"]).cumsum()
    out["floor_ratio_1y"] = out["pinball_floor_ratio"].rolling(12, min_periods=4).median()
    out["breach_rate_1y"] = out["breach_rate"].rolling(12, min_periods=4).mean()
    out["coverage_1y"] = out["coverage_90"].rolling(12, min_periods=4).mean()
    latest = out.iloc[-1]
    logging.info(
        "Health series %s points (%s → %s); latest b=%.4f HAC band [%.4f, %.4f], 1y floor pinball ratio=%.2f",
        len(out), out["Date"].min().date(), out["Date"].max().date(),
        latest["b"], latest["b_lo"], latest["b_hi"], latest["floor_ratio_1y"],
    )
    return out


def _credit():
    return dict(
        x=1, y=-0.16, xref="paper", yref="paper", xanchor="right", yanchor="auto",
        text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>',
        showarrow=False,
    )


def render_health_chart(health, *, title, html_name, jpg_name, series_label):
    latest = health.iloc[-1]
    floor_label = _floor_label(float(latest.get("floor_q", FLOOR_Q)))
    date_label = pd.Timestamp(latest["Date"]).strftime("%d %B %Y")
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.07,
        row_heights=[0.40, 0.30, 0.30],
        specs=[[{"secondary_y": False}], [{"secondary_y": True}], [{"secondary_y": True}]],
        subplot_titles=(
            "Exponent, with Newey–West 95% band",
            "Residual process after AR(1)",
            "30-day walk-forward score",
        ),
    )
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["b_hi"], mode="lines", line=dict(width=0),
        showlegend=False, hoverinfo="skip",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["b_lo"], mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(125,211,252,0.16)",
        name="Expanding b, Newey–West 95%",
        hovertemplate="Date=%{x|%d %b %Y}<br>HAC low=%{y:.3f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["b"], mode="lines", name="Expanding OLS b",
        line=dict(color="#7dd3fc", width=2.2),
        hovertemplate="Date=%{x|%d %b %Y}<br>b=%{y:.3f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["b_fgls"], mode="lines", name="FGLS AR(1) b",
        line=dict(color="#e8edf2", width=1.4, dash="dot"),
        hovertemplate="Date=%{x|%d %b %Y}<br>FGLS b=%{y:.3f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["b_roll"], mode="lines", name="4-year rolling b",
        line=dict(color="#f5a24a", width=1.8),
        hovertemplate="Date=%{x|%d %b %Y}<br>4y b=%{y:.3f}<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["b_floor"], mode="lines", name=f"Expanding {floor_label} floor b",
        line=dict(color="#00FF7F", width=1.8),
        hovertemplate="Date=%{x|%d %b %Y}<br>" + floor_label + " b=%{y:.3f}<extra></extra>",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["phi"], mode="lines", name="AR(1) φ",
        line=dict(color="#f5a24a", width=1.8),
        hovertemplate="Date=%{x|%d %b %Y}<br>φ=%{y:.2f}<br>half-life=%{customdata:.0f} days<extra></extra>",
        customdata=health["half_life"],
    ), row=2, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["innov_sigma"], mode="lines", name="Innovation σ",
        line=dict(color="#7dd3fc", width=1.8),
        hovertemplate="Date=%{x|%d %b %Y}<br>innovation σ=%{y:.3f}<extra></extra>",
    ), row=2, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["resid_mean_1y"], mode="lines", name="1y residual mean",
        line=dict(color="#e8edf2", width=1.2, dash="dot"),
        hovertemplate="Date=%{x|%d %b %Y}<br>1y residual mean=%{y:.3f}<extra></extra>",
    ), row=2, col=1, secondary_y=False)

    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["floor_ratio_1y"], mode="lines", name=f"{floor_label} pinball / naive (1y)",
        line=dict(color="#00FF7F", width=2),
        hovertemplate="Date=%{x|%d %b %Y}<br>floor pinball ratio=%{y:.2f}<extra></extra>",
    ), row=3, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["pinball_50_ratio"].rolling(12, min_periods=4).median(),
        mode="lines", name="50% pinball / naive (1y)",
        line=dict(color="#7dd3fc", width=1.5),
        hovertemplate="Date=%{x|%d %b %Y}<br>50% pinball ratio=%{y:.2f}<extra></extra>",
    ), row=3, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["breach_rate_1y"], mode="lines", name=f"{floor_label} floor breach rate (1y)",
        line=dict(color="#FF4500", width=1.5),
        hovertemplate="Date=%{x|%d %b %Y}<br>breach rate=%{y:.1%}<extra></extra>",
    ), row=3, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(
        x=health["Date"], y=health["coverage_1y"], mode="lines", name="90% band coverage (1y)",
        line=dict(color="#e0c36a", width=1.3, dash="dot"),
        hovertemplate="Date=%{x|%d %b %Y}<br>90% coverage=%{y:.0%}<extra></extra>",
    ), row=3, col=1, secondary_y=True)
    fig.add_hline(y=1.0, line_dash="dot", line_color="#666", row=3, col=1)

    fig.update_yaxes(title_text="Exponent b", row=1, col=1, showgrid=True, gridcolor="#333")
    fig.update_yaxes(title_text="φ and residual mean", row=2, col=1, secondary_y=False, showgrid=True, gridcolor="#333")
    fig.update_yaxes(title_text="Innovation σ", row=2, col=1, secondary_y=True, showgrid=False)
    fig.update_yaxes(title_text="Pinball ratio", row=3, col=1, secondary_y=False, showgrid=True, gridcolor="#333")
    fig.update_yaxes(title_text="Floor breach rate", row=3, col=1, secondary_y=True, showgrid=False, tickformat=".0%")
    fig.update_xaxes(
        title_text=(
            "Date"
            f"<br><span style='font-size:12px;color:#b9b9b9'>"
            f"{series_label}: a slope that stays inside its Newey–West band is persistence, not a break. "
            "Pinball ratio below 1 beats a forecast that just keeps the latest deviation. "
            "r² is not used."
            "</span>"
        ),
        title_standoff=8, row=3, col=1, showgrid=True, gridcolor="#333",
    )
    fig.update_layout(
        title=dict(
            text=(
                f"{title}<br><sup>{date_label}  ·  b = {latest['b']:.3f} "
                f"[{latest['b_lo']:.3f}, {latest['b_hi']:.3f}]  ·  "
                f"{floor_label} b = {latest['b_floor']:.3f}  ·  "
                f"floor pinball ratio = {latest['floor_ratio_1y']:.2f}</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark",
        legend=dict(orientation="h", y=-0.18, x=0),
        hovermode="closest",
        margin=dict(t=90, b=150, l=70, r=70),
        annotations=[_credit()],
    )
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = CHARTS_DIR / html_name
    jpg_path = CHARTS_DIR / jpg_name
    try:
        fig.write_image(str(jpg_path), width=1600, height=1200, scale=2)
    except Exception as exc:
        logging.warning("Could not write %s (%s)", jpg_path, exc)
    fig.write_html(str(html_path), auto_open=False, config=HTML_CONFIG)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path

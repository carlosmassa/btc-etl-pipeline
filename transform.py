import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import statsmodels.api as sm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BTC_CSV = Path("data/BTC_Prices.csv")
GENESIS = pd.Timestamp("2009-01-03")
USEFUL_FROM = pd.Timestamp("2010-07-18")
CHART_QUANTILES = [0.001, 0.05, 0.50, 0.90, 0.98, 0.999]
QUANTILE_LABELS = {
    0.001: "0.1%",
    0.05: "5%",
    0.50: "50%",
    0.90: "90%",
    0.98: "98%",
    0.999: "99.9%",
}
FUTURE_DAYS = 5 * 365
CHARTS_DIR = Path("charts")


def parse_price_dates(series: pd.Series) -> pd.Series:
    """Parse ISO-8601 first, then legacy DD/MM/YYYY."""
    iso = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    if iso.notna().mean() >= 0.5:
        return iso
    dmy = pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")
    if dmy.notna().mean() >= 0.5:
        return dmy
    return pd.to_datetime(series, dayfirst=True, errors="coerce")


def _coef(fit) -> tuple[float, float]:
    params = fit.params
    if hasattr(params, "iloc"):
        return float(params.iloc[0]), float(params.iloc[1])
    return float(params[0]), float(params[1])


def clean_btc(csv_path: Path = BTC_CSV) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"BTC price file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if "Date" not in df.columns or "Value" not in df.columns:
        raise ValueError(f"{csv_path} must have Date and Value columns")

    df["Date"] = parse_price_dates(df["Date"])
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df.dropna(subset=["Date", "Value"])
    df = df[df["Value"] > 0]
    df = df[df["Date"] >= USEFUL_FROM]
    df = df.drop_duplicates(subset="Date", keep="last").sort_values("Date").reset_index(drop=True)
    df["ind"] = (df["Date"] - GENESIS).dt.days
    df = df[df["ind"] > 0].reset_index(drop=True)

    if df.empty:
        raise ValueError("No usable BTC rows after cleaning")

    logging.info("Cleaned %s BTC rows (%s → %s)", len(df), df["Date"].min().date(), df["Date"].max().date())
    return df


def fit_power_law(df: pd.DataFrame) -> dict:
    X = np.log(df["ind"].astype(float))
    y = np.log(df["Value"].astype(float))
    X_with_const = sm.add_constant(X)

    fits = {}
    for q in CHART_QUANTILES:
        fits[q] = sm.QuantReg(y, X_with_const).fit(q=q)
        intercept, slope = _coef(fits[q])
        logging.info("Quantile %s: intercept=%.4f slope=%.4f prsquared=%.3f", QUANTILE_LABELS[q], intercept, slope, fits[q].prsquared)

    work = df.copy()
    for q, label in QUANTILE_LABELS.items():
        work[f"QuantRegPredict_{label}"] = fits[q].predict(X_with_const)
        work[f"LinearReg_{label}"] = np.exp(work[f"QuantRegPredict_{label}"])

    future_dates = pd.date_range(work["Date"].max() + pd.Timedelta(days=1), periods=FUTURE_DAYS, freq="D")
    future = pd.DataFrame({"Date": future_dates})
    future["ind"] = (future["Date"] - GENESIS).dt.days
    X_future = sm.add_constant(np.log(future["ind"].astype(float)))
    for q, label in QUANTILE_LABELS.items():
        future[f"QuantRegPredict_{label}"] = fits[q].predict(X_future)
        future[f"LinearReg_{label}"] = np.exp(future[f"QuantRegPredict_{label}"])

    combined = pd.concat([work, future], ignore_index=True)

    latest = work.iloc[-1]
    latest_price = float(latest["Value"])
    fair_value = float(latest["LinearReg_50%"])
    percent_change = ((latest_price - fair_value) / fair_value) * 100
    price_quantile = estimate_price_quantile(y, X_with_const, latest_price)

    intercept, slope = _coef(fits[0.50])
    logging.info("50%% equation: Price = exp(%.6f * ln(days since genesis) + %.6f)", slope, intercept)
    logging.info(
        "Latest price %.2f is %.2f%% %s 50%% fair value (q≈%s)",
        latest_price,
        abs(percent_change),
        "above" if percent_change >= 0 else "below",
        f"{price_quantile * 100:.2f}%",
    )

    return {
        "df": work,
        "combined": combined,
        "fits": fits,
        "latest_date": pd.Timestamp(latest["Date"]),
        "latest_price": latest_price,
        "fair_value": fair_value,
        "percent_change": percent_change,
        "price_quantile": price_quantile,
        "band": {
            "q001": float(latest["LinearReg_0.1%"]),
            "q05": float(latest["LinearReg_5%"]),
            "q50": fair_value,
            "q90": float(latest["LinearReg_90%"]),
            "q98": float(latest["LinearReg_98%"]),
            "q999": float(latest["LinearReg_99.9%"]),
        },
    }


def estimate_price_quantile(
    y: pd.Series,
    X_with_const,
    latest_price: float,
    lo: float = 0.001,
    hi: float = 0.999,
    max_iter: int = 16,
) -> float:
    """Binary-search the quantile whose fitted value at the last row matches latest_price."""
    target = np.log(latest_price)
    x_last = X_with_const.iloc[[-1]] if hasattr(X_with_const, "iloc") else X_with_const[-1:]

    def pred_at(q: float) -> float:
        fit = sm.QuantReg(y, X_with_const).fit(q=q, max_iter=5000)
        pred = fit.predict(x_last)
        return float(pred.iloc[0] if hasattr(pred, "iloc") else pred[0])

    low_pred = pred_at(lo)
    high_pred = pred_at(hi)
    if target <= low_pred:
        return lo
    if target >= high_pred:
        return hi

    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        mid_pred = pred_at(mid)
        if mid_pred < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def render_chart(model: dict) -> tuple[Path, Path]:
    df = model["df"]
    combined = model["combined"]
    latest_date = model["latest_date"]
    latest_price = model["latest_price"]
    percent_change = model["percent_change"]
    band = model["band"]
    price_quantile = model["price_quantile"]

    is_above = percent_change >= 0
    change_type = "above" if is_above else "below"
    n_points = f"{len(df):,.0f}"
    date_label = latest_date.strftime("%d %B %Y")
    q_label = f"{price_quantile * 100:.2f}%"

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Value"], mode="lines", name="Price", line=dict(color="orange", width=3)))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_50%"], mode="lines", name="50% Quantile", line=dict(color="cyan", width=2)))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_0.1%"], fill=None, mode="lines", line=dict(color="#A8D800", width=1), name="0.1% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_5%"], fill="tonexty", mode="lines", line=dict(color="#00FF7F", width=0), name="5% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_90%"], fill=None, mode="lines", line=dict(color="#00BFFF", width=1), name="90% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_98%"], fill="tonexty", mode="lines", line=dict(color="#87CEFA", width=1), name="98% Quantile", showlegend=False))
    fig.add_trace(go.Scatter(x=combined["Date"], y=combined["LinearReg_99.9%"], fill="tonexty", mode="lines", line=dict(color="#FF4500", width=0), name="99.9% Quantile", showlegend=False))

    annotations = [
        dict(
            xref="paper", yref="paper", x=0.40, y=-0.05,
            xanchor="center", yanchor="top",
            text=(
                f"Chart Date: {date_label} ({n_points} Data Points)"
                f"<br>Latest Price: {latest_price:,.0f} ({abs(percent_change):,.2f}% {change_type} Fair Value); {q_label} Quantile"
                f"<br>Fair Value: ${band['q50']:,.0f} (50% Quantile)"
            ),
            font=dict(family="Arial", size=12, color="rgb(150,150,150)"),
            align="left",
            showarrow=False,
        ),
        dict(
            xref="paper", yref="paper", x=0.65, y=-0.05,
            xanchor="center", yanchor="top",
            text=(
                f"98% to 99.9% Quantile ({band['q98']:,.0f} - {band['q999']:,.0f})"
                f"<br>90% to 98% Quantile ({band['q90']:,.0f} - {band['q98']:,.0f})"
                f"<br>0.1% to 5% Quantile ({band['q001']:,.0f} - {band['q05']:,.0f})"
            ),
            font=dict(family="Arial", size=12, color="rgb(150,150,150)"),
            align="left",
            showarrow=False,
        ),
        dict(
            x=1, y=-0.12,
            text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>',
            showarrow=False, xref="paper", yref="paper",
            xanchor="right", yanchor="auto", xshift=0, yshift=0,
        ),
    ]

    fig.update_layout(
        title="Power Law Probability Channel",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        yaxis_type="log",
        hovermode="closest",
        annotations=annotations,
        showlegend=True,
        legend_orientation="h",
        template="plotly_dark",
    )
    fig.update_yaxes(showgrid=False)

    config = {
        "modeBarButtonsToAdd": [
            "drawline", "drawopenpath", "drawcircle", "drawrect",
            "eraseshape", "v1hovermode", "togglespikelines",
        ]
    }

    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = CHARTS_DIR / "btc_usd_chart.html"
    jpg_path = CHARTS_DIR / "btc_usd_chart.jpg"
    fig.write_html(str(html_path), auto_open=False, config=config)
    fig.write_image(str(jpg_path), width=1600, height=900, scale=2)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path


def transform_data() -> None:
    df = clean_btc()
    model = fit_power_law(df)
    render_chart(model)


if __name__ == "__main__":
    try:
        transform_data()
    except Exception as exc:
        logging.error("Transformation failed: %s", exc)
        raise

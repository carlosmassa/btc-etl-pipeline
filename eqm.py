"""EQM ribbon and rainbow on the 3.4-year moving-average risk.

Ribbon signal: long while EMA40(risk) > EMA80(risk). No price filter.
These charts are not posted to X.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

CHARTS_DIR = Path("charts")
GENESIS = pd.Timestamp("2009-01-03")
SMA_WINDOW = 1344
RANK_WINDOW = 1095
EMA_FAST = 40
EMA_SLOW = 80
RIBBON_SPANS = (40, 48, 56, 64, 72, 80)
PF_SPAN = 150
PF_LOOKBACK = 10
EXEC_LAG = 1
COST_BPS = 10.0
RAINBOW_WINDOW = 1242  # 3.4 years, the author's stated optimal look-back
RAINBOW_QS = [0.001, 0.01, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.99, 0.999]
RAINBOW_BANDS = [
    (0.001, 0.01, "#1d4ed8"),
    (0.01, 0.10, "#2563eb"),
    (0.10, 0.20, "#0891b2"),
    (0.20, 0.30, "#0d9488"),
    (0.30, 0.40, "#16a34a"),
    (0.40, 0.50, "#65a30d"),
    (0.50, 0.60, "#ca8a04"),
    (0.60, 0.70, "#d97706"),
    (0.70, 0.80, "#ea580c"),
    (0.80, 0.90, "#dc2626"),
    (0.90, 0.99, "#b91c1c"),
    (0.99, 0.999, "#7f1d1d"),
]
HTML_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "responsive": True,
    "scrollZoom": True,
    "modeBarButtons": [[
        "toImage", "zoom2d", "pan2d", "select2d", "lasso2d",
        "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d",
        "hoverClosestCartesian", "hoverCompareCartesian", "toggleSpikelines",
        "drawline", "drawopenpath", "drawclosedpath", "drawcircle", "drawrect",
        "eraseshape",
    ]],
}


def _credit():
    return dict(
        x=1, y=-0.12, xref="paper", yref="paper", xanchor="right",
        text='Chart by: <a href="https://x.com/CarlesMassa" target="_blank" style="color: white;">@CarlesMassa</a>',
        showarrow=False,
    )


def _write(fig, html_name, jpg_name, width=1600, height=1000):
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path, jpg_path = CHARTS_DIR / html_name, CHARTS_DIR / jpg_name
    try:
        fig.write_image(str(jpg_path), width=width, height=height, scale=2)
    except Exception as exc:
        logging.warning("Could not write %s (%s)", jpg_path, exc)
    fig.write_html(str(html_path), auto_open=False, config=HTML_CONFIG)
    logging.info("Wrote %s and %s", html_path, jpg_path)
    return html_path, jpg_path


def _close_frame(df):
    work = df.dropna(subset=["Date", "Value"]).sort_values("Date").reset_index(drop=True)
    work = work[work["Value"] > 0]
    close = pd.Series(work["Value"].to_numpy(float), index=pd.to_datetime(work["Date"]), name="close")
    close = close[~close.index.duplicated(keep="last")].sort_index()
    full = pd.date_range(close.index[0], close.index[-1], freq="D")
    return close.reindex(full).interpolate().ffill().bfill()


def compute_mayer(df, window=RAINBOW_WINDOW):
    """Ribbon on the 3.4-year EQM risk. Long while EMA40 > EMA80. No price filter."""
    close = _close_frame(df)
    ma = close.rolling(window, min_periods=window).mean()
    ratio = close / ma
    n = len(close)
    risk_np = np.full(n, np.nan)
    ratio_np = ratio.to_numpy()
    for s in range(window - 1, n):
        sample = ratio_np[s + 1 - window: s + 1]
        sample = sample[~np.isnan(sample)]
        if sample.size < window // 2:
            continue
        risk_np[s] = np.searchsorted(np.sort(sample), ratio_np[s], side="right") / sample.size
    risk = pd.Series(risk_np * 100.0, index=close.index, name="risk")
    spans = pd.concat([risk.ewm(span=sp, adjust=True, min_periods=1).mean() for sp in RIBBON_SPANS], axis=1)
    lo, hi = spans.min(axis=1), spans.max(axis=1)
    ef = risk.ewm(span=EMA_FAST, adjust=True, min_periods=1).mean()
    es = risk.ewm(span=EMA_SLOW, adjust=True, min_periods=1).mean()
    on = ef > es
    regime = pd.Series(np.where(on.fillna(False).to_numpy(), "long", "cash"), index=close.index, name="regime")
    logging.info(
        "EQM ribbon %s days; risk=%.1f regime=%s filter=off",
        n, float(risk.dropna().iloc[-1]), regime.iloc[-1],
    )
    return {
        "close": close, "sma": ma, "risk": risk, "lo": lo, "hi": hi,
        "regime": regime, "gap": ef - es, "price_up": pd.Series(True, index=close.index),
    }


def _risk_to_price(model, s, q):
    if not np.isfinite(q):
        return np.nan
    close = model["close"].to_numpy()
    ma = model["sma"].to_numpy()
    w = close[max(0, s + 1 - RAINBOW_WINDOW): s + 1] / ma[max(0, s + 1 - RAINBOW_WINDOW): s + 1]
    w = w[np.isfinite(w)]
    if w.size == 0 or not np.isfinite(ma[s]):
        return np.nan
    return float(ma[s] * np.quantile(w, float(np.clip(q, 0.0, 1.0))))


def _map_band(model, levels):
    pos = {d: i for i, d in enumerate(model["close"].index)}
    return pd.Series([_risk_to_price(model, pos[d], v / 100.0) for d, v in levels.items()], index=levels.index)


def _runs(regime):
    values = regime.to_numpy()
    runs, start = [], 0
    for i in range(1, len(values)):
        if values[i] != values[start]:
            runs.append((regime.index[start], regime.index[i - 1], values[start]))
            start = i
    runs.append((regime.index[start], regime.index[-1], values[start]))
    return runs


def trade_log(model):
    """Trades on the signal date, so the table matches the chart markers."""
    close, regime, risk = model["close"], model["regime"], model["risk"]
    pos = (regime == "long").astype(int)
    rows, entry = [], None
    for i in range(1, len(pos)):
        if pos.iloc[i] == 1 and pos.iloc[i - 1] == 0:
            entry = (pos.index[i], float(close.iloc[i]), float(risk.asof(pos.index[i])))
        elif pos.iloc[i] == 0 and pos.iloc[i - 1] == 1 and entry:
            rows.append({
                "entry": entry[0], "exit": pos.index[i], "days": (pos.index[i] - entry[0]).days,
                "entry_px": entry[1], "exit_px": float(close.iloc[i]),
                "ret": float(close.iloc[i]) / entry[1] - 1 - 2 * COST_BPS / 1e4,
                "risk_in": entry[2], "open": False,
            })
            entry = None
    if entry:
        rows.append({
            "entry": entry[0], "exit": close.index[-1], "days": (close.index[-1] - entry[0]).days,
            "entry_px": entry[1], "exit_px": float(close.iloc[-1]),
            "ret": float(close.iloc[-1]) / entry[1] - 1 - COST_BPS / 1e4,
            "risk_in": entry[2], "open": True,
        })
    return pd.DataFrame(rows)


def backtest(model):
    close, regime = model["close"], model["regime"]
    start = close.index[RAINBOW_WINDOW - 1]
    pos = (regime == "long").astype(float).shift(EXEC_LAG).fillna(0.0).loc[start:]
    px = close.loc[pos.index]
    ret = px.pct_change().fillna(0.0)
    turn = pos.diff().abs().fillna(pos.iloc[0])
    strat = pos * ret - turn * COST_BPS / 1e4
    out = pd.DataFrame({
        "equity": (1.0 + strat).cumprod(),
        "buy_hold": (1.0 + ret).cumprod(),
        "position": pos,
        "strategy": strat,
    })
    out.attrs["switches"] = int((turn > 0).sum())
    return out


WINDOW_GRID = (365, 500, 730, 900, 1000, 1100, 1242, 1344, 1460, 1600, 1825)


def _best_window(close, end, grid=WINDOW_GRID):
    """Window that maximises the power-law R² of the trailing MA, using only past data."""
    days = (close.index - GENESIS).days.to_numpy(float)
    px = close.to_numpy(float)
    best_w, best_r2 = grid[0], -np.inf
    for w in grid:
        if end + 1 < w:
            continue
        ma = pd.Series(px).rolling(w).mean().iloc[end + 1 - w: end + 1].to_numpy()
        d = days[end + 1 - w: end + 1]
        mask = np.isfinite(ma) & (ma > 0) & (d > 0)
        if mask.sum() < w // 2:
            continue
        x = np.log(d[mask])
        y = np.log(ma[mask])
        coef = np.polyfit(x, y, 1)
        pred = np.polyval(coef, x)
        ss_res = ((y - pred) ** 2).sum()
        ss_tot = ((y - y.mean()) ** 2).sum()
        r2 = 1 - ss_res / ss_tot if ss_tot else -np.inf
        if r2 > best_r2:
            best_w, best_r2 = w, r2
    return best_w


def compute_rainbow(df, window=RAINBOW_WINDOW):
    """Walk-forward EQM rainbow from a trailing moving average.

    The trend is a simple moving average. The standardized price is the
    ratio of price to that average. Band levels are the empirical
    quantiles of the ratio inside the same trailing window, mapped back
    to price. No log transform, one parameter, no look-ahead.
    """
    close = _close_frame(df)
    ma = close.rolling(window, min_periods=window).mean()
    ratio = close / ma
    n = len(close)
    qs = np.asarray(RAINBOW_QS)
    bands = np.full((n, len(qs)), np.nan)
    risk = np.full(n, np.nan)
    ratio_np = ratio.to_numpy()
    ma_np = ma.to_numpy()
    for s in range(window - 1, n):
        sample = ratio_np[s + 1 - window: s + 1]
        sample = sample[~np.isnan(sample)]
        if sample.size < window // 2:
            continue
        ordered = np.sort(sample)
        bands[s] = ma_np[s] * np.quantile(ordered, qs)
        risk[s] = np.searchsorted(ordered, ratio_np[s], side="right") / ordered.size
    out = pd.DataFrame(bands, index=close.index, columns=[f"q{q:g}" for q in qs])
    out["close"] = close
    out["median"] = out["q0.5"]
    out["risk"] = risk
    logging.info(
        "EQM rainbow %s days; risk=%.1f%% median=$%.0f",
        n, 100 * risk[-1], out["median"].iloc[-1],
    )
    return out


def _regime_shapes(index, regime, yref):
    shapes = []
    for start, end, state in _runs(regime.reindex(index).fillna("cash")):
        shapes.append(dict(
            type="rect", x0=start, x1=end, y0=0, y1=1, yref=yref,
            fillcolor="rgba(245,162,74,0.16)" if state == "long" else "rgba(125,211,252,0.06)",
            line=dict(width=0), layer="below",
        ))
    return shapes


def _switches(model, idx):
    regime = model["regime"].reindex(idx)
    close = model["close"].reindex(idx)
    rows = []
    prev = regime.iloc[0]
    for i in range(1, len(regime)):
        state = regime.iloc[i]
        if state != prev:
            rows.append({
                "date": regime.index[i],
                "price": float(close.iloc[i]),
                "to": state,
                "label": "Long" if state == "long" else "Cash",
            })
            prev = state
    return pd.DataFrame(rows)


def _ema_filter(close):
    ema = close.ewm(span=PF_SPAN, adjust=True, min_periods=1).mean()
    log_ema = np.log(ema)
    slope = log_ema.diff(PF_LOOKBACK)
    daily = log_ema.diff(1)
    last_slope = float(slope.iloc[-1])
    last_daily = float(daily.iloc[-1])
    if last_daily == 0 or np.sign(last_daily) == np.sign(last_slope):
        days = None
    else:
        days = abs(last_slope / last_daily)
    return {
        "ema": ema,
        "slope": slope,
        "daily": daily,
        "last_slope": last_slope,
        "last_daily": last_daily,
        "days": days,
        "rising": last_slope > 0,
    }


def _filter_note(state):
    direction = "rising" if state["rising"] else "falling"
    rate = state["last_daily"] * 100
    if state["days"] is None:
        timing = "not heading toward a flip"
    else:
        timing = f"filter flips in ~{state['days']:.0f} days at this rate"
    return f"EMA150 {direction} {rate:+.2f}%/day · {timing}"


def _swing_divs(close, slope, order=20):
    px = close.to_numpy(float)
    sl = slope.to_numpy(float)
    idx = close.index
    lows, highs = [], []
    for i in range(order, len(px) - order):
        window = px[i - order: i + order + 1]
        if px[i] == window.min():
            lows.append(i)
        if px[i] == window.max():
            highs.append(i)
    lines = []
    for a, b in zip(lows, lows[1:]):
        if px[b] < px[a] and sl[b] > sl[a]:
            lines.append(("bull", idx[a], px[a], idx[b], px[b]))
    for a, b in zip(highs, highs[1:]):
        if px[b] > px[a] and sl[b] < sl[a]:
            lines.append(("bear", idx[a], px[a], idx[b], px[b]))
    return lines
    direction = "rising" if state["rising"] else "falling"
    rate = state["last_daily"] * 100
    if state["days"] is None:
        timing = "not heading toward a flip"
    else:
        timing = f"filter flips in ~{state['days']:.0f} days at this rate"
    return f"EMA150 {direction} {rate:+.2f}%/day · {timing}"


def render_ribbon(model, *, html_name, jpg_name, months=None):
    end = model["close"].index[-1]
    if months is None:
        start = model["close"].index[max(SMA_WINDOW, RANK_WINDOW) - 1]
    else:
        start = end - pd.DateOffset(months=months)
    idx = model["close"].index[model["close"].index >= start]
    lo_p = _map_band(model, model["lo"].loc[idx])
    hi_p = _map_band(model, model["hi"].loc[idx])
    latest = model["close"].iloc[-1]
    risk = model["risk"].iloc[-1]
    regime = model["regime"].iloc[-1]
    since = _runs(model["regime"])[-1][0]
    filt = _ema_filter(model["close"])
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        subplot_titles=("EQM ribbon on risk", "EQM ribbon mapped onto price"),
    )
    fig.add_trace(go.Scatter(x=idx, y=model["hi"].loc[idx], mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=idx, y=model["lo"].loc[idx], mode="lines", line=dict(width=0), fill="tonexty",
        fillcolor="rgba(224,195,106,0.22)", name="Ribbon band", hoverinfo="skip",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=idx, y=model["risk"].loc[idx], mode="lines", name="EQM risk",
        line=dict(color="#f8fafc", width=1.5),
        hovertemplate="Date=%{x|%d %b %Y}<br>risk=%{y:.1f}%<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(x=idx, y=hi_p, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=idx, y=lo_p, mode="lines", line=dict(width=0), fill="tonexty",
        fillcolor="rgba(224,195,106,0.18)", showlegend=False, hoverinfo="skip",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=idx, y=model["close"].loc[idx], mode="lines", name="Price",
        line=dict(color="#f5a24a", width=1.7),
        hovertemplate="Date=%{x|%d %b %Y}<br>price=$%{y:,.0f}<extra></extra>",
    ), row=2, col=1)
    switches = _switches(model, idx)
    if len(switches):
        longs = switches[switches["to"] == "long"]
        cash = switches[switches["to"] == "cash"]
        fig.add_trace(go.Scatter(
            x=longs["date"], y=longs["price"], mode="markers+text", name="Switch to long",
            marker=dict(color="#34d399", size=8, symbol="triangle-up", line=dict(color="#052e16", width=1)),
            text=[f"${p:,.0f}" for p in longs["price"]], textposition="top center",
            textfont=dict(size=10, color="#34d399"),
            hovertemplate="Switch to long<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=cash["date"], y=cash["price"], mode="markers+text", name="Switch to cash",
            marker=dict(color="#f87171", size=8, symbol="triangle-down", line=dict(color="#450a0a", width=1)),
            text=[f"${p:,.0f}" for p in cash["price"]], textposition="bottom center",
            textfont=dict(size=10, color="#f87171"),
            hovertemplate="Switch to cash<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
        ), row=2, col=1)
    fig.update_yaxes(title_text="EQM risk (%)", range=[0, 100], row=1, col=1, gridcolor="#333")
    fig.update_yaxes(title_text="Price (USD)", type="log", row=2, col=1, gridcolor="#333")
    fig.update_xaxes(title_text="Date", row=2, col=1, gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTCUSD EQM Mayer Ribbon<br><sup>"
                f"{end:%d %B %Y}  ·  ${latest:,.0f}  ·  risk {risk:.1f}%  ·  "
                f"{regime.upper()} since {since:%d %b %Y}</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.14, x=0),
        margin=dict(t=90, b=90, l=70, r=40),
        shapes=_regime_shapes(idx, model["regime"], "paper"),
        annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=1000)


def render_ribbon_price(model, *, html_name, jpg_name, months=None, risk_series=None):
    end = model["close"].index[-1]
    if months is None:
        start = model["close"].index[max(SMA_WINDOW, RANK_WINDOW) - 1]
    else:
        start = end - pd.DateOffset(months=months)
    idx = model["close"].index[model["close"].index >= start]
    lo_p = _map_band(model, model["lo"].loc[idx])
    hi_p = _map_band(model, model["hi"].loc[idx])
    latest = model["close"].iloc[-1]
    risk_line = risk_series.reindex(model["close"].index) if risk_series is not None else model["risk"]
    risk = float(risk_line.dropna().iloc[-1])
    if risk_series is not None:
        risk *= 100
    regime = model["regime"].iloc[-1]
    since = _runs(model["regime"])[-1][0]
    since_px = float(model["close"].loc[since])
    since_ret = latest / since_px - 1 if since_px else np.nan
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=idx, y=hi_p, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=idx, y=lo_p, mode="lines", line=dict(width=0), fill="tonexty",
        fillcolor="rgba(224,195,106,0.22)", name="Ribbon band", hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=idx, y=model["close"].loc[idx], mode="lines", name="Price",
        line=dict(color="#f5a24a", width=1.7),
        customdata=risk_line.loc[idx] * (100 if risk_series is not None else 1),
        hovertemplate="Date=%{x|%d %b %Y}<br>price=$%{y:,.0f}<br>risk=%{customdata:.1f}%<extra></extra>",
    ))
    switches = _switches(model, idx)
    if len(switches):
        longs = switches[switches["to"] == "long"]
        cash = switches[switches["to"] == "cash"]
        fig.add_trace(go.Scatter(
            x=longs["date"], y=longs["price"], mode="markers+text", name="Switch to long",
            marker=dict(color="#34d399", size=8, symbol="triangle-up", line=dict(color="#052e16", width=1)),
            text=[f"${p:,.0f}" for p in longs["price"]], textposition="top center",
            textfont=dict(size=10, color="#34d399"),
            hovertemplate="Switch to long<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=cash["date"], y=cash["price"], mode="markers+text", name="Switch to cash",
            marker=dict(color="#f87171", size=8, symbol="triangle-down", line=dict(color="#450a0a", width=1)),
            text=[f"${p:,.0f}" for p in cash["price"]], textposition="bottom center",
            textfont=dict(size=10, color="#f87171"),
            hovertemplate="Switch to cash<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
        ))
    fig.update_yaxes(title_text="Price (USD)", type="log", gridcolor="#333")
    fig.update_xaxes(title_text="Date", gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTCUSD EQM Mayer Ribbon — price<br><sup>"
                f"{end:%d %B %Y}  ·  ${latest:,.0f}  ·  risk {risk:.1f}%  ·  "
                f"{regime.upper()} since {since:%d %b %Y} ({since_ret:+.1%})</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40),
        shapes=_regime_shapes(idx, model["regime"], "paper"),
        annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=900)


ETF_DATE = pd.Timestamp("2024-01-11")
CYCLE_BOTTOMS = [
    pd.Timestamp("2015-01-14"),
    pd.Timestamp("2018-12-15"),
    pd.Timestamp("2022-11-21"),
]


def _kpis(bt, trades):
    eq, rets = bt["equity"], bt["strategy"]
    years = max((eq.index[-1] - eq.index[0]).days / 365.25, 1 / 365.25)
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / years) - 1
    vol = rets.std() * np.sqrt(365)
    down = rets[rets < 0].std() * np.sqrt(365)
    max_dd = (eq / eq.cummax() - 1).min()
    closed = trades[~trades["open"]] if len(trades) else trades
    wins = closed[closed["ret"] > 0] if len(closed) else closed
    losses = closed[closed["ret"] <= 0] if len(closed) else closed
    avg_win = float(wins["ret"].mean()) if len(wins) else np.nan
    avg_loss = float(losses["ret"].mean()) if len(losses) else np.nan
    return {
        "Strategy": "EQM",
        "CAGR": cagr,
        "Sharpe": rets.mean() * 365 / vol if vol else np.nan,
        "Sortino": rets.mean() * 365 / down if down else np.nan,
        "Calmar": cagr / abs(max_dd) if max_dd else np.nan,
        "Max DD": max_dd,
        "Win rate": float((closed["ret"] > 0).mean()) if len(closed) else np.nan,
        "R:R": abs(avg_win / avg_loss) if avg_loss and avg_loss != 0 else np.nan,
        "Trades": int(len(closed)),
        "Exposure": float(bt["position"].mean()),
        "sep": False,
    }


def _bh_kpis(bt):
    eq = bt["buy_hold"]
    rets = eq.pct_change().fillna(0.0)
    years = max((eq.index[-1] - eq.index[0]).days / 365.25, 1 / 365.25)
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / years) - 1
    vol = rets.std() * np.sqrt(365)
    down = rets[rets < 0].std() * np.sqrt(365)
    max_dd = (eq / eq.cummax() - 1).min()
    return {
        "Strategy": "Buy & hold",
        "CAGR": cagr,
        "Sharpe": rets.mean() * 365 / vol if vol else np.nan,
        "Sortino": rets.mean() * 365 / down if down else np.nan,
        "Calmar": cagr / abs(max_dd) if max_dd else np.nan,
        "Max DD": max_dd,
        "Win rate": np.nan,
        "R:R": np.nan,
        "Trades": "—",
        "Exposure": 1.0,
        "sep": False,
    }


def render_ribbon_report(model, *, html_name, jpg_name):
    tl = trade_log(model)
    bt = backtest(model)
    periods = [("Full history", bt.index[0], bt.index[-1])]
    bottoms = [d for d in CYCLE_BOTTOMS if d >= bt.index[0]]
    edges = [*bottoms, bt.index[-1]]
    for i in range(len(edges) - 1):
        periods.append((f"Cycle {edges[i]:%b %Y}–{edges[i + 1]:%b %Y}", edges[i], edges[i + 1]))
    periods.append(("Since BTC ETF", ETF_DATE, bt.index[-1]))

    kpi_rows = []
    for i, (name, start, end) in enumerate(periods):
        window = bt.loc[start:end]
        if len(window) < 30:
            continue
        if kpi_rows:
            kpi_rows.append({"Period": "────────", "Strategy": "────────", "CAGR": np.nan, "Sharpe": np.nan, "Sortino": np.nan, "Calmar": np.nan, "Max DD": np.nan, "Win rate": np.nan, "R:R": np.nan, "Trades": "────────", "Exposure": np.nan, "sep": True})
        trades = tl[(tl["entry"] >= start) & (tl["entry"] <= end)] if len(tl) else tl
        row = _kpis(window, trades)
        row["Period"] = name
        kpi_rows.append(row)
        bh = _bh_kpis(window)
        bh["Period"] = name
        kpi_rows.append(bh)
    kpis = pd.DataFrame(kpi_rows)

    def fmt(value, better, kind):
        if pd.isna(value):
            text = "—"
        elif kind == "pct":
            text = f"{value:.1%}"
        else:
            text = f"{value:.2f}"
        return f"<b>{text}</b>" if better else text

    pairs = []
    for i in range(0, len(kpis), 1):
        pass
    rows = []
    i = 0
    while i < len(kpis):
        if kpis.iloc[i]["sep"]:
            rows.append(kpis.iloc[i].to_dict())
            i += 1
            continue
        eqm = kpis.iloc[i]
        bh = kpis.iloc[i + 1]
        for metric, kind, higher in [
            ("CAGR", "pct", True), ("Sharpe", "num", True), ("Sortino", "num", True),
            ("Calmar", "num", True), ("Max DD", "pct", True),
        ]:
            eqm[f"{metric}_txt"] = fmt(eqm[metric], eqm[metric] >= bh[metric], kind)
            bh[f"{metric}_txt"] = fmt(bh[metric], bh[metric] > eqm[metric], kind)
        for metric in ["Win rate", "R:R", "Exposure"]:
            eqm[f"{metric}_txt"] = "—" if pd.isna(eqm[metric]) else (f"{eqm[metric]:.1%}" if metric != "R:R" else f"{eqm[metric]:.2f}")
            bh[f"{metric}_txt"] = "—" if pd.isna(bh[metric]) else (f"{bh[metric]:.1%}" if metric != "R:R" else f"{bh[metric]:.2f}")
        eqm["Trades_txt"] = str(eqm["Trades"])
        bh["Trades_txt"] = "—"
        rows.append(eqm.to_dict())
        rows.append(bh.to_dict())
        i += 2
    show = pd.DataFrame(rows)
    seps = show["sep"].tolist()
    trades = tl.sort_values("entry", ascending=False).copy() if len(tl) else tl
    fig = make_subplots(
        rows=2, cols=1, vertical_spacing=0.08, row_heights=[0.48, 0.52],
        specs=[[{"type": "table"}], [{"type": "table"}]],
        subplot_titles=("EQM Mayer KPIs", "EQM Mayer trades"),
    )

    def blank(v, sep):
        return "" if sep else v

    fig.add_trace(go.Table(
        header=dict(values=["Period", "Strategy", "CAGR", "Sharpe", "Sortino", "Calmar", "Max DD", "Win rate", "R:R", "Trades", "Exposure"],
                    fill_color="#1b2129", font=dict(color="white", size=16), align="center"),
        cells=dict(
            values=[
                ["" if s or strat == "Buy & hold" else v for s, v, strat in zip(seps, show["Period"], show["Strategy"])],
                [blank(v, s) for s, v in zip(seps, show["Strategy"])],
                [blank(v, s) for s, v in zip(seps, show.get("CAGR_txt", show["CAGR"]))],
                [blank(v, s) for s, v in zip(seps, show.get("Sharpe_txt", show["Sharpe"]))],
                [blank(v, s) for s, v in zip(seps, show.get("Sortino_txt", show["Sortino"]))],
                [blank(v, s) for s, v in zip(seps, show.get("Calmar_txt", show["Calmar"]))],
                [blank(v, s) for s, v in zip(seps, show.get("Max DD_txt", show["Max DD"]))],
                [blank(v, s) for s, v in zip(seps, show.get("Win rate_txt", show["Win rate"]))],
                [blank(v, s) for s, v in zip(seps, show.get("R:R_txt", show["R:R"]))],
                [blank(v, s) for s, v in zip(seps, show.get("Trades_txt", show["Trades"]))],
                [blank(v, s) for s, v in zip(seps, show.get("Exposure_txt", show["Exposure"]))],
            ],
            fill_color=[["#2c333c" if s else ("#1a2330" if strat == "Buy & hold" else "#14181e") for s, strat in zip(show["sep"], show["Strategy"])]],
            font=dict(color="#e8edf2", size=15), align="center", height=32,
        ),
    ), row=1, col=1)
    if len(trades):
        fig.add_trace(go.Table(
            header=dict(
                values=["Entry", "Exit", "Days", "Gain/loss", "Entry price", "Exit price", "Risk in", "Status"],
                fill_color="#1b2129", font=dict(color="white", size=12), align="center",
            ),
            cells=dict(
                values=[
                    trades["entry"].dt.strftime("%d %b %Y"),
                    trades["exit"].dt.strftime("%d %b %Y"),
                    trades["days"],
                    trades["ret"].map(lambda v: f"{v:+.1%}"),
                    trades["entry_px"].map(lambda v: f"${v:,.0f}"),
                    trades["exit_px"].map(lambda v: f"${v:,.0f}"),
                    trades["risk_in"].map(lambda v: f"{v:.0f}%"),
                    trades["open"].map(lambda v: "Open" if v else "Closed"),
                ],
                fill_color=[["#14181e" if not o else "#1f2a22" for o in trades["open"]]],
                font=dict(color="#e8edf2", size=11), align="center", height=24,
            ),
        ), row=2, col=1)
    fig.update_layout(
        title=dict(
            text="BTCUSD EQM Mayer — trades and KPIs<br><sup>Cycles are bottom to bottom · ETF era from 11 Jan 2024 · signal-date prices, 10 bps per switch</sup>",
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", margin=dict(t=80, b=40, l=20, r=20),
        annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, width=1400, height=1100)


def render_trades(model, *, html_name, jpg_name, since="2015-01-01"):
    tl = trade_log(model)
    tl = tl[tl["entry"] >= pd.Timestamp(since)].copy()
    if tl.empty:
        raise ValueError("No EQM trades to plot")
    closed = tl[~tl["open"]]
    fig = go.Figure()
    wins = closed[closed["ret"] > 0]
    losses = closed[closed["ret"] <= 0]
    fig.add_trace(go.Scatter(
        x=wins["days"], y=wins["risk_in"], mode="markers", name="Winning trade",
        marker=dict(size=np.clip(np.abs(wins["ret"]) * 80, 10, 48), color="#6DAF9B"),
        text=[f"{r.entry:%b %Y}<br>{r.ret:+.0%} · {r.days:.0f}d" for r in wins.itertuples()],
        hovertemplate="%{text}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=losses["days"], y=losses["risk_in"], mode="markers", name="Losing trade",
        marker=dict(size=np.clip(np.abs(losses["ret"]) * 120, 12, 48), color="#C77D8E"),
        text=[f"{r.entry:%b %Y}<br>{r.ret:+.0%} · {r.days:.0f}d" for r in losses.itertuples()],
        hovertemplate="%{text}<extra></extra>",
    ))
    open_ = tl[tl["open"]]
    if len(open_):
        fig.add_trace(go.Scatter(
            x=open_["days"], y=open_["risk_in"], mode="markers", name="Open long",
            marker=dict(size=16, color="#7dd3fc", symbol="star"),
            text=[f"Open {r.entry:%b %Y}<br>{r.ret:+.0%} · {r.days:.0f}d · risk {r.risk_in:.0f}%" for r in open_.itertuples()],
            hovertemplate="%{text}<extra></extra>",
        ))
    fig.update_layout(
        title=dict(
            text="BTCUSD EQM Ribbon — long trades<br><sup>Bubble size is the trade return · 3.4-year EQM risk · EMA40/80</sup>",
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        xaxis_title="Trade duration (days)", yaxis_title="EQM risk at entry (%)",
        legend=dict(orientation="h", y=-0.16, x=0),
        margin=dict(t=90, b=90, l=70, r=40),
        annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=800)


def render_backtest(model, *, html_name, jpg_name):
    bt = backtest(model)
    years = (bt.index[-1] - bt.index[0]).days / 365.25
    cagr = (bt["equity"].iloc[-1] / bt["equity"].iloc[0]) ** (1 / years) - 1
    vol = bt["strategy"].std() * np.sqrt(365)
    sharpe = bt["strategy"].mean() * 365 / vol if vol else np.nan
    max_dd = (bt["equity"] / bt["equity"].cummax() - 1).min()
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.68, 0.32], vertical_spacing=0.06)
    fig.add_trace(go.Scatter(x=bt.index, y=bt["buy_hold"], mode="lines", name="Buy & hold", line=dict(color="#9aa6b2", width=1.4)), row=1, col=1)
    fig.add_trace(go.Scatter(x=bt.index, y=bt["equity"], mode="lines", name="EQM ribbon", line=dict(color="#f5a24a", width=1.8)), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=bt.index, y=bt["equity"] / bt["equity"].cummax() - 1, mode="lines", name="Strategy drawdown",
        line=dict(color="#C77D8E", width=1.2),
    ), row=2, col=1)
    fig.update_yaxes(title_text="Growth of 1", type="log", row=1, col=1, gridcolor="#333")
    fig.update_yaxes(title_text="Drawdown", tickformat=".0%", row=2, col=1, gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTCUSD EQM Ribbon backtest<br><sup>"
                f"{bt.index[0]:%b %Y} – {bt.index[-1]:%b %Y}  ·  "
                f"CAGR {cagr:.1%}  ·  Sharpe {sharpe:.2f}  ·  max DD {max_dd:.1%}  ·  "
                f"{bt.attrs['switches']} switches  ·  no price filter</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest", legend=dict(orientation="h", y=-0.12, x=0),
        margin=dict(t=90, b=80, l=70, r=40), annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=900)


def _draw_rainbow(fig, df, row):
    for q0, q1, color in RAINBOW_BANDS:
        label = f"{q0:.1%}–{q1:.1%}"
        fig.add_trace(go.Scatter(
            x=df.index, y=df[f"q{q1:g}"], mode="lines", line=dict(width=0),
            name=f"{q1:.1%} band", showlegend=False,
            hovertemplate=f"{q1:.1%} band<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ), row=row, col=1)
        fig.add_trace(go.Scatter(
            x=df.index, y=df[f"q{q0:g}"], mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor=color, name=label, opacity=0.85,
            hovertemplate=f"{q0:.1%} band<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ), row=row, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["median"], mode="lines", name="50% band",
        line=dict(color="white", width=1.4),
        hovertemplate="50% band<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ), row=row, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["close"], mode="lines", name="Price",
        line=dict(color="#f8fafc", width=1.6),
        customdata=df["risk"] * 100,
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<br>risk=%{customdata:.1f}%<extra></extra>",
    ), row=row, col=1)


def render_rainbow(rainbow, *, html_name, jpg_name, start="2012-01-01"):
    df = rainbow.loc[start:].dropna(subset=["q0.5"])
    last = rainbow.dropna(subset=["risk"]).iloc[-1]
    fig = go.Figure()
    _draw_rainbow_figure = make_subplots(rows=1, cols=1)
    _draw_rainbow(_draw_rainbow_figure, df, 1)
    for trace in _draw_rainbow_figure.data:
        fig.add_trace(trace)
    fig.update_yaxes(type="log", title_text="Price (USD)", gridcolor="#333")
    fig.update_xaxes(title_text="Date", gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTCUSD EQM Rainbow — full history<br><sup>"
                f"{rainbow.index[-1]:%d %B %Y}  ·  ${last['close']:,.0f}  ·  "
                f"risk {last['risk'] * 100:.1f}%</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest", legend=dict(orientation="h", y=-0.16, x=0),
        margin=dict(t=100, b=90, l=70, r=40), annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=900)


def render_rainbow_risk(rainbow, *, html_name, jpg_name):
    df = rainbow.dropna(subset=["risk"]).copy()
    last = df.iloc[-1]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df.index, y=df["risk"] * 100, mode="lines", name="EQM risk",
        line=dict(color="#7dd3fc", width=1.6),
        customdata=df["close"],
        hovertemplate=(
            "Date=%{x|%d %b %Y}<br>risk=%{y:.1f}%<br>price=$%{customdata:,.0f}<extra></extra>"
        ),
    ))
    fig.update_yaxes(title_text="EQM risk (%)", range=[0, 100], gridcolor="#333")
    fig.update_xaxes(title_text="Date", gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTCUSD EQM Risk — full history<br><sup>"
                f"{df.index[-1]:%d %B %Y}  ·  risk {last['risk'] * 100:.1f}%  ·  "
                f"${last['close']:,.0f}</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.14, x=0),
        margin=dict(t=90, b=80, l=70, r=40), annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=700)


def render_rainbow_zoom(rainbow, *, html_name, jpg_name, months=12):
    end = rainbow.index[-1]
    df = rainbow.loc[end - pd.DateOffset(months=months):].dropna(subset=["q0.5"])
    last = rainbow.dropna(subset=["risk"]).iloc[-1]
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.68, 0.32], vertical_spacing=0.05)
    _draw_rainbow(fig, df, 1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["risk"] * 100, mode="lines", name="EQM risk",
        line=dict(color="#7dd3fc", width=1.6),
        hovertemplate="Date=%{x|%d %b %Y}<br>risk=%{y:.1f}%<extra></extra>",
    ), row=2, col=1)
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1, gridcolor="#333")
    fig.update_yaxes(title_text="EQM risk (%)", range=[0, 100], row=2, col=1, gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTCUSD EQM Rainbow — last 12 months<br><sup>"
                f"{end:%d %B %Y}  ·  ${last['close']:,.0f}  ·  risk {last['risk'] * 100:.1f}%</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest", legend=dict(orientation="h", y=-0.14, x=0),
        margin=dict(t=90, b=90, l=70, r=40), annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=1000)


def render_eqm_fits(rainbow, btc, *, html_name, jpg_name, future_days=365):
    """Raw EQM 1/50/99 bands (solid) and their log-log fits (dashed), full history."""
    import statsmodels.api as sm

    df = rainbow.dropna(subset=["q0.01", "q0.5", "q0.99"]).copy()
    work = btc.dropna(subset=["Date", "Value"]).sort_values("Date").reset_index(drop=True)
    work = work[work["Value"] > 0].copy()
    work["Date"] = pd.to_datetime(work["Date"])
    price = work.set_index("Date")["Value"]
    price = price[~price.index.duplicated(keep="last")].sort_index()
    end = df.index.max()
    future = pd.date_range(end + pd.Timedelta(days=1), periods=future_days, freq="D")
    all_dates = pd.concat([pd.Series(df.index), pd.Series(future)], ignore_index=True)
    all_ind = (all_dates - GENESIS).dt.days.astype(float)
    X_all = sm.add_constant(np.log(all_ind))
    fits = {}
    for col in ("q0.01", "q0.5", "q0.99"):
        series = df[col].astype(float)
        mask = series > 0
        x = (series.index[mask] - GENESIS).days.astype(float)
        y = np.log(series[mask].to_numpy())
        X = sm.add_constant(np.log(x))
        fits[col] = np.exp(sm.OLS(y, X).fit().predict(X_all))
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=price.index, y=price, mode="lines", name="BTC price",
        line=dict(color="#f8fafc", width=1.5),
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ))
    for col, color, label in (
        ("q0.99", "#f87171", "EQM 99% (raw)"),
        ("q0.5", "#fbbf24", "EQM 50% fair (raw)"),
        ("q0.01", "#60a5fa", "EQM 1% (raw)"),
    ):
        fig.add_trace(go.Scatter(
            x=df.index, y=df[col], mode="lines", name=label,
            line=dict(color=color, width=1.6),
            hovertemplate=f"{label}<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=all_dates[all_dates <= end], y=fits[col][all_dates <= end],
            mode="lines", name=f"{label} log-log fit",
            line=dict(color=color, width=1.4, dash="dash"),
            hovertemplate=f"{label} fit<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=all_dates[all_dates >= end], y=fits[col][all_dates >= end],
            mode="lines", name=f"{label} projection",
            line=dict(color=color, width=1.4, dash="dash"), showlegend=False,
            hovertemplate=f"{label} projection<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ))
    last = {c: float(fits[c][all_dates <= end].iloc[-1]) for c in fits}
    fig.update_yaxes(type="log", title_text="Price (USD, log)", gridcolor="#333")
    fig.update_xaxes(title_text="Date", gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "EQM prices and log-log fits<br><sup>"
                f"{end:%d %B %Y}  ·  99% fit ${last['q0.99']:,.0f}  ·  "
                f"50% fit ${last['q0.5']:,.0f}  ·  1% fit ${last['q0.01']:,.0f}</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.14, x=0),
        margin=dict(t=90, b=90, l=70, r=40),
        annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=900)
    """Power-law 0.1%/99% quantile lines against the walk-forward EQM 1%/99% bands."""
    import statsmodels.api as sm

    work = btc.dropna(subset=["Date", "Value"]).sort_values("Date").reset_index(drop=True)
    work = work[work["Value"] > 0].copy()
    work["Date"] = pd.to_datetime(work["Date"])
    work["ind"] = (work["Date"] - GENESIS).dt.days
    X = sm.add_constant(np.log(work["ind"].astype(float)))
    y = np.log(work["Value"].astype(float))
    fits = {q: sm.QuantReg(y, X).fit(q=q, max_iter=5000) for q in (0.001, 0.99)}
    end = work["Date"].max()
    future = pd.date_range(end + pd.Timedelta(days=1), periods=future_days, freq="D")
    all_dates = pd.concat([work["Date"], pd.Series(future)], ignore_index=True)
    all_ind = (all_dates - GENESIS).dt.days
    X_all = sm.add_constant(np.log(all_ind.astype(float)))
    pl = {q: np.exp(fits[q].predict(X_all)) for q in fits}
    df = rainbow.dropna(subset=["q0.01", "q0.99"])
    price = work.set_index("Date")["Value"]
    price = price[~price.index.duplicated(keep="last")].sort_index()
    ath_i, low_i = price.idxmax(), price.idxmin()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=price.index, y=price, mode="lines", name="BTC price",
        line=dict(color="#f8fafc", width=1.5),
        hovertemplate="Price<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ))
    for q, color, label in ((0.99, "#60a5fa", "Power-law 99%"), (0.001, "#1d4ed8", "Power-law 0.1%")):
        fig.add_trace(go.Scatter(
            x=all_dates[all_dates <= end], y=pl[q][all_dates <= end], mode="lines", name=label,
            line=dict(color=color, width=1.6),
            hovertemplate=f"{label}<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=all_dates[all_dates >= end], y=pl[q][all_dates >= end], mode="lines", name=f"{label} projection",
            line=dict(color=color, width=1.6, dash="dash"), showlegend=False,
            hovertemplate=f"{label} projection<br>%{{x|%d %b %Y}}<br>$%{{y:,.0f}}<extra></extra>",
        ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["q0.99"], mode="lines", name="EQM 99% (walk-forward)",
        line=dict(color="#4ade80", width=1.6),
        hovertemplate="EQM 99%<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["q0.01"], mode="lines", name="EQM 1% (walk-forward)",
        line=dict(color="#15803d", width=1.6),
        hovertemplate="EQM 1%<br>%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>",
    ))
    fig.add_annotation(
        x=ath_i, y=float(price.loc[ath_i]), text=f"ATH ${price.loc[ath_i]/1000:.0f}k",
        showarrow=True, arrowhead=2, ax=30, ay=-30, font=dict(color="#f8fafc", size=11),
    )
    fig.add_annotation(
        x=low_i, y=float(price.loc[low_i]), text=f"low ${price.loc[low_i]/1000:.0f}k",
        showarrow=True, arrowhead=2, ax=30, ay=30, font=dict(color="#f8fafc", size=11),
    )
    last_pl99 = float(pl[0.99][all_dates <= end].iloc[-1])
    last_pl01 = float(pl[0.001][all_dates <= end].iloc[-1])
    last_eq99 = float(df["q0.99"].iloc[-1])
    last_eq01 = float(df["q0.01"].iloc[-1])
    fig.update_yaxes(type="log", title_text="Price (USD, log)", gridcolor="#333")
    fig.update_xaxes(title_text="Date", gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTC Valuation Envelope — Power-law 0.1/99%, EQM 1/99%<br><sup>"
                f"{end:%d %B %Y}  ·  power-law 99% ${last_pl99:,.0f}  ·  EQM 99% ${last_eq99:,.0f}  ·  "
                f"power-law 0.1% ${last_pl01:,.0f}  ·  EQM 1% ${last_eq01:,.0f}</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.14, x=0),
        margin=dict(t=90, b=90, l=70, r=40),
        annotations=fig.layout.annotations + (_credit(),),
    )
    return _write(fig, html_name, jpg_name, height=900)

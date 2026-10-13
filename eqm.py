"""EQM ribbon and rainbow charts for the daily ETL.

Adapted from the Mayer-ribbon and empirical-quantile rainbow models.
The pipeline only stores a daily close, so high = low = close and the
hybrid risk equals the close-only rank. The ribbon is always run with
the 150-day price-trend filter on. These charts are not posted to X.
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


def compute_mayer(df):
    close = _close_frame(df)
    sma = close.rolling(SMA_WINDOW, min_periods=1).mean()
    mc = (close / sma).to_numpy()
    n = len(close)
    asym = np.empty(n)
    for i in range(n):
        w = mc[max(0, i + 1 - RANK_WINDOW): i + 1]
        asym[i] = ((w < mc[i]).sum() + 0.5 * (w == mc[i]).sum()) / w.size * 100.0
    risk = pd.Series(asym, index=close.index, name="risk")
    spans = pd.concat([risk.ewm(span=sp, adjust=True, min_periods=1).mean() for sp in RIBBON_SPANS], axis=1)
    lo, hi = spans.min(axis=1), spans.max(axis=1)
    ef = risk.ewm(span=EMA_FAST, adjust=True, min_periods=1).mean()
    es = risk.ewm(span=EMA_SLOW, adjust=True, min_periods=1).mean()
    price_up = (np.log(close.ewm(span=PF_SPAN, adjust=True, min_periods=1).mean()).diff(PF_LOOKBACK) > 0).fillna(False)
    on = (ef > es) & price_up
    regime = pd.Series(np.where(on.to_numpy(), "long", "cash"), index=close.index, name="regime")
    logging.info(
        "EQM ribbon %s days; risk=%.1f regime=%s filter=%s",
        n, float(risk.iloc[-1]), regime.iloc[-1], "on" if bool(price_up.iloc[-1]) else "off",
    )
    return {
        "close": close, "sma": sma, "risk": risk, "lo": lo, "hi": hi,
        "regime": regime, "gap": ef - es, "price_up": price_up,
    }


def _risk_to_price(model, s, q):
    mc = (model["close"] / model["sma"]).to_numpy()
    w = mc[max(0, s + 1 - RANK_WINDOW): s + 1]
    return float(model["sma"].iloc[s] * np.quantile(w, float(np.clip(q, 0, 1))))


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
    close, regime, risk = model["close"], model["regime"], model["risk"]
    pos = (regime == "long").astype(int).shift(EXEC_LAG).fillna(0).astype(int)
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
    start = close.index[max(SMA_WINDOW, RANK_WINDOW) - 1]
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


def compute_rainbow(df, burn_in=730, resid_window=1460):
    close = _close_frame(df)
    y = np.log(close.to_numpy())
    x = np.log((close.index - GENESIS).days.to_numpy(float))
    n = len(close)
    cx, cy = np.cumsum(x), np.cumsum(y)
    cxx, cxy = np.cumsum(x * x), np.cumsum(x * y)
    k = np.arange(1, n + 1, dtype=float)
    den = k * cxx - cx ** 2
    with np.errstate(invalid="ignore", divide="ignore"):
        b_all = np.where(den > 0, (k * cxy - cx * cy) / den, np.nan)
        a_all = (cy - b_all * cx) / k
    qs = np.asarray(RAINBOW_QS)
    bands = np.full((n, len(qs)), np.nan)
    risk = np.full(n, np.nan)
    for s in range(burn_in, n):
        j = max(0, s + 1 - resid_window)
        r = y[j: s + 1] - (a_all[s] + b_all[s] * x[j: s + 1])
        fit = a_all[s] + b_all[s] * x[s]
        bands[s] = np.exp(fit + np.quantile(r, qs))
        risk[s] = np.searchsorted(np.sort(r), y[s] - fit, side="right") / r.size
    out = pd.DataFrame(bands, index=close.index, columns=[f"q{q:g}" for q in qs])
    out["close"] = close
    out["median"] = out["q0.5"]
    out["risk"] = risk
    logging.info("EQM rainbow %s days; risk=%.1f%% median=$%.0f", n, 100 * risk[-1], out["median"].iloc[-1])
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
                f"{regime.upper()} since {since:%d %b %Y}  ·  EMA150 price filter on</sup>"
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


def render_ribbon_price(model, *, html_name, jpg_name, months=None):
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
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=idx, y=hi_p, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=idx, y=lo_p, mode="lines", line=dict(width=0), fill="tonexty",
        fillcolor="rgba(224,195,106,0.22)", name="Ribbon band", hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=idx, y=model["close"].loc[idx], mode="lines", name="Price",
        line=dict(color="#f5a24a", width=1.7),
        customdata=model["risk"].loc[idx],
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
                f"{regime.upper()} since {since:%d %b %Y}  ·  EMA150 price filter on</sup>"
            ),
            x=0.5, xanchor="center",
        ),
        template="plotly_dark", hovermode="closest",
        legend=dict(orientation="h", y=-0.14, x=0),
        margin=dict(t=90, b=90, l=70, r=40),
        shapes=_regime_shapes(idx, model["regime"], "paper"),
        annotations=[_credit()],
    )
    return _write(fig, html_name, jpg_name, height=800)


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
        "CAGR": cagr,
        "Sharpe": rets.mean() * 365 / vol if vol else np.nan,
        "Sortino": rets.mean() * 365 / down if down else np.nan,
        "Calmar": cagr / abs(max_dd) if max_dd else np.nan,
        "Max DD": max_dd,
        "Win rate": float((closed["ret"] > 0).mean()) if len(closed) else np.nan,
        "R:R": abs(avg_win / avg_loss) if avg_loss and avg_loss != 0 else np.nan,
        "Trades": int(len(closed)),
        "Exposure": float(bt["position"].mean()),
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
    for name, start, end in periods:
        window = bt.loc[start:end]
        if len(window) < 30:
            continue
        trades = tl[(tl["entry"] >= start) & (tl["entry"] <= end)] if len(tl) else tl
        row = _kpis(window, trades)
        row["Period"] = name
        kpi_rows.append(row)
    kpis = pd.DataFrame(kpi_rows)

    def pct(v):
        return "—" if pd.isna(v) else f"{v:.1%}"

    def num(v):
        return "—" if pd.isna(v) else f"{v:.2f}"

    trades = tl.sort_values("entry").copy() if len(tl) else tl
    fig = make_subplots(
        rows=2, cols=1, vertical_spacing=0.08, row_heights=[0.42, 0.58],
        specs=[[{"type": "table"}], [{"type": "table"}]],
        subplot_titles=("EQM Mayer KPIs", "EQM Mayer trades"),
    )
    fig.add_trace(go.Table(
        header=dict(values=["Period", "CAGR", "Sharpe", "Sortino", "Calmar", "Max DD", "Win rate", "R:R", "Trades", "Exposure"],
                    fill_color="#1b2129", font=dict(color="white", size=12), align="center"),
        cells=dict(
            values=[
                kpis["Period"], kpis["CAGR"].map(pct), kpis["Sharpe"].map(num),
                kpis["Sortino"].map(num), kpis["Calmar"].map(num), kpis["Max DD"].map(pct),
                kpis["Win rate"].map(pct), kpis["R:R"].map(num), kpis["Trades"], kpis["Exposure"].map(pct),
            ],
            fill_color="#14181e", font=dict(color="#e8edf2", size=11), align="center", height=26,
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
            text="BTCUSD EQM Mayer — trades and KPIs<br><sup>Cycles are bottom to bottom · ETF era from 11 Jan 2024 · 1-day lag, 10 bps per switch</sup>",
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
            text="BTCUSD EQM Ribbon — long trades<br><sup>Bubble size is the trade return · EMA150 price filter on · close-only risk</sup>",
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
    fig.add_trace(go.Scatter(x=bt.index, y=bt["equity"], mode="lines", name="EQM Mayer + filter", line=dict(color="#f5a24a", width=1.8)), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=bt.index, y=bt["equity"] / bt["equity"].cummax() - 1, mode="lines", name="Strategy drawdown",
        line=dict(color="#C77D8E", width=1.2),
    ), row=2, col=1)
    fig.update_yaxes(title_text="Growth of 1", type="log", row=1, col=1, gridcolor="#333")
    fig.update_yaxes(title_text="Drawdown", tickformat=".0%", row=2, col=1, gridcolor="#333")
    fig.update_layout(
        title=dict(
            text=(
                "BTCUSD EQM Mayer backtest<br><sup>"
                f"{bt.index[0]:%b %Y} – {bt.index[-1]:%b %Y}  ·  "
                f"CAGR {cagr:.1%}  ·  Sharpe {sharpe:.2f}  ·  max DD {max_dd:.1%}  ·  "
                f"{bt.attrs['switches']} switches  ·  EMA150 filter on</sup>"
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

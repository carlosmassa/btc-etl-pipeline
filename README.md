# btc-etl-pipeline

Daily Bitcoin power-law probability channel.

- Live chart: https://carlosmassa.github.io/btc-etl-pipeline/charts/btc_usd_chart.html
- Source prices: CoinMetrics `PriceUSD`, stored in `data/BTC_Prices.csv` (ISO-8601 dates)
- Daily post on X from `@CarlesMassa` when the production workflow runs on `main`

## What runs in production (`main`)

| UTC | Workflow | Effect |
|---|---|---|
| 03:00 | `update_btc_prices.yml` | Append new CoinMetrics rows to `data/BTC_Prices.csv` |
| 04:00 | `daily-etl.yml` | Fit the channel, write HTML/JPG, update gh-pages, post to X |

Scheduled jobs only run on the default branch. `dev` does not tweet and does not overwrite the live chart.

Gold ingest is retired. `data/LBMA-gold_D-gold_D_USD_PM.csv` is kept as historical data only.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/update_btc_price_coinmetrics.py   # optional incremental refresh
python transform.py
python load.py
```

Outputs:

- `charts/btc_usd_chart.html`
- `charts/btc_usd_chart.jpg`

X posting needs `X_API_KEY`, `X_API_SECRET`, `X_ACCESS_TOKEN`, and `X_ACCESS_TOKEN_SECRET`.

## How the model works

1. `clean_btc()` reads `data/BTC_Prices.csv`, drops missing/non-positive prices, keeps dates from 2010-07-18, and sets `ind` to calendar days since the genesis block (2009-01-03).
2. `fit_power_law()` fits log-log quantile regression at 0.1%, 5%, 50%, 90%, 98%, and 99.9%, then projects those bands five years forward.
3. Current quantile label is found with binary search (~16 extra fits), not a 0.001 grid.
4. `render_chart()` writes the dark Plotly HTML and a 1600×900 JPG.

## Dry-run on `dev`

See [TESTING.md](TESTING.md). Short version: Actions → Daily BTC/USD ETL Pipeline → Run workflow → branch `dev` → leave **publish_to_production** unchecked → download the `pipeline-output` artifact.

Production publish happens only on `main`, and only for the scheduled run or a manual run with that checkbox enabled.

## Secrets

| Secret | Used by |
|---|---|
| `X_API_KEY` / `X_API_SECRET` / `X_ACCESS_TOKEN` / `X_ACCESS_TOKEN_SECRET` | `post_to_x.py` |
| `X_BEARER_TOKEN` | unused by current poster; safe to keep |

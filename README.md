# btc-etl-pipeline

Daily Bitcoin power-law probability channels.

- BTC/USD chart: https://carlosmassa.github.io/btc-etl-pipeline/charts/btc_usd_chart.html
- BTCUSD/GOLD chart: https://carlosmassa.github.io/btc-etl-pipeline/charts/btc_gold_ratio_chart.html
- Index: https://carlosmassa.github.io/btc-etl-pipeline/charts/
- Prices: CoinMetrics `PriceUSD` in `data/BTC_Prices.csv` and LBMA Gold PM in `data/LBMA-gold_D-gold_D_USD_PM.csv`
- Daily X posts from `@CarlesMassa` when the production workflow runs on `main`

## What runs in production (`main`)

| UTC | Workflow | Effect |
|---|---|---|
| 03:00 | `update_btc_prices.yml` | Append CoinMetrics rows to `data/BTC_Prices.csv` |
| 04:00 | `daily-etl.yml` | Fit both channels, write HTML/JPG, update gh-pages, post both images to X |
| 22:00 | `update_LBMA_Gold_PM_prices.yml` | Append LBMA Gold PM rows |

Scheduled jobs only run on the default branch. `dev` does not tweet and does not overwrite the live charts.

The BTCUSD/GOLD series is `BTC_USD / Gold_USD_per_oz` (ounces of gold per bitcoin). Gold weekends are forward-filled from the last LBMA PM print.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/update_btc_price_coinmetrics.py
python scripts/update_gold_price_MetalsDev.py   # needs METALS_DEV_API_KEY
python transform.py
python load.py
```

Outputs in `charts/`:

- `btc_usd_chart.html` / `.jpg`
- `btc_gold_ratio_chart.html` / `.jpg`
- `index.html`

## Dry-run on `dev`

See [TESTING.md](TESTING.md). Run the daily ETL workflow on branch `dev` with **publish_to_production** unchecked, then download `pipeline-output`.

## Secrets

| Secret | Used by |
|---|---|
| `X_API_KEY` / `X_API_SECRET` / `X_ACCESS_TOKEN` / `X_ACCESS_TOKEN_SECRET` | `post_to_x.py` |
| `METALS_DEV_API_KEY` | gold price updater |

# Testing on `dev` without touching production

Production is `main`. The daily X posts and live charts come only from `main`.

- BTC/USD: https://carlosmassa.github.io/btc-etl-pipeline/charts/btc_usd_chart.html
- BTCUSD/GOLD: https://carlosmassa.github.io/btc-etl-pipeline/charts/btc_gold_ratio_chart.html

## Dry-run both charts

1. Open https://github.com/carlosmassa/btc-etl-pipeline/actions/workflows/daily-etl.yml
2. **Run workflow**
3. Use branch **dev**
4. Leave **publish_to_production** unchecked
5. Logs should say `MODE=dry-run`
6. Download **pipeline-output** and compare both JPGs / HTMLs with the live pages

That run cannot post to X and cannot change gh-pages.

Optional: run **Daily LBMA Gold PM Prices** on `dev` to refresh gold on this branch only. It will not write to `main`.

# Testing on `dev` without touching production

Production is `main`. The daily X post and the live chart come only from `main`.

Live chart: https://carlosmassa.github.io/btc-etl-pipeline/charts/btc_usd_chart.html

## What is protected

- GitHub **schedule** triggers only run on the default branch (`main`). `dev` will not tweet at 4:00 UTC by itself.
- On `dev`, **Post chart to X** and **Commit chart to gh-pages** are skipped.
- Those two steps run only when the workflow runs on `main` **and** it is either the daily schedule or a manual run with **publish_to_production** checked.

## Dry-run the chart pipeline

1. Open https://github.com/carlosmassa/btc-etl-pipeline/actions/workflows/daily-etl.yml
2. **Run workflow**
3. Use branch **dev**
4. Leave **publish_to_production** unchecked
5. Wait for the job. Logs should say `MODE=dry-run`.
6. Download the **pipeline-output** artifact (JPG + HTML) and compare it with the live chart.

That run cannot post to X and cannot change gh-pages.

## Promote to production

When the artifact looks right:

1. Open a pull request `dev` → `main`
2. Merge
3. The next scheduled job on `main` (04:00 UTC) uses the new code and publishes as usual

If you ever need an immediate production publish after merge: run the same workflow on **main** and check **publish_to_production**.

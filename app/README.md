# Streamlit demo app

Reads the Databricks gold table (actual rolling skater totals) and the dbt
baseline mart (per-player season expectations) and renders the
live-vs-expected comparison per skater and stat.

Two data sources are configured via environment variables:

- `GOLD_TABLE_URI` — Delta gold table path or Databricks SQL table, e.g.
  `s3://bucket/gold/skater_totals`
- `BASELINE_TABLE_ID` — BigQuery baseline table, e.g.
  `project.dataset.baseline_skater_stats`

## Run

```bash
pip install -r requirements.txt
streamlit run app/app.py -- --game-id 2025030212
```

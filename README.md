# NHL Live vs. Historical Stats Pipeline

## What This Is

A streaming data engineering pipeline that compares a player's simulated "live" in-game performance against their historical baseline. Historical NHL play-by-play data is replayed with artificial delays through Kafka to mimic a live game feed — this is not real-time data, it's historical data made to behave like a stream. The project is parameterized by game ID, so any past game can be run through the pipeline.

This is one pipeline, built around streaming. A historical baseline (skater season stats) is prepared ahead of time in BigQuery/dbt — it's reference data the pipeline reads from at the comparison step, not a second pipeline.

**Scope**: skaters only (no goalies). Stats tracked: **goals, assists, shots**.

- **Prep (one-time/offline)**: historical skater stats scraped from Hockey Reference, landed in S3, loaded into BigQuery, transformed with dbt (staging → intermediate → marts) into a baseline table with season averages and percentile rank for each stat.
- **Streaming pipeline (the core build)**: historical play-by-play events for a given game replayed via a Kafka producer, consumed by Databricks Structured Streaming for deduplication and rolling per-skater totals, written to Delta Lake on S3 in a bronze/silver/gold medallion pattern.
- **Join/output**: compares the live rolling goals/assists/shots against the dbt historical baseline for a given skater, surfaced via an optional Streamlit app.

## What It Does

1. (Prep) Loads historical skater stats into BigQuery and models them with dbt into clean marts (season average + percentile rank for goals, assists, shots) — done once, not per run.
2. Pulls historical play-by-play for a given `game_id`.
3. Kafka producer replays the play-by-play for that game, simulating live event arrival.
4. Databricks Structured Streaming consumes the stream, dedupes events, and computes rolling per-skater totals for goals, assists, and shots, written as Delta tables (bronze → silver → gold).
5. A join step pulls the current rolling totals (gold) and the dbt historical baseline, and outputs a live-vs-expected comparison per skater, per stat.

## Actual Data Flow

This flow produces the skater's observed, in-game goals/assists/shots for a selected `game_id`:

1. **Fetch play-by-play** — `ingestion/fetch_game.py` uses the NHL API to retrieve the game's play-by-play JSON.
2. **Store raw events** — the raw API response is stored in AWS S3, with local copies kept under `data/raw/nhl/` for development and testing.
3. **Publish a simulated live feed** — a Python Kafka producer reads the historical events and publishes them to Kafka with artificial delays.
4. **Process the stream** — Databricks Structured Streaming reads the Kafka events, filters to goal/shot events, removes duplicates, and calculates rolling goals, assists, and shots per skater.
5. **Persist the results** — Delta Lake writes the processed data to AWS S3 in bronze, silver, and gold tables. The gold table contains the current actual in-game totals per skater.
6. **Prepare the comparison** — the join step reads the gold table and sends the actual skater totals to the live-vs-expected output. Streamlit can display the result.

## Expected Data Flow

This flow produces the historical baseline used to estimate what a skater's goals, assists, and shots would typically look like:

1. **Collect historical statistics** — the Hockey Reference scraper collects skater game logs.
2. **Land the source data** — AWS S3 stores the scraped historical data as the batch source.
3. **Load the warehouse** — BigQuery loads the historical records for querying and transformation.
4. **Build the baseline** — dbt models the BigQuery data through staging, intermediate, and marts layers to produce, per skater, a season average and percentile rank for goals, assists, and shots.
5. **Prepare the comparison** — the join step reads the dbt historical baseline and matches it to the skater and game in the actual data. Streamlit can display the resulting live-vs-expected comparison.

## Tools

- **Kafka** — simulates live event ingestion
- **Databricks (Structured Streaming, Delta Lake)** — stream processing, medallion architecture
- **AWS S3** — raw data lake storage, Delta table storage
- **BigQuery** — historical stats warehouse
- **dbt** — historical data transformation/modeling
- **Streamlit** (optional) — UI for the live-vs-historical comparison
- **Kubernetes** (optional) — containerizing producer/consumer jobs

## Project Structure

```text
NHL-game-stat-pipeline/
├── README.md
├── PROJECT_CONTEXT.md
├── LICENSE
├── .gitignore
│
├── data/
│   ├── raw/nhl/                 # downloaded API responses
│   ├── extracted/               # flattened event records
│   └── sample/                  # small local datasets and game IDs
│
├── ingestion/
│   ├── fetch_game.py
│   └── extract_plays.py
│
├── streaming/
│   ├── producer/                # Kafka producer
│   └── schemas/                 # shared event schemas
│
├── databricks/                  # bronze, silver, and gold jobs
├── batch/
│   ├── hockey_reference/
│   └── bigquery/
├── dbt/                         # staging, intermediate, and marts models
├── join/                        # live-versus-historical comparison
├── app/                         # optional Streamlit UI
└── tests/
```

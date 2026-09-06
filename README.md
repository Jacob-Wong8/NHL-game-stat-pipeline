# NHL Live vs. Historical Stats Pipeline

## What This Is

A streaming data engineering pipeline that compares a player's simulated "live" in-game performance against their historical baseline. Historical NHL play-by-play data is replayed with artificial delays through Kafka to mimic a live game feed — this is not real-time data, it's historical data made to behave like a stream. The project is parameterized by game ID, so any past game can be run through the pipeline.

This is one pipeline, built around streaming. A historical baseline (player season stats) is prepared ahead of time in BigQuery/dbt — it's reference data the pipeline reads from at the comparison step, not a second pipeline.

- **Prep (one-time/offline)**: historical player/game stats scraped from Hockey Reference, landed in S3, loaded into BigQuery, transformed with dbt (staging → intermediate → marts) into a baseline table.
- **Streaming pipeline (the core build)**: historical play-by-play events for a given game replayed via a Kafka producer, consumed by Databricks Structured Streaming for deduplication and rolling aggregates, written to Delta Lake on S3 in a bronze/silver/gold medallion pattern.
- **Join/output**: compares the live rolling aggregate against the dbt historical baseline for a given player, surfaced via an optional Streamlit app.

## What It Does

1. (Prep) Loads historical stats into BigQuery and models them with dbt into clean marts (e.g. player season averages) — done once, not per run.
2. Pulls historical play-by-play for a given `game_id`.
3. Kafka producer replays the play-by-play for that game, simulating live event arrival.
4. Databricks Structured Streaming consumes the stream, dedupes events, and computes rolling in-game stats (e.g. shots, goals, TOI), written as Delta tables (bronze → silver → gold).
5. A join step pulls the current rolling aggregate (gold) and the dbt historical baseline, and outputs a live-vs-expected comparison per player.

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

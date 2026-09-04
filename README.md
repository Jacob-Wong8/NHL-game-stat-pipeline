# NHL/NBA Live vs. Historical Stats Pipeline

## Overview

This project compares a player's live, in-progress performance against their historical baseline, using NHL and NBA play-by-play data. The system replays historical play-by-play as a simulated live feed, computes rolling stats as the game progresses, and joins those rolling numbers against a historical baseline built from past games. The goal is to answer a simple question. Is this player performing above or below what their history predicts, as the game unfolds.

The project is parameterized by game ID, so it is not built around a single hardcoded game. It runs against any completed game's play-by-play data pulled from the NHL API.

## Why This Project Exists

Existing internship experience already covers Spark and Airflow. This project is scoped to add technologies not already on the resume: Kafka, Databricks Structured Streaming, BigQuery, and dbt. The dual-path design (batch plus streaming) gives a reason to use all four in one coherent system rather than four disconnected demos.

## Architecture

There are two paths that join at the end.

### Batch path (historical baseline)
```
Hockey Reference / Basketball Reference
    -> S3 (raw historical data)
    -> BigQuery
    -> dbt (staging -> intermediate -> marts)
    -> historical baseline table, per player
```

### Streaming path (simulated live feed)
```
NHL API (api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play)
    -> fetch_game.py (pulls and saves raw JSON, parameterized by game_id)
    -> extract_plays.py (flattens JSON into clean per-event rows)
    -> Kafka producer (replays events in game order, with a delay between messages
       to simulate a live broadcast)
    -> Databricks Structured Streaming (consumes the topic, computes rolling
       aggregates per player as events arrive)
    -> Delta Lake, medallion architecture on S3 (bronze/silver/gold)
```

### Join layer
The final step joins the live rolling aggregates from the streaming path against the historical baseline from the batch path, per player and per game. This produces a live comparison: actual performance so far tonight versus what history would predict for this player at this point in the game.

An optional Streamlit app can sit on top of this join layer to visualize the comparison.

## Tech Stack

| Layer | Tool |
|---|---|
| Data source | NHL API (api-web.nhle.com), Hockey Reference, Basketball Reference |
| Streaming ingestion | Kafka |
| Stream processing | Databricks Structured Streaming |
| Storage | S3, Delta Lake (medallion) |
| Batch warehouse | BigQuery |
| Transformation | dbt (staging, intermediate, marts) |
| Visualization (optional) | Streamlit |

Airflow is intentionally excluded from this project. It is already covered by existing internship experience, so including it here would add no new resume value.

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

Raw API responses belong under `data/raw/nhl/`. Generated flattened output belongs under `data/extracted/`; small files used for local development belong under `data/sample/`. Credentials and environment-specific configuration should remain outside version control.

## Project Status

Currently built:
- `ingestion/fetch_game.py`: fetches raw play-by-play JSON from the NHL API for a given game ID.
- Sample NHL game responses and game IDs are stored in `data/sample/`.

Not yet built:
- `ingestion/extract_plays.py`: flattens raw JSON into clean, per-event rows.
- Kafka producer to replay extracted rows onto a topic.
- Databricks Structured Streaming consumer computing rolling aggregates.
- Batch path from Hockey Reference through BigQuery and dbt.
- Join layer combining live and historical data.
- Optional Streamlit visualization.

## Build Order

1. Fetch and parse one game's data locally to validate the schema.
2. Build the Kafka producer and confirm events replay correctly against a local broker.
3. Build the Databricks Structured Streaming consumer and confirm rolling aggregates compute correctly.
4. Build the batch path into BigQuery and the dbt models on top of it.
5. Join the two paths.
6. Add the Streamlit app.



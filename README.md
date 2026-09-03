# NHL Actual vs. Expected Goals Pipeline

## 1. Purpose

A streaming-to-batch pipeline that replays historical NHL play-by-play as a simulated live feed, calculates expected goals (xG) with dbt, and compares actual and expected performance by player and team. It demonstrates Kafka, BigQuery, dbt, and Kubernetes alongside existing Spark and Airflow experience.

---

## 2. Architecture

```
NHL API (api-web.nhle.com)
  -> Python producer (reads play-by-play JSON, replays 1 event every 1-3 sec)
  -> Kafka topic (partitioned by game_id)
  -> Consumer (containerized and deployed with Kubernetes) writes raw events to BigQuery
  -> dbt: raw -> staging -> intermediate (xG calculation) -> marts (actual vs expected)
  -> Streamlit reads BigQuery staging and marts
```

Kafka replay is a **simulation layer**, not true real-time ingestion. It adds artificial delay to historical data to demonstrate streaming tools for a fundamentally batch-oriented dataset. State this clearly in the project and interview narrative.

---

## 3. Component Breakdown

### 3.1 Data Source
- **Endpoint:** `https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`
- The unofficial, free NHL API requires no key. Its `plays[]` array includes event type, coordinates, situation and zone codes, and player IDs.
- Use `rosterSpots[]` for player name, team, and position lookups.
- First save and validate one game's JSON locally, then write the producer.

### 3.2 Kafka Producer
- Reads saved or fetched JSON for a given `game_id`.
- Publishes `plays[]` in order to a topic such as `nhl-playbyplay`, sleeping 1-3 sec between messages.
- Uses `game_id` as the message key so multiple games can share a topic without partition collisions.
- Accepts `game_id` through a CLI argument or environment variable. Never hardcode it.

### 3.3 Consumer → BigQuery (Dataflow/Apache Beam)
- Consumes the topic, parses JSON, and validates required fields and event types.
- Writes one row per event to BigQuery `raw_events`, storing the JSON payload plus `game_id`, `event_id`, `period`, `time_in_period`, `type_desc_key`, `event_owner_team_id`, and ingestion timestamp.
- **Build order:** start with a plain Python `google-cloud-bigquery` consumer. Add Beam/Dataflow only after the local end-to-end path works. Beam is optional polish.

### 3.4 Kubernetes
- Containerize the consumer with Python, a Kafka client, and BigQuery client libraries.
- Deploy it with a Kubernetes Deployment on local minikube or kind.
- Do this after the consumer works locally with `python consumer.py`.

### 3.5 dbt Transformations

**Staging (`stg_events`):**
- Clean and type raw columns, including numeric coordinates, parsed timestamps, and consistent team and player IDs.
- Use one model per event type or a unified model filtered downstream. Keep staging 1:1 with raw and free of business logic.

**Intermediate (`int_shots_with_xg`):**
- Filter to `goal`, `shot-on-goal`, `missed-shot`, and `blocked-shot` events.
- Calculate shot distance and angle from `xCoord` and `yCoord`. Account for the 200 x 85 ft rink, defending side, and period because teams switch ends.
- Assign xG with a `CASE WHEN` lookup by distance bucket and `shot_type`, not a trained model. See Section 4.
- Output one row per shot attempt with `player_id`, `team_id`, `shot_type`, `distance_ft`, `is_goal`, and `xg_value`.

**Marts (`fct_player_game_xg`, `fct_team_game_xg`):**
- Aggregate `int_shots_with_xg` by `player_id, game_id` and `team_id, game_id`.
- Include `shot_attempts`, `actual_goals`, `expected_goals` (the sum of `xg_value`), and `xg_delta` (actual minus expected).
- Streamlit reads these marts for its Expected panel.

### 3.6 Streamlit App
See Section 5 for exact fields. Read two paths at different refresh rates:
- **Actual side:** near-live, polling BigQuery raw or staging every 1-3 sec.
- **Expected side:** polling dbt marts every 15-30 sec, or on manual refresh, as dbt runs on that interval.

### 3.7 (Optional) GitHub Actions
- Run `dbt test` on push to catch broken staging and intermediate models. This is optional.

---

## 4. xG Methodology: Lookup Table, Not a Trained Model

Do **not** train a classifier. Use a bucketed lookup table in `int_shots_with_xg`:

| Shot type | Distance bucket | Approx. xG |
|---|---|---|
| Tip-in / deflected | 0-10 ft | 0.25-0.30 |
| Wrap-around | 0-10 ft | 0.12 |
| Wrist / snap | 10-20 ft | 0.08-0.10 |
| Wrist / snap | 20-40 ft | 0.04-0.06 |
| Slap | 20-40 ft | 0.03-0.05 |
| Any type | 40+ ft | 0.01-0.03 |
| Backhand | 0-15 ft | 0.10 |

Base values on public shooting-percentage-by-zone references, such as MoneyPuck or Natural Stat Trick methodology write-ups. Implement the lookup as one `CASE WHEN` block. The pipeline is the deliverable; xG precision is out of scope.

---

## 5. Streamlit Panels

### Actual panel (near-live, 1-3 sec refresh)
- Running home and away score from `homeScore` and `awayScore` on goal events
- Shots on goal per team from `homeSOG` and `awaySOG`
- Actual goals per player from `scoringPlayerId` on goal events
- Optional scrolling feed of recent events as they arrive

### Expected panel (batch, 15-30 sec refresh)
- **Player table:** one row per skater with at least one shot attempt, containing `shot_attempts`, `actual_goals`, `expected_goals` (for example, `0.87`), and `xg_delta`.
  - Grow the row count as events arrive and dbt runs. Do not pre-populate the roster with zeros.
  - Sort by descending `xg_delta` or by time of first attempt.
- **Team table:** same shape, aggregated to team level.
- Store xG as unrounded **floats**, summing per-shot probabilities. Display values rounded to two decimals.

### Shared controls
- `game_id` selector via dropdown or text input. Never hardcode a game.
- Manual refresh button for Expected, in addition to auto-refresh.

---

## 6. Build Order

1. Pull one game's play-by-play JSON, save locally, inspect structure.
2. Prototype and validate the xG lookup in a notebook against that JSON before building infrastructure.
3. Kafka producer replaying the same static JSON.
4. Plain Python consumer -> BigQuery raw table. Skip Beam initially.
5. dbt staging → intermediate → marts.
6. Streamlit app reading both paths.
7. Kubernetes containerization of the consumer.
8. Add Dataflow/Beam and GitHub Actions CI last. Cut these first if time is limited.

---
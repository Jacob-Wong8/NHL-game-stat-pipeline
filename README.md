# NHL Actual vs. Expected Goals Pipeline — Design Doc

## 1. Purpose

A streaming-to-batch data pipeline that replays historical NHL play-by-play as a simulated live feed, computes expected goals (xG) via dbt, and compares actual vs. expected performance per player/team. Built to demonstrate Kafka, BigQuery, dbt, and Kubernetes — technologies not covered by existing Spark/Airflow internship experience.

---

## 2. Architecture

```
NHL API (api-web.nhle.com)
   → Python producer (reads play-by-play JSON, replays 1 event/1–3 sec)
   → Kafka topic (partitioned by game_id)
   → Consumer (containerized, deployed via Kubernetes) writes raw events to BigQuery
   → dbt: raw → staging → intermediate (xG calc) → marts (actual vs expected)
   → Streamlit reads from BigQuery marts/staging
```

Kafka replay is a **simulation layer**, not true real-time ingestion — it replays historical data with artificial delay to justify streaming tooling on a fundamentally batch-natured dataset. This framing should be stated explicitly in the README/interview narrative.

---

## 3. Component Breakdown

### 3.1 Data Source
- **Endpoint:** `https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`
- Unofficial NHL API, free, no key. Returns a `plays[]` array of event objects with `typeDescKey` (goal, shot-on-goal, missed-shot, blocked-shot, hit, giveaway, takeaway, faceoff, penalty, stoppage, etc.), `xCoord`/`yCoord`, `situationCode`, `zoneCode`, and player IDs.
- Also pull `rosterSpots[]` from the same payload for player name/team/position lookups.
- **Action:** save one game's JSON locally first, validate structure, before writing producer code.

### 3.2 Kafka Producer
- Reads saved/fetched JSON for a given `game_id`.
- Iterates `plays[]` in order, publishes each event to a topic (e.g. `nhl-playbyplay`), sleeping 1–3 sec between messages.
- Key the message by `game_id` so multiple games could theoretically be replayed on the same topic without collision.
- Parameterize `game_id` via CLI arg/env var — never hardcode.

### 3.3 Consumer → BigQuery (Dataflow/Apache Beam)
- Consumes the topic, parses JSON, does minimal validation (required fields present, valid event type).
- Writes raw rows to a BigQuery `raw_events` table (one row per event, JSON payload + extracted top-level fields: `game_id`, `event_id`, `period`, `time_in_period`, `type_desc_key`, `event_owner_team_id`, timestamp of ingestion).
- **Build order:** start with a plain Python `google-cloud-bigquery` consumer script. Add Beam/Dataflow only after the plain version works end-to-end — Beam is polish, not core risk.

### 3.4 Kubernetes
- Containerize the consumer (Dockerfile: Python + Kafka client + BigQuery client libs).
- Deploy via a Deployment manifest on local minikube/kind.
- This is packaging, not pipeline logic — do last, after the consumer works locally via `python consumer.py`.

### 3.5 dbt Transformations

**Staging (`stg_events`):**
- Clean/type raw JSON columns (cast coordinates to numeric, parse timestamps, standardize team/player ID types).
- One model per event type or a unified staging model filtered downstream — team's choice, but keep it 1:1 with raw, no business logic yet.

**Intermediate (`int_shots_with_xg`):**
- Filter to shot-attempt events: `goal`, `shot-on-goal`, `missed-shot`, `blocked-shot`.
- Compute shot distance and angle from `xCoord`/`yCoord` (rink is 200×85 ft; net position depends on `homeTeamDefendingSide` and period, since teams switch ends).
- Assign xG via a lookup table (`CASE WHEN` on distance bucket + `shot_type`) — **not** a trained model. See §4.
- Output: one row per shot attempt with `player_id`, `team_id`, `shot_type`, `distance_ft`, `is_goal`, `xg_value`.

**Marts (`fct_player_game_xg`, `fct_team_game_xg`):**
- Aggregate `int_shots_with_xg` by `player_id, game_id` and by `team_id, game_id`.
- Columns: `shot_attempts`, `actual_goals`, `expected_goals` (sum of xg_value), `xg_delta` (actual − expected).
- This is the table Streamlit's "Expected" panel reads.

### 3.6 Streamlit App
See §5 for exact fields. Reads two paths at different refresh rates:
- **Actual side:** near-live, polls BigQuery raw/staging every 1–3 sec.
- **Expected side:** polls the dbt marts table every 15–30 sec (or manual refresh button), since dbt runs on that interval.

### 3.7 (Optional) GitHub Actions
- CI step: `dbt test` on push, to catch broken staging/intermediate models before merge. Nice-to-have, not core.

---

## 4. xG Methodology — Lookup Table, Not a Trained Model

Do **not** train a classifier. Use a bucketed lookup table in the `int_shots_with_xg` dbt model:

| Shot type | Distance bucket | Approx. xG |
|---|---|---|
| Tip-in / deflected | 0–10 ft | 0.25–0.30 |
| Wrap-around | 0–10 ft | 0.12 |
| Wrist / snap | 10–20 ft | 0.08–0.10 |
| Wrist / snap | 20–40 ft | 0.04–0.06 |
| Slap | 20–40 ft | 0.03–0.05 |
| Any type | 40+ ft | 0.01–0.03 |
| Backhand | 0–15 ft | 0.10 |

Base these on publicly available shooting-percentage-by-zone references (e.g. MoneyPuck/Natural Stat Trick methodology write-ups) rather than inventing numbers. This is implemented as a single `CASE WHEN` block — a few hours of work, not an ML project. State this explicitly as a design decision if asked in an interview: the pipeline is the deliverable, xG precision is out of scope.

---

## 5. Streamlit Panels — Exact Stats Displayed

### Actual panel (near-live, 1–3 sec refresh)
- Running score (home/away), from `homeScore`/`awayScore` on goal events
- Shots on goal count per team, from `homeSOG`/`awaySOG`
- Actual goals per player, from `scoringPlayerId` on goal events
- Event feed (optional): scrolling list of recent events as they "arrive"

### Expected panel (batch, 15–30 sec refresh)
- **Player table:** one row per skater who has recorded ≥1 shot attempt so far — `shot_attempts`, `actual_goals`, `expected_goals` (decimal, e.g. `0.87`), `xg_delta`
  - **Row count grows dynamically** as the game replays — a player appears the moment their first shot attempt has been ingested and picked up by a dbt run. Do not pre-populate the full roster with zeros; query the marts table fresh each refresh and let it grow.
  - Sort by `xg_delta` descending (overperformers on top) or by time-of-first-attempt for stable ordering.
- **Team table:** same shape, aggregated to team level.
- xG values are **floats**, not integers — always sum of per-shot probabilities, displayed rounded to 2 decimals for readability but stored unrounded.

### Shared controls
- `game_id` selector (dropdown or text input) — never hardcode a single game.
- Manual refresh button for the Expected side, in addition to auto-refresh.

---

## 6. Build Order (De-risk First)

1. Pull one game's play-by-play JSON, save locally, inspect structure.
2. Prototype xG lookup logic in a notebook against that static JSON — validate before building any infra.
3. Kafka producer replaying the same static JSON.
4. Plain Python consumer → BigQuery raw table (skip Beam initially).
5. dbt staging → intermediate → marts.
6. Streamlit app reading both paths.
7. Kubernetes containerization of the consumer.
8. Dataflow/Beam polish, GitHub Actions CI — last, and first to cut if time-constrained.

---

## 7. Resume Framing

- Keywords added beyond existing RBC experience: **Kafka, BigQuery, dbt, Kubernetes**.
- Talking points for interviews: streaming-simulation pattern for replaying historical data, medallion-style dbt layering, deliberate choice of lookup-table xG over ML to keep scope on pipeline engineering.
- If also building the Snowflake/Databricks lakehouse project, keep the *analytical question* here framed as "actual vs. expected performance," distinct from the other project's cross-sport equivalency angle, to avoid the two NHL projects reading as one idea duplicated across vendors.

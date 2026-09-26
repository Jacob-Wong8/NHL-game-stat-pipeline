# NHL Actual vs. Expected Stats

This project compares a skater's performance in a past NHL game with their season averages. Historical play-by-play is replayed through Kafka to simulate a live game feed. The pipeline processes the events with Databricks and stores the results in Delta Lake tables on S3.

The project covers skaters and tracks goals, assists, and shots.

## Data Pipelines

**Actual:** Replays a past game's play-by-play as a simulated live feed. The Streamlit actual view shows the score, shots, time remaining, and selected skater stats.

Flow: Game play-by-play JSON → Kafka → S3 → Databricks bronze, silver, and gold Delta Lake tables → Streamlit actual view (left).

**Expected:** Calculates each player's average goals, assists, and shots per game from season stats for both teams. The Streamlit expected view shows the selected skater's averages.

Flow: Team season stats → Python script → Google Cloud Platform/BigQuery → Streamlit expected view (right).

## Streamlit Interface

The interface displays actual stats on the left and expected stats on the right. Each side has a skater dropdown. Selecting a player shows their game stats on the actual side and season averages on the expected side.

## Project Structure

```text
data/                              # Raw game JSON and extracted events
pipelines/
	actual/
		ingestion/                     # Fetch and extract game events
		producer/                      # Publish events to Kafka
		databricks/                    # Stream processing jobs
	expected/batch/                  # Calculate player season averages
app/                               # Streamlit interface
tests/                             # Pipeline tests
```


Created this project because I love watching hockey and seeing the skater stats :)
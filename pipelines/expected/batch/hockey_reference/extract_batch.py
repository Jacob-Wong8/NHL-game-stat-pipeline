import argparse
import json
from pathlib import Path
from typing import Any, Callable

from pipelines.expected.batch.upload_to_s3 import upload_file_to_s3
from pipelines.expected.batch.hockey_reference.fetch_expected_stats import fetch_team_stats


def extract_team_stats(
    teams: list[str],
    season: int,
    playoffs: bool = False,
    fetcher: Callable[..., list[dict[str, Any]]] = fetch_team_stats,
) -> list[dict[str, Any]]:
    """Return normalized Hockey Reference records for the requested teams."""
    records: list[dict[str, Any]] = []
    for team in teams:
        team_abbrev = team.upper()
        for player in fetcher(team_abbrev, season, playoffs=playoffs):
            records.append(
                {
                    "player_name": player["player_name"],
                    "player_url": player.get("player_url", ""),
                    "team_abbrev": team_abbrev,
                    "season": season,
                    "games_played": int(player.get("games_played", 0) or 0),
                    "goals": int(player.get("goals", 0) or 0),
                    "assists": int(player.get("assists", 0) or 0),
                    "shots": int(player.get("shots", 0) or 0),
                }
            )
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract normalized Hockey Reference skater stats as JSONL.")
    parser.add_argument("--season", type=int, required=True, help="Season ending year, such as 2024")
    parser.add_argument("--teams", nargs="+", required=True, help="NHL team abbreviations, such as EDM CGY")
    parser.add_argument("--playoffs", action="store_true", help="Extract playoff stats instead of regular-season stats")
    parser.add_argument("--output", type=Path, required=True, help="Output JSONL path")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--prefix",
        default="expected/hockey_reference",
        help="S3 key prefix; the output filename is appended",
    )
    args = parser.parse_args()

    records = extract_team_stats(args.teams, args.season, playoffs=args.playoffs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as output_file:
        for record in records:
            output_file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    print(f"Hockey Reference records -> {args.output} ({len(records)} players)")
    print(f"Uploaded -> {upload_file_to_s3(args.output, args.bucket, args.prefix)}")


if __name__ == "__main__":
    main()
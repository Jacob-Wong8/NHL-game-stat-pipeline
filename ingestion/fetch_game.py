import json
import urllib.request
import urllib.error
import argparse
from pathlib import Path

NHL_API_BASE = "https://api-web.nhle.com/v1/gamecenter"

#fetches the play by play information using the unique game id
def fetch_play_by_play(game_id: int) -> tuple[dict, int]:
    while True:
        try:
            if game_id <= 0:
                raise ValueError("Game ID must be a positive integer.")

            url = f"{NHL_API_BASE}/{game_id}/play-by-play"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.load(resp)

            if not isinstance(data, dict) or "plays" not in data:
                raise ValueError("No game data was returned for that ID.")

            return data, game_id

        except (urllib.error.HTTPError, urllib.error.URLError, ValueError, json.JSONDecodeError) as exc:
            print(f"Invalid or unavailable NHL game ID: {exc}")
            retry = input("Enter a valid NHL game ID to try again, or press Enter to exit: ").strip()
            if not retry:
                raise SystemExit("Exiting...")

            try:
                game_id = int(retry)
            except ValueError:
                print("not a valid integer game ID.")
                continue


#saves the data as a json file
def save_game(game_id: int, out_dir: str = ".") -> Path:
    data, valid_game_id = fetch_play_by_play(game_id)
    out_path = Path(out_dir) / f"play_by_play_{valid_game_id}.json"
    with open(out_path, "w") as f:
        json.dump(data, f)
    print(f"Saved game {valid_game_id} -> {out_path} ({len(data.get('plays', []))} events)")
    return out_path
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id", type=int, help="NHL game ID, e.g. 2025030213")
    parser.add_argument("--out-dir", default=".")
    args = parser.parse_args()
    save_game(args.game_id, args.out_dir)
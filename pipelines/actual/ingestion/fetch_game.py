import json
import urllib.request
import urllib.error
import argparse
from pathlib import Path

try:
    from .extract_plays import save_extracted_plays
except ImportError:
    from extract_plays import save_extracted_plays

NHL_API_BASE = "https://api-web.nhle.com/v1/gamecenter"
DEFAULT_OUT_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "nhl" #output directory of the json file
DEFAULT_EXTRACTED_OUT_DIR = Path(__file__).resolve().parents[3] / "data" / "extracted"

#fetches the play by play information using the unique game id
def fetch_play_by_play(game_id: int | str) -> tuple[dict, int]:
    while True:
        try:
            game_id = int(game_id)
            if game_id <= 0:
                raise ValueError("Game ID must be a positive integer.")

            url = f"{NHL_API_BASE}/{game_id}/play-by-play"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}) #http request for the NHL API
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.load(resp)

            if not isinstance(data, dict) or "plays" not in data:
                raise ValueError("No game data was returned for that ID.")

            return data, game_id

        except (urllib.error.HTTPError, urllib.error.URLError, ValueError, json.JSONDecodeError) as e:
            print(f"Invalid or unavailable NHL game ID: {e}")
            retry = input("Enter a valid NHL game ID to try again, or press Enter to exit: ").strip()

            #if the user presses Enter
            if not retry:
                raise SystemExit("Exiting...")

            try:
                game_id = int(retry)
            except ValueError:
                print("not a valid integer game ID.")
                continue


#saves the data as a json file
def save_game(game_id: int, out_dir: str | Path = DEFAULT_OUT_DIR) -> Path:
    data, valid_game_id = fetch_play_by_play(game_id)
    out_path = Path(out_dir) / f"play_by_play_{valid_game_id}.json" #output path of the json file
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(data, f)
    extracted_path = DEFAULT_EXTRACTED_OUT_DIR / f"{out_path.stem}.jsonl"
    save_extracted_plays(out_path, extracted_path)
    print(f"Saved game {valid_game_id} -> {out_path} ({len(data.get('plays', []))} events)")
    print(f"Extracted plays -> {extracted_path}")
    return out_path
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id", help="NHL game ID, e.g. 2025030213")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    save_game(args.game_id, args.out_dir)
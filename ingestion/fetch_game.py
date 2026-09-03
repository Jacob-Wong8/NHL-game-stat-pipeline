import json
import urllib.request
import argparse
from pathlib import Path

NHL_API_BASE = "https://api-web.nhle.com/v1/gamecenter"

def fetch_play_by_play(game_id: int) -> dict:
    url = f"{NHL_API_BASE}/{game_id}/play-by-play"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)

def save_game(game_id: int, out_dir: str = ".") -> Path:
    data = fetch_play_by_play(game_id)
    out_path = Path(out_dir) / f"play_by_play_{game_id}.json"
    with open(out_path, "w") as f:
        json.dump(data, f)
    print(f"Saved game {game_id} -> {out_path} ({len(data.get('plays', []))} events)")
    return out_path
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id", type=int, help="NHL game ID, e.g. 2025030213")
    parser.add_argument("--out-dir", default=".")
    args = parser.parse_args()
    save_game(args.game_id, args.out_dir)
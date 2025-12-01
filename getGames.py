"""
getGames.py (API nova NHL)

Vai buscar o calendario de HOJE e AMANHA via https://api-web.nhle.com
e grava em data/games_cache.json com o mesmo contrato que o frontend ja usa.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "games_cache.json")

SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule/{date}"
BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"


def safe_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def map_game_state(state: str) -> int:
    state_up = (state or "").upper()
    if state_up in {"FUT", "PRE", "PREGAME", "WARMUP"}:
        return 1  # scheduled
    if state_up in {"LIVE", "CRIT", "INPROGRESS"}:
        return 2  # live
    if state_up in {"FINAL", "OFF", "POSTPONED", "TBD"}:
        return 3  # final/other
    return 1


def build_team(team_raw: Dict) -> Dict:
    record = team_raw.get("record", {}) or {}
    wins = record.get("wins")
    losses = record.get("losses")
    ot = record.get("ot")

    name = (
        team_raw.get("name")
        or (team_raw.get("commonName") or {}).get("default")
        or team_raw.get("abbrev")
    )
    city = (
        (team_raw.get("placeNameWithPreposition") or {}).get("default")
        or (team_raw.get("placeName") or {}).get("default")
        or team_raw.get("city")
    )

    def build_record():
        if wins is None or losses is None:
            return ""
        if ot is None:
            return f"{wins}-{losses}"
        return f"{wins}-{losses}-{ot}"

    return {
        "teamId": team_raw.get("id"),
        "teamTricode": team_raw.get("abbrev"),
        "teamName": name,
        "teamCity": city,
        "wins": wins,
        "losses": losses,
        "score": team_raw.get("score"),
        # legacy aliases
        "tricode": team_raw.get("abbrev"),
        "name": name,
        "city": city,
        "record": build_record(),
    }


def fetch_linescore(game_id: int, warnings: List[str]) -> Tuple[int, str]:
    url = BOX_URL.format(game_id=game_id)
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        period = data.get("periodDescriptor", {}).get("number")
        clock_data = data.get("clock", {}) or {}
        clock = clock_data.get("timeRemaining") or clock_data.get("displayValue")
        return period, clock
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Falha ao obter clock de {game_id}: {exc}")
        return None, None


def fetch_games_for_date(date_str: str, warnings: List[str]) -> List[Dict]:
    url = SCHEDULE_URL.format(date=date_str)
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Erro ao obter jogos da NHL ({date_str}): {exc}")
        return []

    weeks = data.get("gameWeek", []) or []
    games_raw = []
    for w in weeks:
        games_raw.extend(w.get("games", []) or [])

    games_list = []
    for g in games_raw:
        game_id = g.get("id")
        state = g.get("gameState")
        status = map_game_state(state)
        start_time = g.get("startTimeUTC")

        home_raw = g.get("homeTeam", {}) or {}
        away_raw = g.get("awayTeam", {}) or {}

        period = None
        clock = None
        if game_id:
            period, clock = fetch_linescore(game_id, warnings)

        games_list.append(
            {
                "game_id": str(game_id),
                "status": status,
                "status_text": state,
                "period": period,
                "clock": clock,
                "start_time_utc": start_time,
                "home": build_team(home_raw),
                "away": build_team(away_raw),
            }
        )

    return games_list


def fetch_games():
    warnings: List[str] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)

    today_str = today.isoformat()
    tomorrow_str = tomorrow.isoformat()

    today_games = fetch_games_for_date(today_str, warnings)
    tomorrow_games = fetch_games_for_date(tomorrow_str, warnings)

    live = [g for g in today_games if g.get("status") == 2]
    today_upcoming = [g for g in today_games if g.get("status") == 1]
    tomorrow_upcoming = [g for g in tomorrow_games if g.get("status") == 1]

    ok = not warnings

    return {
        "ok": ok,
        "live_games": live,
        "today_upcoming": today_upcoming,
        "tomorrow_upcoming": tomorrow_upcoming,
        "warnings": warnings,
        "generated_at_utc": now_iso,
    }


def main():
    data = fetch_games()

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"? games_cache.json atualizado em {OUTPUT_FILE} (ok={data['ok']})")


if __name__ == "__main__":
    main()

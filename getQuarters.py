"""
getQuarters.py (NHL, API nova)

Cria/atualiza um CSV com golos por periodo e stats basicas
usando os endpoints novos:
- Calendario: https://api-web.nhle.com/v1/schedule/{date} (devolve 1 semana)
- Boxscore:  https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore

Percorre a epoca 2025-2026 ate hoje.
"""

import os
from datetime import date, timedelta
from typing import Dict, List

import pandas as pd
import requests

SEASON_ID = "20252026"
SEASON_TYPE = "Regular"
START_DATE = date(2025, 10, 1)
OUTPUT_FILE = os.path.join("data", f"nhl_periods_{SEASON_ID}.csv")

SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule/{date}"
BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
MAX_WARNINGS = 20


def safe_int(value, default=0):
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def is_dns_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "name or service not known" in msg or "name resolution" in msg or "getaddrinfo failed" in msg


def fetch_schedule_range(start: date, end: date, warnings: List[str]) -> List[Dict]:
    games: List[Dict] = []
    seen_ids = set()
    current = start
    # cada chamada traz 1 semana de jogos a partir da data dada
    while current <= end:
        url = SCHEDULE_URL.format(date=current.isoformat())
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            for week in data.get("gameWeek", []) or []:
                for g in week.get("games", []) or []:
                    gid = g.get("id")
                    if not gid or gid in seen_ids:
                        continue
                    seen_ids.add(gid)
                    start_time = g.get("startTimeUTC") or ""
                    game_date = start_time.split("T")[0] if start_time else ""
                    games.append(
                        {
                            "id": gid,
                            "date": game_date,
                            "home": g.get("homeTeam", {}) or {},
                            "away": g.get("awayTeam", {}) or {},
                        }
                    )
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Erro ao obter calendario {current}: {exc}")
            if is_dns_error(exc):
                warnings.append("DNS falhou para api-web.nhle.com; a parar recolha.")
                break
        if len(warnings) >= MAX_WARNINGS:
            warnings.append("Limite de avisos atingido; a parar recolha.")
            break
        current += timedelta(days=7)
    return games


def fetch_boxscore(game_id: int, warnings: List[str]) -> Dict:
    url = BOX_URL.format(game_id=game_id)
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Falha boxscore {game_id}: {exc}")
        if is_dns_error(exc):
            warnings.append("DNS falhou para api-web.nhle.com; parar restantes boxscores.")
        return {}


def extract_periods(team_node: Dict) -> Dict[str, int]:
    periods = team_node.get("scoresByPeriod", []) or []
    goals_by_num = {p.get("periodNumber"): safe_int(p.get("goals")) for p in periods}
    p1 = goals_by_num.get(1, 0)
    p2 = goals_by_num.get(2, 0)
    p3 = goals_by_num.get(3, 0)
    ot = sum(v for k, v in goals_by_num.items() if k and k > 3)
    total = safe_int(team_node.get("score"), p1 + p2 + p3 + ot)
    if ot <= 0:
        ot = max(0, total - (p1 + p2 + p3))
    return {"p1": p1, "p2": p2, "p3": p3, "ot": ot, "total": total}


def build_team_row(
    side: str, matchup: str, game_id: int, game_date: str, team_node: Dict
) -> Dict:
    periods = extract_periods(team_node)

    name = (
        team_node.get("commonName", {}) or {}
    ).get("default") or team_node.get("name") or team_node.get("abbrev")
    abbrev = team_node.get("abbrev")

    pp = team_node.get("powerPlayConversion", {}) or {}

    return {
        "GAME_ID": str(game_id),
        "GAME_DATE": game_date,
        "MATCHUP": matchup,
        "TEAM_ID": team_node.get("id"),
        "TEAM_ABBREVIATION": abbrev,
        "TEAM_NAME": name,
        "P1": periods["p1"],
        "P2": periods["p2"],
        "P3": periods["p3"],
        "OT": periods["ot"],
        # aliases para compatibilidade
        "Q1": periods["p1"],
        "Q2": periods["p2"],
        "Q3": periods["p3"],
        "Q4": periods["ot"],
        "PTS": periods["total"],
        "GOALS": periods["total"],
        "SHOTS": safe_int(team_node.get("sog")),
        "POWER_PLAY_GOALS": safe_int(pp.get("goals")),
        "POWER_PLAY_OPPORTUNITIES": safe_int(pp.get("opportunities")),
        "PIM": safe_int(team_node.get("pim")),
        "HITS": safe_int(team_node.get("hits")),
        "BLOCKED": safe_int(team_node.get("blockedShots")),
        "TAKEAWAYS": safe_int(team_node.get("takeaways")),
        "GIVEAWAYS": safe_int(team_node.get("giveaways")),
        "SEASON": SEASON_ID,
        "SEASON_TYPE": SEASON_TYPE,
    }


def build_rows() -> pd.DataFrame:
    warnings: List[str] = []
    today = date.today()
    schedule = fetch_schedule_range(START_DATE, today, warnings)

    all_rows: List[Dict] = []

    for g in schedule:
        game_id = g.get("id")
        if not game_id:
            continue

        box = fetch_boxscore(game_id, warnings)
        if not box:
            continue

        home_node = box.get("homeTeam", {}) or {}
        away_node = box.get("awayTeam", {}) or {}

        home_abbr = home_node.get("abbrev") or g.get("home", {}).get("abbrev")
        away_abbr = away_node.get("abbrev") or g.get("away", {}).get("abbrev")
        matchup = f"{away_abbr} @ {home_abbr}"

        game_date = g.get("date")

        away_row = build_team_row("away", matchup, game_id, game_date, away_node)
        home_row = build_team_row("home", matchup, game_id, game_date, home_node)

        all_rows.extend([away_row, home_row])

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df.sort_values(["GAME_DATE", "GAME_ID", "TEAM_ABBREVIATION"], inplace=True)
    if warnings:
        print("Avisos:", "; ".join(warnings))
    return df


def main():
    try:
        df = build_rows()
    except Exception as exc:  # noqa: BLE001
        print("? Erro geral:", exc)
        return

    if df.empty:
        print("?? Sem dados para gravar.")
        return

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"? Ficheiro atualizado: {OUTPUT_FILE} ({len(df)} linhas)")


if __name__ == "__main__":
    main()

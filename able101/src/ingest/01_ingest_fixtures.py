#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Able101 Fixtures Ingestion (API-Football)

- Pulls fixtures (/fixtures). Finished fixtures (FT/AET/PEN) -> upsert into fixtures.
- For finished fixtures, optionally fetches statistics (/fixtures/statistics?fixture=ID) gated by coverage (/leagues).
- Derives: home_win, draw, away_win, over_1_5, over_2_5, btts + 20 additional derived columns (requires prior migration).
- Rolling mode: also fetches UPCOMING fixtures (non-finished status) for a future window and writes them to 'upcoming_fixtures'.
- Cron-safe: one line per processed fixture; concise email summary with per-league counts (finished + upcoming).
- SQLite PRAGMAs: WAL / NORMAL; idempotent upserts; ASCII-only code.

Exit codes:
  0 = success
  2 = schema missing or DB error
  3 = configuration error (.env or allowlist)
  4 = API error (hard fail)
"""

# =========================
# ===== CONFIG BLOCK  =====
# =========================

DB_PATH = "/mnt/able101_usb/database/able101.sqlite"
ENV_PATH = "/home/zentr/ProjectAbleA/able101/config/.env"
LEAGUE_ALLOWLIST_PATH = "/mnt/able101_usb/config/league_allowlist.txt"

# League selection
USE_ALLOWLIST = True
OVERRIDE_LEAGUES = [39]  # ignored if USE_ALLOWLIST=True

# Ingestion window — choose exactly ONE mode
USE_SEASONS_MODE = False
SEASONS = [2024]

USE_LAST_X_DAYS_MODE = True         # rolling mode ON
LAST_X_DAYS = 7                     # past window

# Upcoming fixtures window (only used in rolling mode)
UPCOMING_WINDOW_DAYS = 7            # default 7; change as needed

# Upcoming table management: "recreate" or "merge"
UPCOMING_TABLE_MODE = "recreate"

# Rate limits (canon cap)
REQUESTS_PER_SECOND = 6.0
DAILY_REQUEST_CAP = 70000

# Flags
PRINT_EACH_FIXTURE = True
SEND_EMAIL = True
FORCE_STATS = False

TIMEZONE = "Europe/London"
MIN_NON_NULL_STATS_PER_TEAM = 10

# =========================
# ======  IMPLEMENT  ======
# =========================

import os
import sys
import ssl
import time
import smtplib
import sqlite3
import traceback
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import threading
import requests

API_BASE = "https://v3.football.api-sports.io"
USER_AGENT = "Able101-Ingest/1.4 (+https://github.com/craigtaylor98787/ProjectAbleA)"
FINISHED_STATUS = {"FT", "AET", "PEN"}

def load_env(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f".env not found at {path}")
    env: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env

class RateLimiter:
    def __init__(self, rps: float, daily_cap: int):
        self.min_interval = 1.0 / max(0.001, rps)
        self.daily_cap = daily_cap
        self.lock = threading.Lock()
        self.last_time = 0.0
        self.count = 0
    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_time = time.time()
            self.count += 1
            if self.count > self.daily_cap:
                raise RuntimeError(f"Daily request cap exceeded ({self.daily_cap})")

class ApiClient:
    def __init__(self, api_key: str, rps: float, daily_cap: int):
        self.session = requests.Session()
        self.session.headers.update({
            "x-apisports-key": api_key,
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        self.rate = RateLimiter(rps, daily_cap)
    def _request(self, method: str, url: str, params: Dict[str, Any], max_retries: int = 5) -> Dict[str, Any]:
        backoff = 1.0
        for attempt in range(1, max_retries + 1):
            self.rate.wait()
            try:
                resp = self.session.request(method, url, params=params, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(backoff); backoff = min(backoff * 2.0, 20.0); continue
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            except requests.RequestException:
                if attempt == max_retries:
                    raise
                time.sleep(backoff); backoff = min(backoff * 2.0, 20.0)
        raise RuntimeError("Exhausted retries")
    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{API_BASE}{path}"
        return self._request("GET", url, params=params)

def connect_db(path: str) -> sqlite3.Connection:
    if not os.path.exists(path):
        raise RuntimeError(f"DB file missing at {path}")
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fixtures';")
    if cur.fetchone() is None:
        conn.close()
        raise RuntimeError("Required table 'fixtures' does not exist. No migrations allowed.")
    return conn

def ensure_upcoming_table(conn: sqlite3.Connection, mode: str) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS upcoming_fixtures (
        fixture_id INTEGER PRIMARY KEY,
        date TEXT,
        league_id INTEGER,
        season INTEGER,
        home_team TEXT,
        away_team TEXT,
        league_name TEXT,
        country TEXT,
        status_short TEXT,
        status_long TEXT,
        last_updated TEXT
    );
    """
    if mode.lower() == "recreate":
        conn.execute('DROP TABLE IF EXISTS upcoming_fixtures;')
    conn.execute(ddl)

def load_league_ids() -> List[int]:
    if USE_ALLOWLIST:
        if not os.path.exists(LEAGUE_ALLOWLIST_PATH):
            raise FileNotFoundError(f"Allowlist not found at {LEAGUE_ALLOWLIST_PATH}")
        ids: List[int] = []
        with open(LEAGUE_ALLOWLIST_PATH, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"): continue
                try: ids.append(int(s))
                except ValueError: pass
        if not ids: raise RuntimeError("Allowlist is empty.")
        return ids
    else:
        if not OVERRIDE_LEAGUES:
            raise RuntimeError("OVERRIDE_LEAGUES is empty while USE_ALLOWLIST=False.")
        return list(OVERRIDE_LEAGUES)

class CoverageGate:
    def __init__(self, client: ApiClient, force: bool = False):
        self.client = client
        self.force = force
        self.cache: Dict[Tuple[int, int], bool] = {}
    def stats_available(self, league_id: int, season: int) -> bool:
        if self.force: return True
        key = (league_id, season)
        if key in self.cache: return self.cache[key]
        data = self.client.get("/leagues", {"id": league_id, "season": season})
        ok = False
        try:
            resp = data.get("response", [])
            if resp:
                seasons = resp[0].get("seasons", [])
                for s in seasons:
                    if int(s.get("year")) == int(season):
                        cov = s.get("coverage", {}) or {}
                        fx = cov.get("fixtures", {}) or {}
                        ok = bool(fx.get("statistics_fixtures", False))
                        break
        except Exception:
            ok = False
        self.cache[key] = ok
        return ok

STAT_MAP = {
    "Shots on Goal": "shots_on_goal",
    "Shots off Goal": "shots_off_goal",
    "Total Shots": "total_shots",
    "Blocked Shots": "blocked_shots",
    "Shots insidebox": "shots_insidebox",
    "Shots outsidebox": "shots_outsidebox",
    "Fouls": "fouls",
    "Corner Kicks": "corner_kicks",
    "Offsides": "offsides",
    "Ball Possession": "ball_possession",
    "Yellow Cards": "yellow_cards",
    "Red Cards": "red_cards",
    "Goalkeeper Saves": "goalkeeper_saves",
    "Total passes": "total_passes",
    "Passes accurate": "passes_accurate",
    "Passes %": "passes_%",
}
STAT_BASE_KEYS = list(STAT_MAP.values())
HOME_STAT_COLS = [f"home_{k}" for k in STAT_BASE_KEYS]
AWAY_STAT_COLS = [f"away_{k}" for k in STAT_BASE_KEYS]
ALL_STAT_COLS = HOME_STAT_COLS + AWAY_STAT_COLS

DERIVED_COLUMNS = [
    "goal_diff", "total_goals",
    "home_shot_accuracy", "away_shot_accuracy",
    "home_pass_accuracy", "away_pass_accuracy",
    "home_possession_share", "home_disciplinary",
    "corners_total", "shots_inside_share_home",
    "clean_sheet_home", "clean_sheet_away",
    "over_3_5", "under_2_5",
    "shots_diff", "sog_diff", "corners_diff", "possession_diff",
    "conversion_rate_home", "conversion_rate_away",
]

ALL_COLUMNS = [
    "fixture_id", "date", "league_id", "season", "home_team", "away_team",
    "league_name", "country", "home_score", "away_score",
    "home_win", "draw", "away_win", "over_1_5", "over_2_5", "btts",
    "home_shots_on_goal", "home_shots_off_goal", "home_total_shots", "home_blocked_shots",
    "home_shots_insidebox", "home_shots_outsidebox", "home_fouls", "home_corner_kicks",
    "home_offsides", "home_ball_possession", "home_yellow_cards", "home_red_cards",
    "home_goalkeeper_saves", "home_total_passes", "home_passes_accurate", "home_passes_%",
    "away_shots_on_goal", "away_shots_off_goal", "away_total_shots", "away_blocked_shots",
    "away_shots_insidebox", "away_shots_outsidebox", "away_fouls", "away_corner_kicks",
    "away_offsides", "away_ball_possession", "away_yellow_cards", "away_red_cards",
    "away_goalkeeper_saves", "away_total_passes", "away_passes_accurate", "away_passes_%",
    "last_updated",
] + DERIVED_COLUMNS

def _parse_stat_value(v: Any) -> Optional[float]:
    if v is None: return None
    if isinstance(v, str) and v.endswith("%"):
        try: return float(v.strip("%"))
        except ValueError: return None
    if isinstance(v, (int, float)): return float(v)
    try: return float(str(v).strip())
    except Exception: return None

def extract_stats_per_team(stat_list: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {v: None for v in STAT_MAP.values()}
    for item in stat_list or []:
        t = item.get("type")
        if t not in STAT_MAP: continue
        key = STAT_MAP[t]
        val = _parse_stat_value(item.get("value"))
        out[key] = val
    return out

def map_statistics_payload(payload: Dict[str, Any],
                           home_team_id: Optional[int],
                           home_team_name: str,
                           away_team_id: Optional[int],
                           away_team_name: str) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    home_stats = {v: None for v in STAT_MAP.values()}
    away_stats = {v: None for v in STAT_MAP.values()}
    entries = payload.get("response", [])
    if not entries: return home_stats, away_stats
    def matches(e, tid, tname):
        team = e.get("team", {}) or {}
        if tid is not None and team.get("id") == tid: return True
        if tname and team.get("name") == tname: return True
        return False
    home_entry = next((e for e in entries if matches(e, home_team_id, home_team_name)), None)
    away_entry = next((e for e in entries if matches(e, away_team_id, away_team_name)), None)
    if home_entry is None and entries: home_entry = entries[0]
    if away_entry is None and len(entries) > 1: away_entry = entries[1]
    if home_entry: home_stats = extract_stats_per_team(home_entry.get("statistics", []))
    if away_entry: away_stats = extract_stats_per_team(away_entry.get("statistics", []))
    return home_stats, away_stats

def get_existing_fixture(conn: sqlite3.Connection, fixture_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute('SELECT ' + ", ".join([f'"{c}"' for c in ALL_COLUMNS]) + ' FROM "fixtures" WHERE "fixture_id"=?;', (fixture_id,))
    row = cur.fetchone()
    if row is None: return None
    return {col: row[i] for i, col in enumerate(ALL_COLUMNS)}

def count_non_null_stats(row: Dict[str, Any], prefix: str) -> int:
    keys = [f"{prefix}_{k}" for k in STAT_BASE_KEYS]
    return sum(1 for k in keys if row.get(k) is not None)

def is_row_stats_complete(row: Dict[str, Any]) -> bool:
    if row is None: return False
    return (count_non_null_stats(row, "home") >= MIN_NON_NULL_STATS_PER_TEAM and
            count_non_null_stats(row, "away") >= MIN_NON_NULL_STATS_PER_TEAM)

def merge_stats_preserving_existing(existing: Optional[Dict[str, Any]], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for col in ALL_STAT_COLS:
        new_val = incoming.get(col)
        if new_val is not None:
            merged[col] = new_val
        else:
            merged[col] = existing.get(col) if existing else None
    return merged

def safe_div(n, d):
    if n is None or d is None or d == 0: return None
    try: return float(n) / float(d)
    except Exception: return None

def compute_derived(row: Dict[str, Any]) -> Dict[str, Any]:
    hs = row.get("home_score"); as_ = row.get("away_score")
    hts = row.get("home_total_shots"); ats = row.get("away_total_shots")
    hsog = row.get("home_shots_on_goal"); asog = row.get("away_shots_on_goal")
    htp = row.get("home_total_passes"); atp = row.get("away_total_passes")
    hpa = row.get("home_passes_accurate"); apa = row.get("away_passes_accurate")
    hpos = row.get("home_ball_possession"); apos = row.get("away_ball_possession")
    hicb = row.get("home_shots_insidebox")
    hck = row.get("home_corner_kicks"); ack = row.get("away_corner_kicks")

    total_goals = (hs + as_) if (hs is not None and as_ is not None) else None
    goal_diff = (hs - as_) if (hs is not None and as_ is not None) else None

    home_shot_accuracy = safe_div(hsog, hts)
    away_shot_accuracy = safe_div(asog, ats)
    home_pass_accuracy = safe_div(hpa, htp)
    away_pass_accuracy = safe_div(apa, atp)

    home_possession_share = None
    if hpos is not None and apos is not None and (hpos + apos) > 0:
        home_possession_share = float(hpos) / float(hpos + apos)

    hy = row.get("home_yellow_cards"); hr = row.get("home_red_cards")
    home_disciplinary = None
    if hy is not None or hr is not None:
        home_disciplinary = (hy or 0) + 2 * (hr or 0)

    corners_total = None
    if hck is not None or ack is not None:
        corners_total = (hck or 0) + (ack or 0)

    shots_inside_share_home = safe_div(hicb, hts)

    clean_sheet_home = (1 if as_ == 0 else 0) if as_ is not None else None
    clean_sheet_away = (1 if hs == 0 else 0) if hs is not None else None

    over_3_5 = (1 if total_goals > 3 else 0) if total_goals is not None else None
    under_2_5 = (1 if total_goals <= 2 else 0) if total_goals is not None else None

    shots_diff = (row.get("home_total_shots") - row.get("away_total_shots")
                  if row.get("home_total_shots") is not None and row.get("away_total_shots") is not None else None)
    sog_diff = (row.get("home_shots_on_goal") - row.get("away_shots_on_goal")
                if row.get("home_shots_on_goal") is not None and row.get("away_shots_on_goal") is not None else None)
    corners_diff = (hck - ack) if (hck is not None and ack is not None) else None
    possession_diff = (float(hpos) - float(apos)) if (hpos is not None and apos is not None) else None

    conversion_rate_home = safe_div(hs, hts)
    conversion_rate_away = safe_div(as_, ats)

    return {
        "goal_diff": goal_diff, "total_goals": total_goals,
        "home_shot_accuracy": home_shot_accuracy, "away_shot_accuracy": away_shot_accuracy,
        "home_pass_accuracy": home_pass_accuracy, "away_pass_accuracy": away_pass_accuracy,
        "home_possession_share": home_possession_share, "home_disciplinary": home_disciplinary,
        "corners_total": corners_total, "shots_inside_share_home": shots_inside_share_home,
        "clean_sheet_home": clean_sheet_home, "clean_sheet_away": clean_sheet_away,
        "over_3_5": over_3_5, "under_2_5": under_2_5,
        "shots_diff": shots_diff, "sog_diff": sog_diff, "corners_diff": corners_diff,
        "possession_diff": possession_diff,
        "conversion_rate_home": conversion_rate_home, "conversion_rate_away": conversion_rate_away,
    }

def upsert_fixture(conn: sqlite3.Connection, row: Dict[str, Any]) -> bool:
    qcols = ", ".join([f'"{c}"' for c in ALL_COLUMNS])
    placeholders = ", ".join(["?"] * len(ALL_COLUMNS))
    updates = ", ".join([f'"{c}"=excluded."{c}"' for c in ALL_COLUMNS if c != "fixture_id"])
    sql = f'INSERT INTO "fixtures" ({qcols}) VALUES ({placeholders}) ON CONFLICT("fixture_id") DO UPDATE SET {updates};'
    cur = conn.execute('SELECT 1 FROM "fixtures" WHERE "fixture_id"=?;', (row["fixture_id"],))
    exists = cur.fetchone() is not None
    values = [row.get(c) for c in ALL_COLUMNS]
    conn.execute(sql, values)
    return not exists

def upsert_upcoming(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    cols = ["fixture_id","date","league_id","season","home_team","away_team","league_name","country","status_short","status_long","last_updated"]
    qcols = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    updates = ", ".join([f'"{c}"=excluded."{c}"' for c in cols if c != "fixture_id"])
    sql = f'INSERT INTO "upcoming_fixtures" ({qcols}) VALUES ({placeholders}) ON CONFLICT("fixture_id") DO UPDATE SET {updates};'
    values = [row.get(c) for c in cols]
    conn.execute(sql, values)

def derive_outcomes(home: Optional[int], away: Optional[int]) -> Tuple[int, int, int, int, int, int]:
    if home is None or away is None: return (0, 0, 0, 0, 0, 0)
    home_win = int(home > away); draw = int(home == away); away_win = int(away > home)
    total = home + away; over_1_5 = int(total > 1); over_2_5 = int(total > 2); btts = int(home > 0 and away > 0)
    return home_win, draw, away_win, over_1_5, over_2_5, btts

def to_utc_iso(dt_str: str) -> str:
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return dt_str

def date_range_iso(past_days: int, future_days: int = 0) -> Tuple[str, str]:
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=past_days)
    end = today + timedelta(days=future_days)
    return start.isoformat(), end.isoformat()

def iter_dates_iso(start_date: str, end_date: str):
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    d = start
    while d <= end:
        yield d.isoformat()
        d += timedelta(days=1)

def fmt_score(h: Optional[int], a: Optional[int]) -> str:
    if h is None or a is None: return "-:-"
    return f"{h}-{a}"

def main() -> int:
    start_time = time.time()
    totals = {
        "inserted": 0, "updated": 0, "skipped": 0,
        "finished_seen": 0, "stats_calls_attempted": 0,
        "fixtures_with_stats": 0, "fixtures_missing_stats": 0,
        "stats_api_errors": 0, "requests_made": 0,
        "db_rows_checked": 0, "stats_skipped_already_complete": 0,
        "upcoming_written": 0,
    }
    per_league_counts: Dict[str, int] = {}
    upcoming_per_league: Dict[str, int] = {}

    try:
        env = load_env(ENV_PATH)
        API_KEY = env["APIFOOTBALL_KEY"]
    except Exception as e:
        print(f"[FATAL] .env load/config error: {e}", file=sys.stderr); return 3

    client = ApiClient(API_KEY, REQUESTS_PER_SECOND, DAILY_REQUEST_CAP)
    gate = CoverageGate(client, force=FORCE_STATS)

    try:
        conn = connect_db(DB_PATH)
    except Exception as e:
        print(f"[FATAL] DB error: {e}", file=sys.stderr); return 2

    # Prepare upcoming table
    try:
        ensure_upcoming_table(conn, UPCOMING_TABLE_MODE)
        conn.commit()
    except Exception as e:
        print(f"[FATAL] Upcoming table error: {e}", file=sys.stderr); return 2

    try:
        league_ids = load_league_ids()
    except Exception as e:
        print(f"[FATAL] League selection error: {e}", file=sys.stderr); return 3
    allowed_league_ids = set(league_ids)

    workloads: List[Tuple[int, Dict[str, Any], str]] = []
    if USE_SEASONS_MODE and USE_LAST_X_DAYS_MODE:
        print("[FATAL] Choose exactly ONE ingestion mode.", file=sys.stderr); return 3
    if not USE_SEASONS_MODE and not USE_LAST_X_DAYS_MODE:
        print("[FATAL] No ingestion mode enabled.", file=sys.stderr); return 3

    if USE_SEASONS_MODE:
        for lg in league_ids:
            workloads.append((lg, {}, f"=== League {lg} | Mode: Seasons={SEASONS} ==="))
    else:
        date_from, date_to = date_range_iso(LAST_X_DAYS, UPCOMING_WINDOW_DAYS)
        # banners (for visibility only)
        for lg in league_ids:
            workloads.append((lg, {"from": date_from, "to": date_to}, f"=== League {lg} | Mode: Window {date_from} -> {date_to} (past {LAST_X_DAYS} / next {UPCOMING_WINDOW_DAYS}) ==="))

    for _, _, banner in workloads:
        print(banner)

    try:
        if USE_SEASONS_MODE:
            for lg, _extra, _banner in workloads:
                for season in SEASONS:
                    data = client.get("/fixtures", {"league": lg, "season": season, "timezone": TIMEZONE})
                    for fx in data.get("response", []):
                        process_fixture(fx, conn, client, gate, totals, per_league_counts, upcoming_per_league)
        else:
            # Per-day fetch across the whole window (single call per day),
            # then filter locally to the allowlist.
            date_from, date_to = workloads[0][1]["from"], workloads[0][1]["to"]
            for day in iter_dates_iso(date_from, date_to):
                params = {"date": day, "timezone": TIMEZONE}
                data = client.get("/fixtures", params)
                resp = data.get("response", []) or []
                if not resp and PRINT_EACH_FIXTURE:
                    print(f"[INFO] No fixtures at all on {day}")

                kept = 0
                total = len(resp)
                for fx in resp:
                    lg_obj = fx.get("league", {}) or {}
                    lg_id = lg_obj.get("id")
                    if lg_id in allowed_league_ids:
                        kept += 1
                        process_fixture(fx, conn, client, gate, totals, per_league_counts, upcoming_per_league)
                if PRINT_EACH_FIXTURE:
                    print(f"[INFO] {day}: kept {kept}/{total} fixtures in allowlist")

        conn.commit()
        totals["requests_made"] = client.rate.count

    except Exception as e:
        totals["requests_made"] = client.rate.count
        summary = build_summary(totals, per_league_counts, upcoming_per_league, start_time, fatal=str(e))
        print(summary)
        if SEND_EMAIL:
            try: send_email_summary(summary, env)
            except Exception as em_err: print(f"[WARN] email send failed: {em_err}", file=sys.stderr)
        try: conn.close()
        except Exception: pass
        return 4

    try: conn.close()
    except Exception: pass

    summary = build_summary(totals, per_league_counts, upcoming_per_league, start_time, fatal=None)
    print(summary)
    if SEND_EMAIL:
        try: send_email_summary(summary, env)
        except Exception as em_err: print(f"[WARN] email send failed: {em_err}", file=sys.stderr)
    return 0

def process_fixture(fx: Dict[str, Any],
                    conn: sqlite3.Connection,
                    client: ApiClient,
                    gate: CoverageGate,
                    totals: Dict[str, int],
                    per_league_counts: Dict[str, int],
                    upcoming_per_league: Dict[str, int]) -> None:
    fixture = fx.get("fixture", {}) or {}
    league = fx.get("league", {}) or {}
    teams = fx.get("teams", {}) or {}
    goals = fx.get("goals", {}) or {}

    fixture_id = int(fixture.get("id"))
    status = (fixture.get("status", {}) or {})
    status_short = status.get("short")
    status_long = status.get("long")
    date_iso = to_utc_iso(fixture.get("date"))

    league_id = int(league.get("id")) if league.get("id") is not None else None
    season = int(league.get("season")) if league.get("season") is not None else None
    league_name = league.get("name"); country = league.get("country")

    home_team_name = (teams.get("home", {}) or {}).get("name")
    away_team_name = (teams.get("away", {}) or {}).get("name")
    home_team_id = (teams.get("home", {}) or {}).get("id")
    away_team_id = (teams.get("away", {}) or {}).get("id")

    home_goals = goals.get("home")
    away_goals = goals.get("away")

    # Decide path: finished vs upcoming
    is_finished = status_short in FINISHED_STATUS

    if is_finished:
        # Existing finished-flow (with stats + upsert into fixtures)
        home_win, draw, away_win, o15, o25, btts = derive_outcomes(home_goals, away_goals)
        row: Dict[str, Any] = {c: None for c in ALL_COLUMNS}
        row.update({
            "fixture_id": fixture_id, "date": date_iso,
            "league_id": league_id, "season": season,
            "home_team": home_team_name, "away_team": away_team_name,
            "league_name": league_name, "country": country,
            "home_score": home_goals, "away_score": away_goals,
            "home_win": home_win, "draw": draw, "away_win": away_win,
            "over_1_5": o15, "over_2_5": o25, "btts": btts,
            "last_updated": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        })

        existing = get_existing_fixture(conn, fixture_id)
        if existing: totals["db_rows_checked"] += 1

        stats_status = "SKIP"
        if season is not None and league_id is not None:
            totals["finished_seen"] += 1
            if existing and is_row_stats_complete(existing):
                totals["stats_skipped_already_complete"] += 1
                # carry existing stats forward
                for col in ALL_STAT_COLS: row[col] = existing.get(col)
                stats_status = "OK_DB"
            else:
                if gate.stats_available(league_id, season):
                    try:
                        data = client.get("/fixtures/statistics", {"fixture": fixture_id})
                        totals["stats_calls_attempted"] += 1
                        home_stats, away_stats = map_statistics_payload(
                            data, home_team_id, home_team_name, away_team_id, away_team_name
                        )
                        incoming = {}
                        for k, v in home_stats.items(): incoming[f"home_{k}"] = v
                        for k, v in away_stats.items(): incoming[f"away_{k}"] = v
                        merged = merge_stats_preserving_existing(existing, incoming)
                        row.update(merged)
                        nonnull_home = sum(1 for k in HOME_STAT_COLS if row.get(k) is not None)
                        nonnull_away = sum(1 for k in AWAY_STAT_COLS if row.get(k) is not None)
                        if nonnull_home > 0 or nonnull_away > 0:
                            totals["fixtures_with_stats"] += 1
                            stats_status = "LOADED"
                        else:
                            totals["fixtures_missing_stats"] += 1
                            stats_status = "MISSING"
                    except Exception:
                        totals["stats_api_errors"] += 1
                        if existing:
                            for col in ALL_STAT_COLS: row[col] = existing.get(col)
                        stats_status = "ERR"
                else:
                    if existing:
                        for col in ALL_STAT_COLS: row[col] = existing.get(col)
                    stats_status = "COVERAGE_OFF"

        # Compute deriveds and upsert
        row.update(compute_derived(row))
        try:
            inserted = upsert_fixture(conn, row)
            action = "INSERT" if inserted else "UPDATE"
            if inserted: totals["inserted"] += 1
            else: totals["updated"] += 1
            # Sanity: ensure it is not present in upcoming
            conn.execute('DELETE FROM "upcoming_fixtures" WHERE "fixture_id"=?;', (fixture_id,))
        except Exception as e:
            totals["skipped"] += 1; action = "SKIP"; stats_status = f"ERR:{str(e)[:40]}"

        # per-league processed (finished)
        lg_key = league_key(league_name, country, league_id, season)
        per_league_counts[lg_key] = per_league_counts.get(lg_key, 0) + 1

        if PRINT_EACH_FIXTURE:
            score_txt = fmt_score(home_goals, away_goals)
            date_txt = (date_iso or "")[:10]
            lg_txt = f"L{league_id or '?'}-{season or '?'}"
            home = home_team_name or "Home"; away = away_team_name or "Away"
            print(f"[{action}] {fixture_id} | {date_txt} | {lg_txt} | {home} {score_txt} {away} | {status_short or ''} | stats:{stats_status}")

    else:
        # Upcoming path -> write to upcoming_fixtures
        up_row = {
            "fixture_id": fixture_id,
            "date": date_iso,
            "league_id": league_id,
            "season": season,
            "home_team": home_team_name,
            "away_team": away_team_name,
            "league_name": league_name,
            "country": country,
            "status_short": status_short,
            "status_long": status_long,
            "last_updated": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }
        try:
            upsert_upcoming(conn, up_row)
            totals["upcoming_written"] += 1
            lg_key = league_key(league_name, country, league_id, season)
            upcoming_per_league[lg_key] = upcoming_per_league.get(lg_key, 0) + 1
            if PRINT_EACH_FIXTURE:
                date_txt = (date_iso or "")[:10]
                lg_txt = f"L{league_id or '?'}-{season or '?'}"
                home = home_team_name or "Home"; away = away_team_name or "Away"
                print(f"[UPCOMING] {fixture_id} | {date_txt} | {lg_txt} | {home} vs {away} | {status_short or ''}")
        except Exception as e:
            totals["skipped"] += 1
            if PRINT_EACH_FIXTURE:
                print(f"[SKIP] {fixture_id} | ERR:{str(e)[:40]}")

def league_key(name: Optional[str], country: Optional[str], league_id: Optional[int], season: Optional[int]) -> str:
    n = name or "Unknown"
    c = country or "Unknown"
    lid = f"L{league_id or '?'}"
    s = f"{season or '?'}"
    return f"{n} ({c}) [{lid}] {s}"

def build_summary(totals: Dict[str, int],
                  per_league_counts: Dict[str, int],
                  upcoming_per_league: Dict[str, int],
                  start_time: float,
                  fatal: Optional[str]) -> str:
    dur = time.time() - start_time
    lines = []
    lines.append("=== Able101 Fixtures Ingest - Summary ===")
    lines.append(f"Duration: {dur:.1f}s")
    lines.append(f"Requests: {totals.get('requests_made', 0)}")
    lines.append(f"Inserted: {totals['inserted']} | Updated: {totals['updated']} | Skipped: {totals['skipped']}")
    lines.append(f"Finished seen: {totals['finished_seen']}")
    lines.append("Stats: attempts={a}, loaded={l}, missing={m}, api_errors={e}, skipped_complete={s}".format(
        a=totals["stats_calls_attempted"], l=totals["fixtures_with_stats"],
        m=totals["fixtures_missing_stats"], e=totals["stats_api_errors"],
        s=totals["stats_skipped_already_complete"],
    ))
    lines.append(f"DB rows checked: {totals['db_rows_checked']}")
    if per_league_counts:
        lines.append("Per-league processed (finished/historical):")
        for k in sorted(per_league_counts.keys()):
            lines.append(f"  - {k}: {per_league_counts[k]}")
    # New upcoming block
    lines.append("Upcoming fixtures written: {u}".format(u=totals["upcoming_written"]))
    if upcoming_per_league:
        lines.append("Upcoming by league (next {d} days):".format(d=UPCOMING_WINDOW_DAYS))
        for k in sorted(upcoming_per_league.keys()):
            lines.append(f"  - {k}: {upcoming_per_league[k]}")
    if fatal:
        lines.append(f"FATAL: {fatal}")
    return "\n".join(lines)

def send_email_summary(body: str, env: Dict[str, str]) -> None:
    EMAIL_FROM = env.get("EMAIL_FROM", ""); EMAIL_TO = env.get("EMAIL_TO", "")
    SMTP_HOST = env.get("SMTP_HOST", ""); SMTP_PORT = int(env.get("SMTP_PORT", "587"))
    SMTP_USER = env.get("SMTP_USER", ""); SMTP_PASS = env.get("SMTP_PASS", "")
    if not (EMAIL_FROM and EMAIL_TO and SMTP_HOST): return
    msg = EmailMessage(); msg["Subject"] = "Able101: Fixtures Ingestion Summary"
    msg["From"] = EMAIL_FROM; msg["To"] = EMAIL_TO; msg.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls(context=context)
        if SMTP_USER and SMTP_PASS: server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

if __name__ == "__main__":
    rc = 0
    try:
        rc = main()
    except Exception as e:
        print(f"[FATAL] Uncaught exception: {e}\n{traceback.format_exc()}", file=sys.stderr); rc = 2
    sys.exit(rc)

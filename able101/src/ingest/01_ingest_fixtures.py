"""
Able101 Fixtures + Stats Ingestion (API-Football -> SQLite)

- Respects existing 'fixtures' schema (no DDL).
- Simple top-of-file config with comment-to-switch modes.
- Two modes:
  A) Historical by explicit SEASONS (e.g., [2023])
  B) Rolling window by LAST_X_DAYS (e.g., 7)
- Pull fixtures; for finished fixtures also pull statistics.
- Deduce home_win/draw/away_win, over_1_5/over_2_5, btts.
- Rate limit and daily cap guards.
- Prints each processed fixture; emails a concise summary on completion.
"""

# --- CONFIG (EDIT ME) -------------------------------------------------------

# Paths
DB_PATH = "/mnt/able101_usb/database/able101.sqlite"
ENV_PATH = "/home/zentr/ProjectAbleA/able101/config/.env"  # contains API key + email creds
LEAGUE_ALLOWLIST_PATH = "/mnt/able101_usb/config/league_allowlist.txt"

# League selection
USE_ALLOWLIST = False      # True = read leagues from allow-list file below
# To override with explicit leagues, set USE_ALLOWLIST = False and list them here:
OVERRIDE_LEAGUES = [364]    # e.g., [39]

# Ingestion window (choose exactly one mode)
# Option A) Historical seasons
USE_SEASONS_MODE = True
SEASONS = [2023]          # used only if USE_SEASONS_MODE = True

# Option B) Rolling window by days
USE_LAST_X_DAYS_MODE = False
LAST_X_DAYS = 7

# API pacing (set to your plan; you said 6 rps)
REQUESTS_PER_SECOND = 6.0
DAILY_REQUEST_CAP = 70000

# Logging / email
PRINT_EACH_FIXTURE = True
SEND_EMAIL = True

# ---------------------------------------------------------------------------

import os
import sys
import time
import json
import sqlite3
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
import urllib.request
import urllib.parse

# --- Helper: read .env ------------------------------------------------------

def load_env(env_path: str) -> dict:
    env = {}
    if not os.path.exists(env_path):
        print(f"[WARN] ENV file not found: {env_path}")
        return env
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env

# --- Rate limiter -----------------------------------------------------------

class Pace:
    def __init__(self, rps: float, daily_cap: int):
        self.min_interval = 1.0 / max(rps, 0.0001)
        self.daily_cap = daily_cap
        self.count = 0
        self.last = 0.0

    def wait(self):
        now = time.time()
        sleep_for = self.min_interval - (now - self.last)
        if sleep_for > 0:
            time.sleep(sleep_for)
        self.last = time.time()
        self.count += 1
        if self.count > self.daily_cap:
            raise RuntimeError(f"Daily request cap exceeded ({self.daily_cap}).")

# --- API-Football v3 tiny client -------------------------------------------

class APIFootballClient:
    BASE = "https://v3.football.api-sports.io"

    def __init__(self, api_key: str, pace: Pace):
        self.api_key = api_key
        self.pace = pace
        self.headers = {
            "x-apisports-key": api_key,
            "Accept": "application/json"
        }

    def _get(self, path: str, params: dict) -> dict:
        self.pace.wait()
        url = f"{self.BASE}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)
        req = urllib.request.Request(url, headers=self.headers)
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
        return json.loads(data.decode("utf-8"))

    def fixtures_by_league_season(self, league: int, season: int) -> list:
        data = self._get("/fixtures", {"league": league, "season": season})
        return data.get("response", [])

    def fixtures_by_date_range(self, date_from: str, date_to: str, league: int = None) -> list:
        params = {"from": date_from, "to": date_to}
        if league:
            params["league"] = league
        data = self._get("/fixtures", params)
        return data.get("response", [])

    def fixture_statistics(self, fixture_id: int) -> list:
        data = self._get("/fixtures/statistics", {"fixture": fixture_id})
        return data.get("response", [])

# --- DB helpers -------------------------------------------------------------

def connect_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def ensure_fixtures_table(con: sqlite3.Connection):
    """Verify the canonical fixtures table exists; do not mutate schema."""
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fixtures'"
    ).fetchone()
    if row is None:
        raise RuntimeError(
            "fixtures table not found at DB_PATH; create it first (we never auto-create)."
        )

def upsert_fixture(con: sqlite3.Connection, row: dict):
    """INSERT or UPDATE a fixture row, quoting identifiers for columns like home_passes_%."""
    cols = list(row.keys())
    def qcol(c: str) -> str:
        return '"' + c.replace('"', '""') + '"'
    col_list = ", ".join(qcol(c) for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    values = tuple(row[c] for c in cols)
    update_clause = ", ".join(f"{qcol(c)}=excluded.{qcol(c)}"
                              for c in cols if c != "fixture_id")
    sql = (
        f'INSERT INTO fixtures ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT("fixture_id") DO UPDATE SET {update_clause};'
    )
    con.execute(sql, values)

# --- Stats extraction helpers ----------------------------------------------

def _val(stats, team: str, name: str):
    """Extract value by team ('home'|'away') and stat name from /fixtures/statistics response."""
    if not stats:
        return None
    idx = 0 if team == "home" else 1
    if idx < len(stats):
        items = stats[idx].get("statistics", [])
        for it in items:
            t = (it.get("type") or "")
            if t.lower() == name.lower():
                val = it.get("value")
                if isinstance(val, str) and val.endswith("%"):
                    try:
                        return float(val.strip("%"))
                    except Exception:
                        return None
                if isinstance(val, (int, float)):
                    return float(val)
                return None
    return None

def deduce_outcomes(home_score, away_score):
    if home_score is None or away_score is None:
        return (None, None, None, None, None, None)
    home_win = 1 if home_score > away_score else 0
    draw = 1 if home_score == away_score else 0
    away_win = 1 if away_score > home_score else 0
    total = (home_score or 0) + (away_score or 0)
    over_1_5 = 1 if total > 1.5 else 0
    over_2_5 = 1 if total > 2.5 else 0
    btts = 1 if (home_score > 0 and away_score > 0) else 0
    return (home_win, draw, away_win, over_1_5, over_2_5, btts)

def parse_stats_to_row(stats, base):
    row = dict(base)
    row["home_ball_possession"] = _val(stats, "home", "Ball Possession")
    row["away_ball_possession"] = _val(stats, "away", "Ball Possession")

    row["home_shots_on_goal"]   = _val(stats, "home", "Shots on Goal")
    row["home_shots_off_goal"]  = _val(stats, "home", "Shots off Goal")
    row["home_total_shots"]     = _val(stats, "home", "Total Shots")
    row["home_blocked_shots"]   = _val(stats, "home", "Blocked Shots")
    row["home_shots_insidebox"] = _val(stats, "home", "Shots insidebox")
    row["home_shots_outsidebox"]= _val(stats, "home", "Shots outsidebox")

    row["away_shots_on_goal"]   = _val(stats, "away", "Shots on Goal")
    row["away_shots_off_goal"]  = _val(stats, "away", "Shots off Goal")
    row["away_total_shots"]     = _val(stats, "away", "Total Shots")
    row["away_blocked_shots"]   = _val(stats, "away", "Blocked Shots")
    row["away_shots_insidebox"] = _val(stats, "away", "Shots insidebox")
    row["away_shots_outsidebox"]= _val(stats, "away", "Shots outsidebox")

    row["home_fouls"]           = _val(stats, "home", "Fouls")
    row["home_offsides"]        = _val(stats, "home", "Offsides")
    row["home_corner_kicks"]    = _val(stats, "home", "Corner Kicks")

    row["away_fouls"]           = _val(stats, "away", "Fouls")
    row["away_offsides"]        = _val(stats, "away", "Offsides")
    row["away_corner_kicks"]    = _val(stats, "away", "Corner Kicks")

    row["home_yellow_cards"]    = _val(stats, "home", "Yellow Cards")
    row["home_red_cards"]       = _val(stats, "home", "Red Cards")
    row["home_goalkeeper_saves"]= _val(stats, "home", "Goalkeeper Saves")

    row["away_yellow_cards"]    = _val(stats, "away", "Yellow Cards")
    row["away_red_cards"]       = _val(stats, "away", "Red Cards")
    row["away_goalkeeper_saves"]= _val(stats, "away", "Goalkeeper Saves")

    row["home_total_passes"]    = _val(stats, "home", "Total passes")
    row["home_passes_accurate"] = _val(stats, "home", "Passes accurate")
    row["home_passes_%"]        = _val(stats, "home", "Passes %")

    row["away_total_passes"]    = _val(stats, "away", "Total passes")
    row["away_passes_accurate"] = _val(stats, "away", "Passes accurate")
    row["away_passes_%"]        = _val(stats, "away", "Passes %")

    return row

# --- Allow list -------------------------------------------------------------

def load_league_allowlist(path: str) -> list:
    leagues = []
    if not os.path.exists(path):
        print(f"[WARN] Allow-list file not found: {path}")
        return leagues
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            try:
                leagues.append(int(s))
            except Exception:
                print(f"[WARN] Skipping invalid league id in allow-list: {s}")
    return leagues

# --- Email ------------------------------------------------------------------

def send_email_summary(env: dict, subject: str, body: str):
    if not env.get("SMTP_HOST"):
        print("[WARN] No SMTP_HOST in env; skipping email.")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = env.get("EMAIL_FROM", env.get("SMTP_USER", "noreply@example.com"))
    msg["To"] = env.get("EMAIL_TO", env.get("SMTP_USER", ""))

    with smtplib.SMTP(env["SMTP_HOST"], int(env.get("SMTP_PORT", 587))) as s:
        s.starttls()
        s.login(env["SMTP_USER"], env["SMTP_PASS"])
        s.send_message(msg)

# --- Row builder ------------------------------------------------------------

def fixture_row_base(fix: dict):
    f = fix.get("fixture", {})
    t = fix.get("teams", {})
    g = fix.get("goals", {})
    l = fix.get("league", {})

    fixture_id = f.get("id")
    date_iso = f.get("date")
    status_short = f.get("status", {}).get("short")

    league_id = l.get("id")
    season = l.get("season")
    league_name = l.get("name")
    country = l.get("country")

    home_team = (t.get("home", {}) or {}).get("name")
    away_team = (t.get("away", {}) or {}).get("name")

    home_score = g.get("home")
    away_score = g.get("away")

    (home_win, draw, away_win, over_1_5, over_2_5, btts) = deduce_outcomes(home_score, away_score)

    row = {
        "fixture_id": fixture_id,
        "date": date_iso,
        "league_id": league_id,
        "season": season,
        "home_team": home_team,
        "away_team": away_team,
        "league_name": league_name,
        "country": country,
        "home_score": home_score,
        "away_score": away_score,
        "home_win": home_win,
        "draw": draw,
        "away_win": away_win,
        "over_1_5": over_1_5,
        "over_2_5": over_2_5,
        "btts": btts,
        "home_shots_on_goal": None,
        "home_shots_off_goal": None,
        "home_total_shots": None,
        "home_blocked_shots": None,
        "home_shots_insidebox": None,
        "home_shots_outsidebox": None,
        "home_fouls": None,
        "home_corner_kicks": None,
        "home_offsides": None,
        "home_ball_possession": None,
        "home_yellow_cards": None,
        "home_red_cards": None,
        "home_goalkeeper_saves": None,
        "home_total_passes": None,
        "home_passes_accurate": None,
        "home_passes_%": None,
        "away_shots_on_goal": None,
        "away_shots_off_goal": None,
        "away_total_shots": None,
        "away_blocked_shots": None,
        "away_shots_insidebox": None,
        "away_shots_outsidebox": None,
        "away_fouls": None,
        "away_corner_kicks": None,
        "away_offsides": None,
        "away_ball_possession": None,
        "away_yellow_cards": None,
        "away_red_cards": None,
        "away_goalkeeper_saves": None,
        "away_total_passes": None,
        "away_passes_accurate": None,
        "away_passes_%": None,
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    return row, status_short

# --- Main -------------------------------------------------------------------

def main():
    start_ts = time.time()
    env = load_env(ENV_PATH)
    api_key = env.get("APIFOOTBALL_KEY")
    if not api_key:
        print(f"[ERROR] APIFOOTBALL_KEY not found in {ENV_PATH}")
        sys.exit(1)

    if USE_ALLOWLIST:
        leagues = load_league_allowlist(LEAGUE_ALLOWLIST_PATH)
    else:
        leagues = list(OVERRIDE_LEAGUES or [])
    if not leagues:
        print("[WARN] No leagues configured. Exiting.")
        return

    today = datetime.utcnow().date()
    if USE_SEASONS_MODE and not USE_LAST_X_DAYS_MODE:
        seasons = list(SEASONS or [])
        if not seasons:
            print("[WARN] No seasons configured. Exiting.")
            return
        mode_desc = f"Seasons={seasons}"
    elif USE_LAST_X_DAYS_MODE and not USE_SEASONS_MODE:
        date_to = today
        date_from = today - timedelta(days=int(LAST_X_DAYS))
        seasons = None
        mode_desc = f"Last {LAST_X_DAYS} days ({date_from} -> {date_to})"
    else:
        print("[ERROR] Configure exactly one mode: SEASONS or LAST_X_DAYS.")
        sys.exit(2)

    pace = Pace(REQUESTS_PER_SECOND, DAILY_REQUEST_CAP)
    api = APIFootballClient(api_key, pace)

    con = connect_db(DB_PATH)
    ensure_fixtures_table(con)

    totals = {"inserted": 0, "updated": 0, "skipped": 0, "per_league": {}}
    def bump_league(league_id, key):
        d = totals["per_league"].setdefault(league_id, {"inserted":0,"updated":0,"skipped":0})
        d[key] += 1

    error_text = None

    try:
        for league_id in leagues:
            print(f"\n=== League {league_id} | Mode: {mode_desc} ===")
            if seasons:
                fixtures = []
                for season in seasons:
                    print(f"Fetching fixtures for league={league_id} season={season} ...")
                    fixtures += api.fixtures_by_league_season(league_id, season)
            else:
                date_to = today
                date_from = today - timedelta(days=int(LAST_X_DAYS))
                print(f"Fetching fixtures for league={league_id} {date_from}->{date_to} ...")
                fixtures = api.fixtures_by_date_range(date_from.isoformat(), date_to.isoformat(), league=league_id)

            for fix in fixtures:
                base_row, status_short = fixture_row_base(fix)
                fid = base_row["fixture_id"]

                stats_row = dict(base_row)
                if status_short in ("FT", "AET", "PEN"):
                    stats = api.fixture_statistics(fid)
                    stats_row = parse_stats_to_row(stats, base_row)

                cur = con.execute("SELECT last_updated FROM fixtures WHERE fixture_id=?", (fid,)).fetchone()
                if cur is None:
                    upsert_fixture(con, stats_row)
                    totals["inserted"] += 1
                    bump_league(league_id, "inserted")
                    action = "INSERT"
                else:
                    upsert_fixture(con, stats_row)
                    totals["updated"] += 1
                    bump_league(league_id, "updated")
                    action = "UPDATE"

                if PRINT_EACH_FIXTURE:
                    hs = stats_row.get("home_score")
                    as_ = stats_row.get("away_score")
                    print(f"[{action}] {fid} | {stats_row['date']} | {stats_row['league_name']} {stats_row['season']} | "
                          f"{stats_row['home_team']} {hs}-{as_} {stats_row['away_team']} | status={status_short}")

            con.commit()
    except Exception as e:
        error_text = str(e)
        print(f"[ERROR] {error_text}")
    finally:
        con.commit()
        con.close()

    dur = time.time() - start_ts
    per_league_lines = []
    for lid, counts in sorted(totals["per_league"].items()):
        per_league_lines.append(f"  League {lid}: +{counts['inserted']} / upd {counts['updated']} / skip {counts['skipped']}")

    summary = f"""Able101 Ingestion Summary
Run at (UTC): {datetime.utcnow().isoformat(timespec='seconds')}
Mode: {mode_desc}
Leagues: {leagues}

Totals:
  Inserted: {totals['inserted']}
  Updated : {totals['updated']}
  Skipped : {totals['skipped']}

Per league:
{os.linesep.join(per_league_lines) if per_league_lines else '  (none)'}

Duration: {dur:.1f}s
Request count (this run): {pace.count}
"""

    if error_text:
        summary += f"\nERROR: {error_text}\n"

    print("\n" + "="*60)
    print(summary)
    print("="*60 + "\n")

    if SEND_EMAIL:
        try:
            subj = f"Able101 Ingestion: {totals['inserted']} new, {totals['updated']} upd | reqs={pace.count}"
            send_email_summary(env, subj, summary)
        except Exception as e:
            print(f"[WARN] Email sending failed: {e}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import json, os, sqlite3, sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CFG_PATH = os.path.join(ROOT, "config", "able101.json")
ENV_PATH = os.path.join(ROOT, "config", ".env")

def load_config():
    with open(CFG_PATH, "r") as f: return json.load(f)

def load_env():
    env = {}
    if os.path.exists(ENV_PATH):
        for line in open(ENV_PATH):
            line=line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k,v = line.split("=",1); env[k.strip()] = v.strip()
    return env

def db_connect(path):
    con = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fixtures (
          fixture_id INTEGER PRIMARY KEY,
          league_id INTEGER, season INTEGER,
          date_utc TEXT, home_team TEXT, away_team TEXT,
          status TEXT, created_at TEXT
        )
    """)
    return con

def main():
    cfg = load_config()
    env = load_env()
    if not env.get("API_FOOTBALL_KEY"):
        print("FATAL: API_FOOTBALL_KEY missing in able101/config/.env", file=sys.stderr); sys.exit(1)

    con = db_connect(cfg["database_path"])
    demo_id = int(datetime.utcnow().timestamp())
    con.execute("""INSERT OR IGNORE INTO fixtures
      (fixture_id, league_id, season, date_utc, home_team, away_team, status, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
      (demo_id, 0, datetime.utcnow().year, datetime.utcnow().isoformat(timespec="seconds"),
       "HOME_DEMO","AWAY_DEMO","NS", datetime.utcnow().isoformat(timespec="seconds")))
    con.commit(); con.close()
    print(f"[able101] demo fixture inserted id={demo_id} into {cfg['database_path']}")

if __name__ == "__main__":
    main()

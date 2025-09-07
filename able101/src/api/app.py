# Simple Able101 Micro-API (read-only fixtures + write predictions)
# Run: uvicorn app:app --host 0.0.0.0 --port 8080
import os, sqlite3, hashlib, time
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Header, Query

DB_PATH = "/mnt/able101_usb/database/able101.sqlite"
API_TOKEN = os.environ.get("ABLE101_API_TOKEN", "changeme")

def rows(q, params=()):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(q, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        con.close()

def execmany(q, seq):
    con = sqlite3.connect(DB_PATH)
    try:
        con.executemany(q, seq)
        con.commit()
    finally:
        con.close()

app = FastAPI(title="Able101 Micro-API")

def auth(x_token: Optional[str]):
    if x_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/health")
def health():
    h = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
    return {"ok": True, "ts": time.time(), "nonce": h}

@app.get("/fixtures/upcoming")
def get_upcoming(
    x_token: Optional[str] = Header(None),
    leagues: Optional[str] = Query(None, description="comma-separated league_id list"),
    days_ahead: int = 21,
    limit: int = 1000,
    offset: int = 0,
):
    auth(x_token)
    league_filter = ""
    params = []
    if leagues:
        ids = [int(x) for x in leagues.split(",")]
        league_filter = f" AND league_id IN ({','.join(['?']*len(ids))}) "
        params.extend(ids)

    q = f"""
    SELECT fixture_id, date, league_id, season, home_team, away_team
    FROM fixtures
    WHERE date >= DATE('now')
      AND date <= DATE('now', '+' || ? || ' days')
      {league_filter}
    ORDER BY date ASC
    LIMIT ? OFFSET ?;
    """
    params = [days_ahead] + params + [limit, offset]
    return {"items": rows(q, params), "limit": limit, "offset": offset}

@app.get("/fixtures/historical")
def get_historical(
    x_token: Optional[str] = Header(None),
    season: int = Query(...),
    leagues: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = 1000,
    offset: int = 0,
):
    auth(x_token)
    where = ["1=1", "home_score IS NOT NULL", "away_score IS NOT NULL", "season = ?"]
    params = [season]
    if leagues:
        ids = [int(x) for x in leagues.split(",")]
        where.append(f"league_id IN ({','.join(['?']*len(ids))})")
        params.extend(ids)
    if date_from: where.append("date >= ?"); params.append(date_from)
    if date_to:   where.append("date <= ?"); params.append(date_to)

    q = f"""
    SELECT fixture_id, date, league_id, season, home_team, away_team,
           home_score, away_score,
           home_total_shots, away_total_shots,
           home_shots_on_goal, away_shots_on_goal,
           home_corner_kicks, away_corner_kicks,
           home_ball_possession, away_ball_possession
    FROM fixtures
    WHERE {' AND '.join(where)}
    ORDER BY date ASC
    LIMIT ? OFFSET ?;
    """
    params = params + [limit, offset]
    return {"items": rows(q, params), "limit": limit, "offset": offset}

@app.post("/able102/predictions")
def post_predictions(payload: List[dict], x_token: Optional[str] = Header(None)):
    auth(x_token)
    # Minimal schema validation
    required = {"fixture_id","model_name","model_version","created_at"}
    for i, row in enumerate(payload):
        if not required.issubset(row):
            raise HTTPException(status_code=400, detail=f"Row {i} missing required keys {required}")
    execmany("""
        INSERT OR REPLACE INTO able102_predictions
        (fixture_id, model_name, model_version, prob_home, prob_draw, prob_away, pred_label, created_at)
        VALUES (:fixture_id, :model_name, :model_version, :prob_home, :prob_draw, :prob_away, :pred_label, :created_at)
    """, payload)
    return {"ok": True, "n": len(payload)}

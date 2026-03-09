
import os
import math
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from app.database import get_connection, init_db, DB_PATH
logger = logging.getLogger("api")

app = FastAPI(
    title="Raqamli Iqtisodiyot Dashboard API",
    description="Digital Economy Statistics of Uzbekistan – stat.uz",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.on_event("startup")
async def startup():
    init_db()

    import fcntl, tempfile, pathlib

    seed_flag = pathlib.Path(DB_PATH + ".seeded")
    seed_lock = pathlib.Path(DB_PATH + ".seedlock")

    lock_fd = open(seed_lock, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        if not seed_flag.exists():
            conn = get_connection()
            cnt  = conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0]
            conn.close()

            if cnt == 0:
                logger.info("DB empty – seeding sample data…")
                from app.seed_data import seed_all
                seed_all()

            seed_flag.touch()
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()

    sched_flag = pathlib.Path(DB_PATH + ".schedpid")
    try:
        existing_pid = int(sched_flag.read_text()) if sched_flag.exists() else None
        if existing_pid and existing_pid != os.getpid():
            try:
                os.kill(existing_pid, 0)
                raise OSError
            except OSError:
                pass

        sched_flag.write_text(str(os.getpid()))
        from app.scheduler import start_scheduler
        start_scheduler()
    except Exception as e:
        logger.warning(f"Scheduler not started: {e}")



FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/", include_in_schema=False)
async def serve_frontend():
    index = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return JSONResponse({"message": "Digital Economy Dashboard API", "docs": "/docs"})



@app.get("/api/categories")
async def list_categories(lang: str = Query("uz", regex="^(uz|en|ru)$")):
    """Return all indicator categories."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, slug, icon, name_uz, name_en, name_ru FROM categories ORDER BY id"
    ).fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "slug": r["slug"],
            "icon": r["icon"],
            "name": r[f"name_{lang}"] or r["name_uz"],
        }
        for r in rows
    ]


@app.get("/api/indicators")
async def list_indicators(
    category: Optional[str] = None,
    lang: str = Query("uz", regex="^(uz|en|ru)$"),
):
    conn = get_connection()
    if category:
        rows = conn.execute(
            """SELECT i.id, i.slug, i.name_uz, i.name_en, i.name_ru,
                      i.unit_uz, i.unit_en, c.slug as cat_slug, c.name_uz as cat_name_uz,
                      c.name_en as cat_name_en, c.name_ru as cat_name_ru, c.icon
               FROM indicators i JOIN categories c ON c.id=i.category_id
               WHERE c.slug=? ORDER BY i.id""",
            (category,),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT i.id, i.slug, i.name_uz, i.name_en, i.name_ru,
                      i.unit_uz, i.unit_en, c.slug as cat_slug, c.name_uz as cat_name_uz,
                      c.name_en as cat_name_en, c.name_ru as cat_name_ru, c.icon
               FROM indicators i JOIN categories c ON c.id=i.category_id ORDER BY i.id"""
        ).fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "slug": r["slug"],
            "name": r[f"name_{lang}"] or r["name_uz"],
            "unit": r[f"unit_{lang}"] if lang != "ru" else r["unit_uz"],
            "category": r["cat_slug"],
            "category_name": r[f"cat_name_{lang}"] or r["cat_name_uz"],
            "icon": r["icon"],
        }
        for r in rows
    ]


@app.get("/api/data/{indicator_slug}")
async def get_indicator_data(
    indicator_slug: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    lang: str = Query("uz", regex="^(uz|en|ru)$"),
):
    conn = get_connection()
    ind = conn.execute(
        "SELECT * FROM indicators WHERE slug=?", (indicator_slug,)
    ).fetchone()
    if not ind:
        conn.close()
        raise HTTPException(404, f"Indicator '{indicator_slug}' not found")

    query = "SELECT year, quarter, value FROM data_points WHERE indicator_id=?"
    params: list = [ind["id"]]
    if year_from:
        query += " AND year>=?"; params.append(year_from)
    if year_to:
        query += " AND year<=?"; params.append(year_to)
    query += " ORDER BY year, quarter"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    series = [{"year": r["year"], "quarter": r["quarter"], "value": r["value"]} for r in rows]
    values = [r["value"] for r in rows]

    return {
        "indicator": {
            "slug": ind["slug"],
            "name": ind[f"name_{lang}"] or ind["name_uz"],
            "unit": ind[f"unit_{lang}"] if lang != "ru" else ind["unit_uz"],
        },
        "series": series,
        "stats": _compute_stats(values),
    }


@app.get("/api/kpi")
async def get_kpi_summary(lang: str = Query("uz", regex="^(uz|en|ru)$")):
    """Return KPI summary: latest value + YoY growth for each indicator."""
    conn = get_connection()
    indicators = conn.execute(
        """SELECT i.id, i.slug, i.name_uz, i.name_en, i.name_ru,
                  i.unit_uz, i.unit_en, c.name_uz as cat_uz, c.name_en as cat_en,
                  c.name_ru as cat_ru, c.icon, c.slug as cat_slug
           FROM indicators i JOIN categories c ON c.id=i.category_id ORDER BY i.id"""
    ).fetchall()

    kpis = []
    for ind in indicators:
        rows = conn.execute(
            "SELECT year, value FROM data_points WHERE indicator_id=? ORDER BY year DESC LIMIT 2",
            (ind["id"],),
        ).fetchall()
        if not rows:
            continue
        latest = rows[0]
        prev   = rows[1] if len(rows) > 1 else None
        growth = None
        if prev and prev["value"] and prev["value"] != 0:
            growth = round((latest["value"] - prev["value"]) / prev["value"] * 100, 2)

        kpis.append({
            "slug": ind["slug"],
            "name": ind[f"name_{lang}"] or ind["name_uz"],
            "unit": ind[f"unit_{lang}"] if lang != "ru" else ind["unit_uz"],
            "category": ind["cat_slug"],
            "category_name": ind[f"cat_{lang}"] or ind["cat_uz"],
            "icon": ind["icon"],
            "latest_value": latest["value"],
            "latest_year": latest["year"],
            "yoy_growth_pct": growth,
        })
    conn.close()
    return kpis


@app.get("/api/compare")
async def compare_indicators(
    slugs: str = Query(..., description="Comma-separated indicator slugs"),
    lang: str = Query("uz", regex="^(uz|en|ru)$"),
):
    slug_list = [s.strip() for s in slugs.split(",") if s.strip()]
    if not slug_list:
        raise HTTPException(400, "No slugs provided")

    conn = get_connection()
    result = {}
    for slug in slug_list[:6]:  # cap at 6 series
        ind = conn.execute("SELECT * FROM indicators WHERE slug=?", (slug,)).fetchone()
        if not ind:
            continue
        rows = conn.execute(
            "SELECT year, value FROM data_points WHERE indicator_id=? ORDER BY year",
            (ind["id"],),
        ).fetchall()
        result[slug] = {
            "name": ind[f"name_{lang}"] or ind["name_uz"],
            "unit": ind[f"unit_{lang}"] if lang != "ru" else ind["unit_uz"],
            "data": [{"year": r["year"], "value": r["value"]} for r in rows],
        }
    conn.close()
    return result


@app.get("/api/growth")
async def growth_analysis(lang: str = Query("uz", regex="^(uz|en|ru)$")):
    conn = get_connection()
    rows = conn.execute(
        """SELECT i.slug, i.name_uz, i.name_en, i.name_ru, i.unit_uz,
                  MIN(d.year) as first_year, MAX(d.year) as last_year,
                  MIN(d.value) as min_val, MAX(d.value) as max_val,
                  (SELECT value FROM data_points WHERE indicator_id=i.id ORDER BY year LIMIT 1) as first_val,
                  (SELECT value FROM data_points WHERE indicator_id=i.id ORDER BY year DESC LIMIT 1) as last_val
           FROM indicators i JOIN data_points d ON d.indicator_id=i.id
           GROUP BY i.id ORDER BY i.id"""
    ).fetchall()
    conn.close()

    result = []
    for r in rows:
        n = r["last_year"] - r["first_year"]
        cagr = None
        if n > 0 and r["first_val"] and r["first_val"] > 0:
            cagr = round((pow(r["last_val"] / r["first_val"], 1 / n) - 1) * 100, 2)
        total_growth = None
        if r["first_val"] and r["first_val"] != 0:
            total_growth = round((r["last_val"] - r["first_val"]) / r["first_val"] * 100, 2)

        result.append({
            "slug": r["slug"],
            "name": r[f"name_{lang}"] or r["name_uz"],
            "first_year": r["first_year"],
            "last_year": r["last_year"],
            "first_value": r["first_val"],
            "last_value": r["last_val"],
            "cagr_pct": cagr,
            "total_growth_pct": total_growth,
        })
    return result


@app.get("/api/scrape/status")
async def scrape_status():
    """Return last scrape log entry."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        return {"status": "never_run"}
    return dict(row)


@app.post("/api/scrape/trigger")
async def trigger_scrape():
    from app.scraper import run_scraper
    try:
        count = run_scraper()
        return {"status": "success", "records_updated": count}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/years")
async def available_years():
    conn = get_connection()
    row = conn.execute("SELECT MIN(year) as mn, MAX(year) as mx FROM data_points").fetchone()
    conn.close()
    return {"min_year": row["mn"], "max_year": row["mx"]}



def _compute_stats(values: list[float]) -> dict:
    if not values:
        return {}
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return {
        "count": n,
        "min": min(values),
        "max": max(values),
        "mean": round(mean, 4),
        "std": round(math.sqrt(variance), 4),
    }

import sqlite3
import os
import time
import fcntl
from datetime import datetime

DB_PATH   = os.environ.get("DB_PATH", "digital_economy.db")
_LOCK_PATH = DB_PATH + ".initlock"
_BUSY_TIMEOUT_MS = 30_000   # 30 s


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():

    lock_fd = open(_LOCK_PATH, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        conn = get_connection()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS categories (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name_uz     TEXT NOT NULL,
                name_en     TEXT,
                name_ru     TEXT,
                slug        TEXT UNIQUE NOT NULL,
                icon        TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS indicators (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id  INTEGER REFERENCES categories(id),
                name_uz      TEXT NOT NULL,
                name_en      TEXT,
                name_ru      TEXT,
                unit_uz      TEXT,
                unit_en      TEXT,
                slug         TEXT UNIQUE NOT NULL,
                description  TEXT,
                source_url   TEXT,
                created_at   TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS data_points (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator_id INTEGER REFERENCES indicators(id),
                year         INTEGER NOT NULL,
                quarter      INTEGER,
                value        REAL NOT NULL,
                raw_value    TEXT,
                notes        TEXT,
                scraped_at   TEXT DEFAULT (datetime('now')),
                UNIQUE(indicator_id, year, quarter)
            );

            CREATE TABLE IF NOT EXISTS scrape_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at  TEXT,
                finished_at TEXT,
                status      TEXT,
                records     INTEGER DEFAULT 0,
                error       TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_dp_indicator ON data_points(indicator_id);
            CREATE INDEX IF NOT EXISTS idx_dp_year      ON data_points(year);
        """)
        conn.commit()
        conn.close()
        print(f"[DB] Initialized: {DB_PATH}")
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


def insert_category(name_uz, name_en, name_ru, slug, icon="📊"):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO categories (name_uz,name_en,name_ru,slug,icon) VALUES (?,?,?,?,?)",
            (name_uz, name_en, name_ru, slug, icon)
        )
        conn.commit()
        row = conn.execute("SELECT id FROM categories WHERE slug=?", (slug,)).fetchone()
        return row["id"]
    finally:
        conn.close()


def insert_indicator(category_id, name_uz, name_en, name_ru, unit_uz, unit_en, slug, description="", source_url=""):
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO indicators
               (category_id,name_uz,name_en,name_ru,unit_uz,unit_en,slug,description,source_url)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (category_id, name_uz, name_en, name_ru, unit_uz, unit_en, slug, description, source_url)
        )
        conn.commit()
        row = conn.execute("SELECT id FROM indicators WHERE slug=?", (slug,)).fetchone()
        return row["id"]
    finally:
        conn.close()


def upsert_data_point(indicator_id, year, value, quarter=None, raw_value=None, notes=None):
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO data_points (indicator_id,year,quarter,value,raw_value,notes)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(indicator_id,year,quarter) DO UPDATE
               SET value=excluded.value, raw_value=excluded.raw_value, scraped_at=datetime('now')""",
            (indicator_id, year, quarter, value, raw_value, notes)
        )
        conn.commit()
    finally:
        conn.close()


def log_scrape(started_at, finished_at, status, records=0, error=None):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO scrape_log (started_at,finished_at,status,records,error) VALUES (?,?,?,?,?)",
            (started_at, finished_at, status, records, error)
        )
        conn.commit()
    finally:
        conn.close()

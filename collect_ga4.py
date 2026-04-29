#!/usr/bin/env python3
"""
collect_ga4.py — Aspire tout Google Analytics 4 dans Railway Postgres.

Propriétés collectées :
  BDouin Shop       (382670810)
  Awlad Quiz GO     (477845965)
  Awlad School Mobile (349962048)

Tables créées :
  ga4_sessions        — sessions/utilisateurs par jour, pays, source/medium, device
  ga4_pages           — pages vues par page, par jour
  ga4_events          — événements (purchase, search, scroll, etc.) par jour
  ga4_ecommerce       — transactions shop : item_name, revenus, quantités
  ga4_search_terms    — recherches internes dans le shop
  ga4_user_acquisition — canaux d'acquisition nouveaux utilisateurs

Usage:
    python3 collect_ga4.py                    # tout depuis le début
    python3 collect_ga4.py --days 90          # 90 derniers jours
    python3 collect_ga4.py --property bdouin  # une propriété
"""

import argparse
import time
import json
import psycopg2
from psycopg2.extras import execute_values
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
from google.oauth2 import service_account

CREDS_FILE = "/tmp/ga4_creds.json"
DB_URL     = "postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway"

PROPERTIES = {
    "bdouin":  ("BDouin Shop",          "382670810"),
    "quiz":    ("Awlad Quiz GO",         "477845965"),
    "school":  ("Awlad School Mobile",  "349962048"),
}

# Collect from launch date of each property
START_DATES = {
    "382670810":  "2021-01-01",
    "477845965":  "2022-01-01",
    "349962048":  "2022-01-01",
}

DDL = """
CREATE TABLE IF NOT EXISTS ga4_sessions (
    property_id   TEXT,
    date          DATE,
    country       TEXT,
    source        TEXT,
    medium        TEXT,
    device        TEXT,
    sessions      INTEGER,
    users         INTEGER,
    new_users     INTEGER,
    engaged_sessions INTEGER,
    bounce_rate   NUMERIC(6,4),
    avg_session_duration NUMERIC(10,2),
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (property_id, date, country, source, medium, device)
);

CREATE TABLE IF NOT EXISTS ga4_pages (
    property_id   TEXT,
    date          DATE,
    page_path     TEXT,
    page_title    TEXT,
    views         INTEGER,
    users         INTEGER,
    avg_time_on_page NUMERIC(10,2),
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (property_id, date, page_path)
);

CREATE TABLE IF NOT EXISTS ga4_events (
    property_id   TEXT,
    date          DATE,
    event_name    TEXT,
    event_count   INTEGER,
    users         INTEGER,
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (property_id, date, event_name)
);

CREATE TABLE IF NOT EXISTS ga4_ecommerce (
    property_id     TEXT,
    date            DATE,
    item_id         TEXT,
    item_name       TEXT,
    item_category   TEXT,
    item_revenue    NUMERIC(12,2),
    items_purchased INTEGER,
    transactions    INTEGER,
    collected_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (property_id, date, item_id)
);

CREATE TABLE IF NOT EXISTS ga4_search_terms (
    property_id   TEXT,
    date          DATE,
    search_term   TEXT,
    event_count   INTEGER,
    users         INTEGER,
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (property_id, date, search_term)
);

CREATE TABLE IF NOT EXISTS ga4_user_acquisition (
    property_id         TEXT,
    date                DATE,
    first_user_source   TEXT,
    first_user_medium   TEXT,
    first_user_campaign TEXT,
    new_users           INTEGER,
    sessions            INTEGER,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (property_id, date, first_user_source, first_user_medium, first_user_campaign)
);

CREATE INDEX IF NOT EXISTS idx_ga4_sessions_prop_date   ON ga4_sessions(property_id, date);
CREATE INDEX IF NOT EXISTS idx_ga4_pages_prop_date      ON ga4_pages(property_id, date);
CREATE INDEX IF NOT EXISTS idx_ga4_pages_path           ON ga4_pages(page_path);
CREATE INDEX IF NOT EXISTS idx_ga4_events_prop_date     ON ga4_events(property_id, date);
CREATE INDEX IF NOT EXISTS idx_ga4_ecom_prop_date       ON ga4_ecommerce(property_id, date);
CREATE INDEX IF NOT EXISTS idx_ga4_search_prop_date     ON ga4_search_terms(property_id, date);
CREATE INDEX IF NOT EXISTS idx_ga4_acq_prop_date        ON ga4_user_acquisition(property_id, date);
"""


def get_client():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=creds)


def run_report(client, property_id, dimensions, metrics, start_date, end_date, limit=100000):
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit,
    )
    return client.run_report(req)


def val(row, i, cast=str, default=None):
    try:
        v = row.dimension_values[i].value if i < len(row.dimension_values) else None
        if v in (None, "(not set)", ""):
            return default
        return v
    except Exception:
        return default


def mval(row, i, cast=float, default=0):
    try:
        v = row.metric_values[i].value
        return cast(v) if v else default
    except Exception:
        return default


# ─── Collectors ───────────────────────────────────────────────────────────────

def dedup(rows, key_indices):
    """Remove duplicate rows by key (tuple of column indices)."""
    seen = {}
    for row in rows:
        k = tuple(row[i] for i in key_indices)
        seen[k] = row
    return list(seen.values())


def collect_sessions(client, cur, prop_id, prop_name, start_date, end_date):
    print(f"  [sessions] {prop_name} {start_date}→{end_date}")
    try:
        resp = run_report(client, prop_id,
            dimensions=["date", "country", "sessionSource", "sessionMedium", "deviceCategory"],
            metrics=["sessions", "totalUsers", "newUsers", "engagedSessions",
                     "bounceRate", "averageSessionDuration"],
            start_date=start_date, end_date=end_date,
        )
    except Exception as e:
        print(f"    ✗ {e}")
        return 0

    rows = []
    for row in resp.rows:
        rows.append((
            prop_id,
            val(row, 0),   # date
            val(row, 1, default=""),  # country
            val(row, 2, default=""),  # source
            val(row, 3, default=""),  # medium
            val(row, 4, default=""),  # device
            mval(row, 0, int),  # sessions
            mval(row, 1, int),  # users
            mval(row, 2, int),  # new_users
            mval(row, 3, int),  # engaged_sessions
            mval(row, 4, float),  # bounce_rate
            mval(row, 5, float),  # avg_session_duration
        ))

    rows = dedup(rows, [0, 1, 2, 3, 4, 5])  # prop_id,date,country,source,medium,device
    if rows:
        execute_values(cur, """
            INSERT INTO ga4_sessions
              (property_id,date,country,source,medium,device,sessions,users,
               new_users,engaged_sessions,bounce_rate,avg_session_duration)
            VALUES %s ON CONFLICT (property_id,date,country,source,medium,device)
            DO UPDATE SET sessions=EXCLUDED.sessions, users=EXCLUDED.users,
              new_users=EXCLUDED.new_users, engaged_sessions=EXCLUDED.engaged_sessions,
              bounce_rate=EXCLUDED.bounce_rate, avg_session_duration=EXCLUDED.avg_session_duration
        """, rows)
    print(f"    {len(rows)} rows")
    return len(rows)


def collect_pages(client, cur, prop_id, prop_name, start_date, end_date):
    print(f"  [pages] {prop_name}")
    try:
        resp = run_report(client, prop_id,
            dimensions=["date", "pagePath", "pageTitle"],
            metrics=["screenPageViews", "totalUsers", "averageSessionDuration"],
            start_date=start_date, end_date=end_date,
        )
    except Exception as e:
        print(f"    ✗ {e}")
        return 0

    rows = []
    for row in resp.rows:
        path = val(row, 1, default="")
        if len(path) > 500:
            path = path[:500]
        rows.append((
            prop_id,
            val(row, 0),
            path,
            val(row, 2, default="")[:500],
            mval(row, 0, int),
            mval(row, 1, int),
            mval(row, 2, float),
        ))

    rows = dedup(rows, [0, 1, 2])  # prop_id, date, page_path
    if rows:
        execute_values(cur, """
            INSERT INTO ga4_pages
              (property_id,date,page_path,page_title,views,users,avg_time_on_page)
            VALUES %s ON CONFLICT (property_id,date,page_path)
            DO UPDATE SET views=EXCLUDED.views, users=EXCLUDED.users,
              avg_time_on_page=EXCLUDED.avg_time_on_page
        """, rows)
    print(f"    {len(rows)} rows")
    return len(rows)


def collect_events(client, cur, prop_id, prop_name, start_date, end_date):
    print(f"  [events] {prop_name}")
    try:
        resp = run_report(client, prop_id,
            dimensions=["date", "eventName"],
            metrics=["eventCount", "totalUsers"],
            start_date=start_date, end_date=end_date,
        )
    except Exception as e:
        print(f"    ✗ {e}")
        return 0

    rows = [(
        prop_id,
        val(row, 0),
        val(row, 1, default=""),
        mval(row, 0, int),
        mval(row, 1, int),
    ) for row in resp.rows]

    rows = dedup(rows, [0, 1, 2])
    if rows:
        execute_values(cur, """
            INSERT INTO ga4_events (property_id,date,event_name,event_count,users)
            VALUES %s ON CONFLICT (property_id,date,event_name)
            DO UPDATE SET event_count=EXCLUDED.event_count, users=EXCLUDED.users
        """, rows)
    print(f"    {len(rows)} rows")
    return len(rows)


def collect_ecommerce(client, cur, prop_id, prop_name, start_date, end_date):
    # Only relevant for BDouin Shop
    if prop_id != "382670810":
        return 0
    print(f"  [ecommerce] {prop_name}")
    try:
        resp = run_report(client, prop_id,
            dimensions=["date", "itemId", "itemName", "itemCategory"],
            metrics=["itemRevenue", "itemsPurchased", "transactions"],
            start_date=start_date, end_date=end_date,
        )
    except Exception as e:
        print(f"    ✗ {e}")
        return 0

    rows = [(
        prop_id,
        val(row, 0),
        val(row, 1, default="")[:200],
        val(row, 2, default="")[:500],
        val(row, 3, default="")[:200],
        mval(row, 0, float),
        mval(row, 1, int),
        mval(row, 2, int),
    ) for row in resp.rows]

    rows = dedup(rows, [0, 1, 2])
    if rows:
        execute_values(cur, """
            INSERT INTO ga4_ecommerce
              (property_id,date,item_id,item_name,item_category,item_revenue,
               items_purchased,transactions)
            VALUES %s ON CONFLICT (property_id,date,item_id)
            DO UPDATE SET item_revenue=EXCLUDED.item_revenue,
              items_purchased=EXCLUDED.items_purchased, transactions=EXCLUDED.transactions
        """, rows)
    print(f"    {len(rows)} rows")
    return len(rows)


def collect_search_terms(client, cur, prop_id, prop_name, start_date, end_date):
    # Only for BDouin Shop
    if prop_id != "382670810":
        return 0
    print(f"  [search_terms] {prop_name}")
    try:
        resp = run_report(client, prop_id,
            dimensions=["date", "searchTerm"],
            metrics=["eventCount", "totalUsers"],
            start_date=start_date, end_date=end_date,
        )
    except Exception as e:
        print(f"    ✗ {e}")
        return 0

    rows = [(
        prop_id,
        val(row, 0),
        val(row, 1, default="")[:500],
        mval(row, 0, int),
        mval(row, 1, int),
    ) for row in resp.rows if val(row, 1)]

    rows = dedup(rows, [0, 1, 2])
    if rows:
        execute_values(cur, """
            INSERT INTO ga4_search_terms (property_id,date,search_term,event_count,users)
            VALUES %s ON CONFLICT (property_id,date,search_term)
            DO UPDATE SET event_count=EXCLUDED.event_count, users=EXCLUDED.users
        """, rows)
    print(f"    {len(rows)} rows")
    return len(rows)


def collect_user_acquisition(client, cur, prop_id, prop_name, start_date, end_date):
    print(f"  [user_acquisition] {prop_name}")
    try:
        resp = run_report(client, prop_id,
            dimensions=["date", "firstUserSource", "firstUserMedium", "firstUserCampaignName"],
            metrics=["newUsers", "sessions"],
            start_date=start_date, end_date=end_date,
        )
    except Exception as e:
        print(f"    ✗ {e}")
        return 0

    rows = [(
        prop_id,
        val(row, 0),
        val(row, 1, default="")[:200],
        val(row, 2, default="")[:200],
        val(row, 3, default="")[:200],
        mval(row, 0, int),
        mval(row, 1, int),
    ) for row in resp.rows]

    rows = dedup(rows, [0, 1, 2, 3, 4])
    if rows:
        execute_values(cur, """
            INSERT INTO ga4_user_acquisition
              (property_id,date,first_user_source,first_user_medium,
               first_user_campaign,new_users,sessions)
            VALUES %s ON CONFLICT
              (property_id,date,first_user_source,first_user_medium,first_user_campaign)
            DO UPDATE SET new_users=EXCLUDED.new_users, sessions=EXCLUDED.sessions
        """, rows)
    print(f"    {len(rows)} rows")
    return len(rows)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(days=None, property_filter=None):
    from datetime import datetime, timedelta

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur  = conn.cursor()

    print("Setting up tables...")
    cur.execute(DDL)
    conn.commit()
    print("✓ Tables ready")

    client = get_client()
    end_date = datetime.today().strftime("%Y-%m-%d")

    targets = {}
    for key, (name, pid) in PROPERTIES.items():
        if property_filter and key != property_filter:
            continue
        start = START_DATES.get(pid, "2021-01-01")
        if days:
            start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        targets[pid] = (name, start)

    total = 0
    for pid, (name, start_date) in targets.items():
        print(f"\n=== {name} ({pid}) — {start_date} → {end_date} ===")

        n  = collect_sessions(client, cur, pid, name, start_date, end_date)
        n += collect_pages(client, cur, pid, name, start_date, end_date)
        n += collect_events(client, cur, pid, name, start_date, end_date)
        n += collect_ecommerce(client, cur, pid, name, start_date, end_date)
        n += collect_search_terms(client, cur, pid, name, start_date, end_date)
        n += collect_user_acquisition(client, cur, pid, name, start_date, end_date)

        conn.commit()
        total += n
        print(f"  ✓ {name}: {n} rows committed")
        time.sleep(1)

    print("\n=== FINAL COUNTS ===")
    for t in ["ga4_sessions", "ga4_pages", "ga4_events", "ga4_ecommerce",
              "ga4_search_terms", "ga4_user_acquisition"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t}: {cur.fetchone()[0]}")

    conn.close()
    print(f"\nDone! Total rows: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days",     type=int, help="Collect last N days only")
    parser.add_argument("--property", help="bdouin | quiz | school")
    args = parser.parse_args()
    main(days=args.days, property_filter=args.property)

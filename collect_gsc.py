#!/usr/bin/env python3
"""
collect_gsc.py — Aspire Google Search Console pour bdouin.com.

Tables :
  gsc_queries    — requêtes SEO (clicks, impressions, ctr, position) par jour
  gsc_pages      — performance par page
  gsc_countries  — performance par pays
  gsc_devices    — performance par device
"""

import time
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from google.oauth2 import service_account
from googleapiclient.discovery import build

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
CREDS  = '/tmp/ga4_creds.json'
SITE   = 'sc-domain:bdouin.com'

DDL = """
CREATE TABLE IF NOT EXISTS gsc_queries (
    date          DATE,
    query         TEXT,
    clicks        INTEGER,
    impressions   INTEGER,
    ctr           NUMERIC(8,6),
    position      NUMERIC(8,2),
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, query)
);
CREATE TABLE IF NOT EXISTS gsc_pages (
    date          DATE,
    page          TEXT,
    clicks        INTEGER,
    impressions   INTEGER,
    ctr           NUMERIC(8,6),
    position      NUMERIC(8,2),
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, page)
);
CREATE TABLE IF NOT EXISTS gsc_countries (
    date          DATE,
    country       TEXT,
    clicks        INTEGER,
    impressions   INTEGER,
    ctr           NUMERIC(8,6),
    position      NUMERIC(8,2),
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, country)
);
CREATE TABLE IF NOT EXISTS gsc_devices (
    date          DATE,
    device        TEXT,
    clicks        INTEGER,
    impressions   INTEGER,
    ctr           NUMERIC(8,6),
    position      NUMERIC(8,2),
    collected_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, device)
);
CREATE INDEX IF NOT EXISTS idx_gsc_queries_date ON gsc_queries(date);
CREATE INDEX IF NOT EXISTS idx_gsc_queries_q    ON gsc_queries(query);
CREATE INDEX IF NOT EXISTS idx_gsc_pages_date   ON gsc_pages(date);
"""


def get_svc():
    creds = service_account.Credentials.from_service_account_file(
        CREDS, scopes=['https://www.googleapis.com/auth/webmasters.readonly'])
    return build('searchconsole', 'v1', credentials=creds, cache_discovery=False)


def fetch_dimension(svc, dimension, start_date, end_date, row_limit=25000):
    """Fetch all rows for a dimension across a date range."""
    all_rows = []
    start_row = 0
    while True:
        body = {
            'startDate': start_date,
            'endDate':   end_date,
            'dimensions': ['date', dimension],
            'rowLimit':  row_limit,
            'startRow':  start_row,
        }
        try:
            resp = svc.searchanalytics().query(siteUrl=SITE, body=body).execute()
        except Exception as e:
            print(f'  ✗ {dimension}: {e}')
            break
        rows = resp.get('rows', [])
        if not rows:
            break
        all_rows.extend(rows)
        start_row += len(rows)
        if len(rows) < row_limit:
            break
        time.sleep(0.5)
    return all_rows


def insert_dimension(cur, table, dimension, rows):
    if not rows:
        return 0
    seen = {}
    for r in rows:
        d   = r['keys'][0]
        dim = r['keys'][1][:500]
        seen[(d, dim)] = (
            d, dim,
            int(r.get('clicks') or 0),
            int(r.get('impressions') or 0),
            float(r.get('ctr') or 0),
            float(r.get('position') or 0),
        )
    sql = f"""
        INSERT INTO {table} (date, {dimension}, clicks, impressions, ctr, position)
        VALUES %s ON CONFLICT (date, {dimension}) DO UPDATE SET
          clicks=EXCLUDED.clicks, impressions=EXCLUDED.impressions,
          ctr=EXCLUDED.ctr, position=EXCLUDED.position
    """
    execute_values(cur, sql, list(seen.values()))
    return len(seen)


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur  = conn.cursor()
    cur.execute(DDL); conn.commit()
    print('✓ Tables ready')

    svc = get_svc()
    end   = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')
    start = '2024-01-01'  # GSC has 16-month history

    print(f'\nFetching GSC for {SITE} from {start} to {end}...\n')

    for dimension, table in [
        ('query',     'gsc_queries'),
        ('page',      'gsc_pages'),
        ('country',   'gsc_countries'),
        ('device',    'gsc_devices'),
    ]:
        print(f'[{dimension}] ...')
        rows = fetch_dimension(svc, dimension, start, end)
        n = insert_dimension(cur, table, dimension, rows)
        conn.commit()
        print(f'  ✓ {n} rows in {table}')

    print('\n=== FINAL COUNTS ===')
    for t in ['gsc_queries', 'gsc_pages', 'gsc_countries', 'gsc_devices']:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'  {t}: {cur.fetchone()[0]}')
    conn.close()


if __name__ == '__main__':
    main()

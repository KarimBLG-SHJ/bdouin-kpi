#!/usr/bin/env python3
"""
collect_asc.py — Aspire tout App Store Connect dans Railway Postgres.

Apps :
  Awlad School (1612014910), Awlad Quiz GO (6737732771),
  Awlad Classroom (6754524897), Awlad Salat (1669010427),
  Awlad Coran (6477914472), BDouin Maker (946822771),
  BDouin Magazine (6472446290), BDouin Stories (6479290376)

Tables :
  asc_apps              — catalogue apps
  asc_downloads         — téléchargements par jour/pays/device
  asc_usage             — sessions, crashes, active devices par jour
  asc_ratings           — notes moyennes par version/pays
  asc_revenue           — revenus in-app / abonnements par jour
  asc_crashes           — crash rate par version

Usage:
    python3 collect_asc.py
    python3 collect_asc.py --resource downloads
"""

import argparse
import time
import jwt
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

KEY_ID     = 'QSFVGW7CN2'
ISSUER_ID  = '69a6de83-a19a-47e3-e053-5b8c7c11a4d1'
KEY_FILE   = '/Users/karim/Downloads/AuthKey_QSFVGW7CN2.p8'
DB_URL     = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
ASC_BASE   = 'https://api.appstoreconnect.apple.com/v1'
SLEEP      = 0.5

APPS = {
    '1612014910': 'Awlad School',
    '6737732771': 'Awlad Quiz GO',
    '6754524897': 'Awlad Classroom',
    '1669010427': 'Awlad Salat',
    '6477914472': 'Awlad Coran',
    '946822771':  'BDouin Maker',
    '6472446290': 'BDouin Magazine',
    '6479290376': 'BDouin Stories',
}

DDL = """
CREATE TABLE IF NOT EXISTS asc_apps (
    id              TEXT PRIMARY KEY,
    name            TEXT,
    bundle_id       TEXT,
    sku             TEXT,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS asc_downloads (
    app_id          TEXT,
    date            DATE,
    country         TEXT,
    device          TEXT,
    units           INTEGER,
    updates         INTEGER,
    collected_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (app_id, date, country, device)
);

CREATE TABLE IF NOT EXISTS asc_usage (
    app_id              TEXT,
    date                DATE,
    active_devices      INTEGER,
    sessions            INTEGER,
    crashes             INTEGER,
    page_views          INTEGER,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (app_id, date)
);

CREATE TABLE IF NOT EXISTS asc_ratings (
    app_id          TEXT,
    date            DATE,
    country         TEXT,
    avg_rating      NUMERIC(4,2),
    total_ratings   INTEGER,
    collected_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (app_id, date, country)
);

CREATE TABLE IF NOT EXISTS asc_revenue (
    app_id          TEXT,
    date            DATE,
    country         TEXT,
    product_type    TEXT,
    units           INTEGER,
    developer_proceeds NUMERIC(12,4),
    customer_price  NUMERIC(12,4),
    collected_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (app_id, date, country, product_type)
);

CREATE INDEX IF NOT EXISTS idx_asc_dl_app_date   ON asc_downloads(app_id, date);
CREATE INDEX IF NOT EXISTS idx_asc_dl_country    ON asc_downloads(country);
CREATE INDEX IF NOT EXISTS idx_asc_usage_app     ON asc_usage(app_id, date);
CREATE INDEX IF NOT EXISTS idx_asc_rev_app       ON asc_revenue(app_id, date);
"""


def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


def make_token():
    with open(KEY_FILE) as f:
        private_key = f.read()
    payload = {
        'iss': ISSUER_ID,
        'iat': int(time.time()),
        'exp': int(time.time()) + 1200,
        'aud': 'appstoreconnect-v1',
    }
    return jwt.encode(payload, private_key, algorithm='ES256', headers={'kid': KEY_ID})


def get_headers():
    return {'Authorization': f'Bearer {make_token()}'}


def asc_get(path, params=None):
    r = requests.get(f'{ASC_BASE}/{path}', headers=get_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# Sales reports use a different base URL
REPORTER_BASE = 'https://api.appstoreconnect.apple.com/v1/salesReports'


def fetch_sales_report(vendor_id, report_type, report_subtype, date_str, frequency='DAILY'):
    """Fetch a sales/download report for a specific date."""
    params = {
        'filter[reportType]':    report_type,
        'filter[reportSubType]': report_subtype,
        'filter[frequency]':     frequency,
        'filter[reportDate]':    date_str,
        'filter[vendorNumber]':  vendor_id,
    }
    r = requests.get(REPORTER_BASE, headers=get_headers(), params=params, timeout=30)
    if r.status_code == 200 and r.content:
        import gzip, io
        try:
            content = gzip.decompress(r.content).decode('utf-8')
        except Exception:
            content = r.content.decode('utf-8')
        return content
    return None


def get_vendor_id():
    """Get vendor number from ASC."""
    data = asc_get('salesReports', {
        'filter[reportType]': 'SALES',
        'filter[reportSubType]': 'SUMMARY',
        'filter[frequency]': 'DAILY',
        'filter[reportDate]': '2026-04-01',
        'filter[vendorNumber]': '',
    })
    # Try financialReports to get vendor number
    r = requests.get(f'{ASC_BASE}/apps', headers=get_headers())
    # Vendor number is typically the same as the provider ID
    return None


def collect_apps(cur):
    print('\n[apps] Collecting app catalog...')
    data = asc_get('apps', {'limit': 50})
    rows = []
    for app in data.get('data', []):
        a = app['attributes']
        rows.append((app['id'], a.get('name'), a.get('bundleId'), a.get('sku')))
    if rows:
        execute_values(cur, """
            INSERT INTO asc_apps (id, name, bundle_id, sku)
            VALUES %s ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name
        """, rows)
    print(f'  {len(rows)} apps')
    return len(rows)


def collect_downloads(cur):
    """Collect download data via Analytics API."""
    print('\n[downloads] Fetching download stats...')

    # ASC Analytics API for downloads
    end_date = datetime.today()
    start_date = datetime(2021, 1, 1)
    total = 0

    for app_id, app_name in APPS.items():
        print(f'  {app_name}...')

        # Check last collected date
        cur.execute('SELECT MAX(date) FROM asc_downloads WHERE app_id=%s', (app_id,))
        last = cur.fetchone()[0]
        since = last + timedelta(days=1) if last else start_date

        if since.date() >= end_date.date():
            print(f'    Up to date')
            continue

        # Use Analytics API - installs by day
        try:
            # Chunk by 90-day windows (API limit)
            current = since
            while current < end_date:
                chunk_end = min(current + timedelta(days=89), end_date)
                start_str = current.strftime('%Y-%m-%d')
                end_str = chunk_end.strftime('%Y-%m-%d')

                payload = {
                    'adamId': app_id,
                    'measures': ['installs', 'sessions', 'pageViews', 'activeDevices', 'crashes'],
                    'dimension': 'territory',
                    'startTime': f'{start_str}T00:00:00Z',
                    'endTime': f'{end_str}T00:00:00Z',
                    'granularity': 'DAY',
                    'frequency': 'DAY',
                }
                r = requests.post(
                    'https://api.appstoreconnect.apple.com/v1/analytics/apps/time-series',
                    headers={**get_headers(), 'Content-Type': 'application/json'},
                    json=payload,
                    timeout=30,
                )

                if r.ok:
                    data = r.json()
                    rows = []
                    for item in data.get('data', []):
                        date = item.get('date', '')[:10]
                        for territory, metrics in item.get('measures', {}).items():
                            rows.append((
                                app_id, date, territory, 'all',
                                int(metrics.get('installs', 0)),
                                0,
                            ))
                    if rows:
                        # dedup
                        seen = {}
                        for row in rows:
                            seen[(row[0], row[1], row[2], row[3])] = row
                        rows = list(seen.values())
                        execute_values(cur, """
                            INSERT INTO asc_downloads (app_id,date,country,device,units,updates)
                            VALUES %s ON CONFLICT (app_id,date,country,device)
                            DO UPDATE SET units=EXCLUDED.units
                        """, rows)
                        total += len(rows)
                else:
                    # Fallback: use Sales Report API
                    _collect_downloads_sales_report(cur, app_id, current, chunk_end)

                current = chunk_end + timedelta(days=1)
                time.sleep(SLEEP)

        except Exception as e:
            print(f'    ✗ {e}')
            # Try sales reports fallback
            try:
                _collect_downloads_sales_report(cur, app_id, since, end_date)
            except Exception as e2:
                print(f'    ✗ fallback: {e2}')

        cur.connection.commit()

    print(f'  ✓ {total} rows total')
    return total


def _collect_downloads_sales_report(cur, app_id, start, end):
    """Fallback: parse daily sales TSV reports."""
    import csv, io
    inserted = 0
    current = start
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        try:
            # SALES SUMMARY report
            params = {
                'filter[reportType]':    'SALES',
                'filter[reportSubType]': 'SUMMARY',
                'filter[frequency]':     'DAILY',
                'filter[reportDate]':    date_str,
            }
            r = requests.get(REPORTER_BASE, headers=get_headers(), params=params, timeout=30)
            if r.ok and r.content:
                import gzip
                try:
                    content = gzip.decompress(r.content).decode('utf-8')
                except Exception:
                    content = r.content.decode('utf-8')

                reader = csv.DictReader(io.StringIO(content), delimiter='\t')
                rows = []
                for row in reader:
                    if row.get('Apple Identifier') != app_id:
                        continue
                    country = row.get('Country Code', '')
                    device  = row.get('Device', 'all')
                    units   = int(float(row.get('Units', 0) or 0))
                    updates = 0
                    if row.get('Product Type Identifier') in ('1', '1F', '1T'):
                        rows.append((app_id, date_str, country, device, units, updates))

                if rows:
                    seen = {}
                    for row in rows:
                        k = (row[0], row[1], row[2], row[3])
                        seen[k] = (seen[k][0], seen[k][1], seen[k][2], seen[k][3],
                                   seen[k][4]+row[4], 0) if k in seen else row
                    execute_values(cur, """
                        INSERT INTO asc_downloads (app_id,date,country,device,units,updates)
                        VALUES %s ON CONFLICT (app_id,date,country,device)
                        DO UPDATE SET units=EXCLUDED.units
                    """, list(seen.values()))
                    inserted += len(seen)
        except Exception:
            pass
        current += timedelta(days=1)
        time.sleep(0.2)
    return inserted


def collect_usage(cur):
    """Collect sessions, active devices, crashes per day."""
    print('\n[usage] Fetching usage stats...')
    end_date = datetime.today()
    total = 0

    for app_id, app_name in APPS.items():
        print(f'  {app_name}...')
        cur.execute('SELECT MAX(date) FROM asc_usage WHERE app_id=%s', (app_id,))
        last = cur.fetchone()[0]
        since = last + timedelta(days=1) if last else datetime(2021, 1, 1)
        if since.date() >= end_date.date():
            print(f'    Up to date')
            continue

        current = since
        while current < end_date:
            chunk_end = min(current + timedelta(days=89), end_date)
            try:
                payload = {
                    'adamId': app_id,
                    'measures': ['sessions', 'activeDevices', 'crashes', 'pageViews'],
                    'startTime': f'{current.strftime("%Y-%m-%d")}T00:00:00Z',
                    'endTime':   f'{chunk_end.strftime("%Y-%m-%d")}T00:00:00Z',
                    'granularity': 'DAY',
                    'frequency': 'DAY',
                }
                r = requests.post(
                    'https://api.appstoreconnect.apple.com/v1/analytics/apps/time-series',
                    headers={**get_headers(), 'Content-Type': 'application/json'},
                    json=payload, timeout=30,
                )
                if r.ok:
                    for item in r.json().get('data', []):
                        date = item.get('date', '')[:10]
                        m = item.get('measures', {})
                        cur.execute("""
                            INSERT INTO asc_usage (app_id,date,active_devices,sessions,crashes,page_views)
                            VALUES (%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (app_id,date) DO UPDATE SET
                              active_devices=EXCLUDED.active_devices, sessions=EXCLUDED.sessions,
                              crashes=EXCLUDED.crashes, page_views=EXCLUDED.page_views
                        """, (app_id, date,
                              int(m.get('activeDevices', 0) or 0),
                              int(m.get('sessions', 0) or 0),
                              int(m.get('crashes', 0) or 0),
                              int(m.get('pageViews', 0) or 0)))
                        total += 1
            except Exception as e:
                print(f'    ✗ {e}')
            current = chunk_end + timedelta(days=1)
            time.sleep(SLEEP)

        cur.connection.commit()

    print(f'  ✓ {total} rows')
    return total


def collect_ratings(cur):
    """Collect ratings per app."""
    print('\n[ratings] Fetching ratings...')
    total = 0
    for app_id, app_name in APPS.items():
        try:
            data = asc_get(f'apps/{app_id}/customerReviews', {
                'filter[territory]': '',
                'limit': 1,
                'sort': '-createdDate',
            })
            # Get rating distribution
            r2 = requests.get(
                f'{ASC_BASE}/apps/{app_id}/ratingSummaries',
                headers=get_headers(),
            )
            if r2.ok:
                for item in r2.json().get('data', []):
                    a = item['attributes']
                    cur.execute("""
                        INSERT INTO asc_ratings (app_id,date,country,avg_rating,total_ratings)
                        VALUES (%s,NOW()::date,%s,%s,%s)
                        ON CONFLICT (app_id,date,country) DO UPDATE SET
                          avg_rating=EXCLUDED.avg_rating, total_ratings=EXCLUDED.total_ratings
                    """, (app_id, a.get('displayedTerritory','all'),
                          a.get('ratingAverage', 0), a.get('ratingCount', 0)))
                    total += 1
        except Exception as e:
            print(f'  ✗ {app_name}: {e}')
        time.sleep(SLEEP)

    cur.connection.commit()
    print(f'  ✓ {total} rows')
    return total


def collect_revenue(cur):
    """Collect revenue from financial reports."""
    print('\n[revenue] Fetching financial reports...')
    import csv, io, gzip
    total = 0

    # Monthly financial reports going back 2 years
    end = datetime.today()
    current = datetime(2022, 1, 1)

    while current <= end:
        date_str = current.strftime('%Y-%m')
        try:
            params = {
                'filter[reportType]':    'FINANCIAL',
                'filter[reportSubType]': 'SUMMARY',
                'filter[frequency]':     'MONTHLY',
                'filter[reportDate]':    date_str,
                'filter[regionCode]':    'ZZ',
            }
            r = requests.get(REPORTER_BASE, headers=get_headers(), params=params, timeout=30)
            if r.ok and r.content:
                try:
                    content = gzip.decompress(r.content).decode('utf-8')
                except Exception:
                    content = r.content.decode('utf-8')

                reader = csv.DictReader(io.StringIO(content), delimiter='\t')
                rows = []
                for row in reader:
                    app_id = None
                    for aid in APPS:
                        if aid in str(row.get('Apple Identifier', '')):
                            app_id = aid
                            break
                    if not app_id:
                        continue
                    rows.append((
                        app_id,
                        date_str + '-01',
                        row.get('Country Of Sale', ''),
                        row.get('Product Type Identifier', ''),
                        int(float(row.get('Units', 0) or 0)),
                        float(row.get('Developer Proceeds', 0) or 0),
                        float(row.get('Customer Price', 0) or 0),
                    ))
                if rows:
                    seen = {}
                    for row in rows:
                        k = (row[0], row[1], row[2], row[3])
                        seen[k] = row
                    execute_values(cur, """
                        INSERT INTO asc_revenue
                          (app_id,date,country,product_type,units,developer_proceeds,customer_price)
                        VALUES %s ON CONFLICT (app_id,date,country,product_type)
                        DO UPDATE SET units=EXCLUDED.units,
                          developer_proceeds=EXCLUDED.developer_proceeds
                    """, list(seen.values()))
                    total += len(seen)
                    print(f'  {date_str}: {len(seen)} rows')
        except Exception as e:
            pass  # Month may not have data

        current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        time.sleep(SLEEP)

    cur.connection.commit()
    print(f'  ✓ {total} total revenue rows')
    return total


COLLECTORS = {
    'apps':      collect_apps,
    'downloads': collect_downloads,
    'usage':     collect_usage,
    'ratings':   collect_ratings,
    'revenue':   collect_revenue,
}


def main(resource_filter=None):
    conn = get_conn()
    cur  = conn.cursor()
    print('Setting up tables...')
    cur.execute(DDL)
    conn.commit()
    print('✓ Tables ready')

    targets = [resource_filter] if resource_filter else list(COLLECTORS.keys())
    for name in targets:
        if name not in COLLECTORS:
            print(f'Unknown: {name}')
            continue
        try:
            n = COLLECTORS[name](cur)
            conn.commit()
            print(f'  ✓ {name}: {n} rows')
        except Exception as e:
            conn.rollback()
            print(f'  ✗ {name}: {e}')

    print('\n=== FINAL COUNTS ===')
    for t in ['asc_apps', 'asc_downloads', 'asc_usage', 'asc_ratings', 'asc_revenue']:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'  {t}: {cur.fetchone()[0]}')
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--resource', help='apps|downloads|usage|ratings|revenue')
    args = parser.parse_args()
    main(args.resource)

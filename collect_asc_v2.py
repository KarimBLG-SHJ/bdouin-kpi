#!/usr/bin/env python3
"""
collect_asc_v2.py — Nouvelle collecte ASC via Analytics Reports API
(remplace les endpoints obsolètes /v1/analytics/apps/time-series et les sales reports
qui exigeaient un vendor number).

Flux :
  1. Pour chaque app, récupérer la analyticsReportRequest ONGOING (créée à l'avance)
  2. Lister les analyticsReports (catégorie COMMERCE / APP_USAGE)
  3. Pour chaque report cible, lister les analyticsReportInstances (DAILY)
  4. Pour chaque nouvelle instance, télécharger les segments (CSV.gz S3)
  5. Parser les CSV et insérer dans asc_downloads / asc_revenue

Usage:
    python3 collect_asc_v2.py
    python3 collect_asc_v2.py --resource downloads
    python3 collect_asc_v2.py --app 1612014910
"""

import argparse, time, jwt, requests, gzip, csv, io, json, os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

KEY_ID    = 'QSFVGW7CN2'
ISSUER_ID = '69a6de83-a19a-47e3-e053-5b8c7c11a4d1'
KEY_FILE  = '/Users/karim/Downloads/AuthKey_QSFVGW7CN2.p8'
DB_URL    = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
ASC_BASE  = 'https://api.appstoreconnect.apple.com/v1'

# Reports we care about (one entry per (report_name, target_table))
TARGET_REPORTS = {
    'App Downloads Standard':   'asc_downloads',
    'App Store Purchases Standard': 'asc_revenue',
    'App Sessions Standard':    'asc_usage',
}

_TOKEN_CACHE = {'tok': None, 'exp': 0}

def jwt_token():
    now = int(time.time())
    if _TOKEN_CACHE['tok'] and _TOKEN_CACHE['exp'] > now + 60:
        return _TOKEN_CACHE['tok']
    with open(KEY_FILE) as f: priv = f.read()
    payload = {'iss': ISSUER_ID, 'iat': now, 'exp': now + 1200, 'aud': 'appstoreconnect-v1'}
    tok = jwt.encode(payload, priv, algorithm='ES256', headers={'kid': KEY_ID})
    _TOKEN_CACHE['tok'] = tok; _TOKEN_CACHE['exp'] = now + 1200
    return tok

def H():
    return {'Authorization': f'Bearer {jwt_token()}'}

def get_apps():
    r = requests.get(f'{ASC_BASE}/apps', headers=H(), params={'limit':50}, timeout=30)
    r.raise_for_status()
    return [(a['id'], a['attributes'].get('name','?')) for a in r.json().get('data', [])]

def find_ongoing_request(app_id):
    r = requests.get(f'{ASC_BASE}/apps/{app_id}/analyticsReportRequests', headers=H(), params={'limit':10}, timeout=30)
    r.raise_for_status()
    for d in r.json().get('data', []):
        if d['attributes'].get('accessType') == 'ONGOING' and not d['attributes'].get('stoppedDueToInactivity'):
            return d['id']
    return None

def list_reports(req_id):
    """Returns dict {report_name: report_id}."""
    out = {}
    r = requests.get(f'{ASC_BASE}/analyticsReportRequests/{req_id}/reports', headers=H(), params={'limit':200}, timeout=30)
    r.raise_for_status()
    for rep in r.json().get('data', []):
        out[rep['attributes'].get('name','?')] = rep['id']
    return out

def list_instances(report_id, since_date=None):
    """Returns list of (instance_id, processing_date, granularity)."""
    out = []
    next_url = f'{ASC_BASE}/analyticsReports/{report_id}/instances?limit=200&filter[granularity]=DAILY'
    while next_url:
        r = requests.get(next_url, headers=H(), timeout=30)
        if r.status_code != 200:
            return out
        body = r.json()
        for inst in body.get('data', []):
            a = inst['attributes']
            d = a.get('processingDate')
            if since_date and d and d <= since_date:
                continue
            out.append((inst['id'], d, a.get('granularity')))
        next_url = body.get('links', {}).get('next')
    return out

def download_segment(seg_url, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.get(seg_url, timeout=120)
            if r.status_code != 200:
                return None
            try:
                return gzip.decompress(r.content).decode('utf-8')
            except Exception:
                return r.content.decode('utf-8', errors='replace')
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == max_retries - 1:
                print(f'    ⚠ segment download failed after {max_retries}: {e}')
                return None
            time.sleep(2 ** attempt)
    return None

def list_segments(instance_id):
    r = requests.get(f'{ASC_BASE}/analyticsReportInstances/{instance_id}/segments', headers=H(), timeout=30)
    if r.status_code != 200:
        return []
    return [s['attributes'].get('url') for s in r.json().get('data', []) if s['attributes'].get('url')]


def get_last_collected(cur, table, app_id):
    cur.execute(f'SELECT MAX(date) FROM {table} WHERE app_id=%s', (app_id,))
    r = cur.fetchone()
    return r[0].isoformat() if r and r[0] else None


def parse_purchases_rows(content, app_id):
    """Parse 'App Store Purchases Standard' CSV → asc_revenue rows.

    Columns: Date, App Name, App Apple Identifier, Purchase Type, Content Name,
             Content Apple Identifier, Payment Method, Device, Platform Version,
             Source Type, Page Type, App Download Date, Pre-Order, Territory,
             Purchases, Proceeds, Proceeds in USD, Paying Users
    """
    reader = csv.DictReader(io.StringIO(content), delimiter='\t')
    agg = {}
    for row in reader:
        date = row.get('Date','')
        if not date: continue
        country = row.get('Territory','') or ''
        ptype   = row.get('Content Name','') or row.get('Purchase Type','')
        try:
            units = int(float(row.get('Purchases',0) or 0))
        except (ValueError, TypeError):
            units = 0
        try:
            proceeds = float(row.get('Proceeds',0) or 0)
        except (ValueError, TypeError):
            proceeds = 0.0
        try:
            customer_price = float(row.get('Proceeds in USD', 0) or 0)
        except (ValueError, TypeError):
            customer_price = 0.0
        key = (app_id, date, country, ptype)
        if key in agg:
            agg[key]['units']    += units
            agg[key]['proceeds'] += proceeds
            agg[key]['cprice']   += customer_price
        else:
            agg[key] = {'units': units, 'proceeds': proceeds, 'cprice': customer_price}
    return [(k[0], k[1], k[2], k[3], v['units'], v['proceeds'], v['cprice']) for k, v in agg.items()]


def collect_app_revenue(cur, app_id, app_name):
    print(f'\n[revenue] {app_name} ({app_id})')
    req_id = find_ongoing_request(app_id)
    if not req_id:
        print('  ✗ no ONGOING request')
        return 0
    reports = list_reports(req_id)
    rep_id = reports.get('App Store Purchases Standard')
    if not rep_id:
        print('  ✗ no Purchases report')
        return 0
    last = get_last_collected(cur, 'asc_revenue', app_id)
    print(f'  last: {last}')
    instances = list_instances(rep_id, since_date=last)
    print(f'  new instances: {len(instances)}')

    inserted = 0
    for i, (inst_id, pdate, gran) in enumerate(instances):
        seg_urls = list_segments(inst_id)
        if not seg_urls:
            continue
        for url in seg_urls:
            content = download_segment(url)
            if not content:
                continue
            rows = parse_purchases_rows(content, app_id)
            if rows:
                seen = {}
                for row in rows:
                    k = (row[0], row[1], row[2], row[3])
                    if k in seen:
                        seen[k] = (row[0], row[1], row[2], row[3],
                                   seen[k][4]+row[4], seen[k][5]+row[5], seen[k][6]+row[6])
                    else:
                        seen[k] = row
                execute_values(cur, """
                    INSERT INTO asc_revenue
                      (app_id,date,country,product_type,units,developer_proceeds,customer_price)
                    VALUES %s
                    ON CONFLICT (app_id,date,country,product_type)
                    DO UPDATE SET units=EXCLUDED.units,
                                  developer_proceeds=EXCLUDED.developer_proceeds,
                                  customer_price=EXCLUDED.customer_price
                """, list(seen.values()))
                inserted += len(seen)
        cur.connection.commit()
        if (i+1) % 5 == 0:
            print(f'    [{i+1}/{len(instances)}] {pdate} (cum: {inserted})')
        time.sleep(0.2)
    print(f'  ✓ {inserted} rows')
    return inserted


def parse_downloads_rows(content, app_id):
    """Parse 'App Downloads Standard' CSV → asc_downloads rows.

    Columns: Date, App Name, App Apple Identifier, Download Type, App Version, Device,
             Platform Version, Source Type, Page Type, Pre-Order, Territory, Counts
    """
    reader = csv.DictReader(io.StringIO(content), delimiter='\t')
    # Aggregate by (app_id, date, territory, device) → sum counts; treat update types separately
    agg = {}
    for row in reader:
        date = row.get('Date','')
        if not date: continue
        country = row.get('Territory','') or ''
        device  = row.get('Device','') or 'all'
        dtype   = (row.get('Download Type','') or '').lower()
        try:
            cnt = int(float(row.get('Counts',0) or 0))
        except (ValueError, TypeError):
            cnt = 0
        key = (app_id, date, country, device)
        if key not in agg:
            agg[key] = {'units': 0, 'updates': 0}
        if 'update' in dtype:
            agg[key]['updates'] += cnt
        else:
            agg[key]['units'] += cnt
    return [(k[0], k[1], k[2], k[3], v['units'], v['updates']) for k, v in agg.items()]


def collect_app_downloads(cur, app_id, app_name):
    print(f'\n[downloads] {app_name} ({app_id})')
    req_id = find_ongoing_request(app_id)
    if not req_id:
        print('  ✗ no ONGOING request — run setup first')
        return 0
    reports = list_reports(req_id)
    rep_id = reports.get('App Downloads Standard')
    if not rep_id:
        print('  ✗ no App Downloads Standard report')
        return 0

    last = get_last_collected(cur, 'asc_downloads', app_id)
    print(f'  last collected: {last}')
    instances = list_instances(rep_id, since_date=last)
    print(f'  new instances: {len(instances)}')

    inserted = 0
    for i, (inst_id, pdate, gran) in enumerate(instances):
        seg_urls = list_segments(inst_id)
        if not seg_urls:
            continue
        for url in seg_urls:
            content = download_segment(url)
            if not content:
                print(f'    [{i+1}/{len(instances)}] {pdate} segment failed')
                continue
            rows = parse_downloads_rows(content, app_id)
            if rows:
                # Dedup by primary key
                seen = {}
                for row in rows:
                    k = (row[0], row[1], row[2], row[3])
                    if k in seen:
                        seen[k] = (row[0], row[1], row[2], row[3],
                                   seen[k][4] + row[4], seen[k][5] + row[5])
                    else:
                        seen[k] = row
                execute_values(cur, """
                    INSERT INTO asc_downloads (app_id,date,country,device,units,updates)
                    VALUES %s
                    ON CONFLICT (app_id,date,country,device)
                    DO UPDATE SET units = EXCLUDED.units, updates = EXCLUDED.updates
                """, list(seen.values()))
                inserted += len(seen)
        cur.connection.commit()
        if (i+1) % 5 == 0:
            print(f'    [{i+1}/{len(instances)}] {pdate} processed (cum: {inserted} rows)')
        time.sleep(0.2)
    print(f'  ✓ {inserted} rows')
    return inserted


def main(resource=None, only_app=None):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    apps = get_apps()
    if only_app:
        apps = [(a, n) for a, n in apps if a == only_app]
    print(f'Apps: {len(apps)}')

    total = 0
    for app_id, name in apps:
        if resource is None or resource == 'downloads':
            try:
                total += collect_app_downloads(cur, app_id, name)
            except Exception as e:
                print(f'  ✗ ERROR (downloads): {e}')
                conn.rollback()
        if resource is None or resource == 'revenue':
            try:
                total += collect_app_revenue(cur, app_id, name)
            except Exception as e:
                print(f'  ✗ ERROR (revenue): {e}')
                conn.rollback()

    print(f'\n=== Total inserted: {total} ===')
    cur.execute("SELECT count(*) FROM asc_downloads"); print('asc_downloads:', cur.fetchone()[0])
    cur.execute("SELECT count(*) FROM asc_revenue");   print('asc_revenue:', cur.fetchone()[0])
    conn.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--resource', choices=['downloads','revenue'])
    p.add_argument('--app', help='Limit to a specific app_id')
    args = p.parse_args()
    main(args.resource, args.app)

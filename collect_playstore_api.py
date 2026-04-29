#!/usr/bin/env python3
"""
collect_playstore_api.py — Aspire les données Google Play via l'API officielle.

Source: Google Play Developer API + Reporting API
Auth:   Service account bdouin-dashboard@heroic-footing-273510 (existant)

Tables :
  playstore_apps       — métadonnées (titre, package, dernière version, ...)
  playstore_reviews    — avis (limité aux 7 derniers jours via API)
  playstore_metrics    — installs/uninstalls/ratings par jour (via Reporting API)

NOTE : Pour les avis historiques (>7 jours), Google Play met à dispo des
       bulk reports dans un bucket GCS — voir collect_playstore_bulk.py
       (à venir) si besoin.

Usage:
    python3 collect_playstore_api.py
    python3 collect_playstore_api.py --resource reviews
"""

import argparse
import time
import json
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
CREDS  = '/tmp/ga4_creds.json'

APPS = {
    'com.bdouin.awladquiz':          'AWLAD QUIZ GO',
    'com.bdouin.awladschool':        'Awlad - Learn Arabic',
    'com.bdouin.awladsalat':         'Awlad - Salat & Ablutions',
    'com.bdouin.awladclassroom':     'Awlad Classroom',
    'com.bdouin.awladquran':         'Awlad Coran',
    'com.bdouin.apps.muslimstrips':  'BDouin Maker',
}

DDL = """
CREATE TABLE IF NOT EXISTS playstore_apps (
    package_name        TEXT PRIMARY KEY,
    title               TEXT,
    default_language    TEXT,
    contact_email       TEXT,
    contact_phone       TEXT,
    contact_website     TEXT,
    listing_data        JSONB,
    collected_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS playstore_reviews (
    id                       TEXT PRIMARY KEY,
    package_name             TEXT,
    author_name              TEXT,
    review_text              TEXT,
    star_rating              INTEGER,
    language_code            TEXT,
    device                   TEXT,
    android_os_version       TEXT,
    app_version_code         INTEGER,
    app_version_name         TEXT,
    review_created_at        TIMESTAMP,
    review_modified_at       TIMESTAMP,
    developer_reply          TEXT,
    developer_replied_at     TIMESTAMP,
    raw                      JSONB,
    collected_at             TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS playstore_metrics (
    package_name        TEXT,
    date                DATE,
    metric              TEXT,
    dimension_country   TEXT,
    value               NUMERIC,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (package_name, date, metric, dimension_country)
);

CREATE INDEX IF NOT EXISTS idx_ps_reviews_pkg     ON playstore_reviews(package_name);
CREATE INDEX IF NOT EXISTS idx_ps_reviews_rating  ON playstore_reviews(star_rating);
CREATE INDEX IF NOT EXISTS idx_ps_reviews_created ON playstore_reviews(review_created_at);
CREATE INDEX IF NOT EXISTS idx_ps_metrics_pkg     ON playstore_metrics(package_name, date);
"""


def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


def get_publisher():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_file(
        CREDS, scopes=['https://www.googleapis.com/auth/androidpublisher']
    )
    return build('androidpublisher', 'v3', credentials=creds, cache_discovery=False)


def get_reporting():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_file(
        CREDS, scopes=['https://www.googleapis.com/auth/playdeveloperreporting']
    )
    return build('playdeveloperreporting', 'v1beta1', credentials=creds, cache_discovery=False)


def collect_app_metadata(cur):
    print('\n[apps] Fetching app metadata...')
    pub = get_publisher()
    inserted = 0
    for pkg, name in APPS.items():
        try:
            # Get app details
            app_info = pub.applications().get(packageName=pkg).execute()

            cur.execute("""
                INSERT INTO playstore_apps
                  (package_name, title, default_language, contact_email,
                   contact_phone, contact_website, listing_data)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (package_name) DO UPDATE SET
                  title=EXCLUDED.title, listing_data=EXCLUDED.listing_data,
                  collected_at=NOW()
            """, (
                pkg,
                app_info.get('title') or name,
                app_info.get('defaultLanguage'),
                app_info.get('contactEmail'),
                app_info.get('contactPhone'),
                app_info.get('contactWebsite'),
                json.dumps(app_info, ensure_ascii=False),
            ))
            inserted += 1
            print(f'  ✓ {name}')
        except Exception as e:
            print(f'  ✗ {name}: {str(e)[:200]}')
        time.sleep(0.5)

    cur.connection.commit()
    return inserted


def collect_reviews(cur):
    """Collect reviews (limited to last 7 days via API)."""
    print('\n[reviews] Fetching reviews (last 7 days)...')
    pub = get_publisher()
    inserted = 0

    for pkg, name in APPS.items():
        print(f'  {name}...')
        try:
            page_token = None
            count = 0
            while True:
                kwargs = {'packageName': pkg, 'maxResults': 100}
                if page_token:
                    kwargs['token'] = page_token

                resp = pub.reviews().list(**kwargs).execute()
                reviews = resp.get('reviews', [])
                if not reviews:
                    break

                rows = []
                for r in reviews:
                    review_id = r.get('reviewId')
                    author    = r.get('authorName', '')
                    comments  = r.get('comments', [])

                    user_comment = None
                    dev_reply    = None
                    for c in comments:
                        if 'userComment' in c:
                            user_comment = c['userComment']
                        elif 'developerComment' in c:
                            dev_reply = c['developerComment']

                    if not user_comment:
                        continue

                    review_text     = user_comment.get('text', '') or ''
                    star_rating     = int(user_comment.get('starRating') or 0)
                    lang_code       = str(user_comment.get('reviewerLanguage', '') or '')
                    device          = str(user_comment.get('device', '') or '')
                    android_os      = str(user_comment.get('androidOsVersion', '') or '')
                    app_version_code= int(user_comment.get('appVersionCode') or 0)
                    app_version_name= str(user_comment.get('appVersionName', '') or '')

                    def parse_ts(v):
                        if not v:
                            return None
                        try:
                            if isinstance(v, dict):
                                return datetime.fromtimestamp(int(v.get('seconds', 0)))
                            if isinstance(v, (int, float)):
                                return datetime.fromtimestamp(int(v))
                            if isinstance(v, str):
                                return datetime.fromisoformat(v.replace('Z','+00:00'))
                        except Exception:
                            return None
                        return None

                    review_created = parse_ts(user_comment.get('lastModified'))

                    dev_reply_text = None
                    dev_replied_at = None
                    if dev_reply:
                        dev_reply_text = dev_reply.get('text', '')
                        dev_replied_at = parse_ts(dev_reply.get('lastModified'))

                    rows.append((
                        review_id, pkg, author[:200], review_text[:5000],
                        star_rating, lang_code[:10], device[:200], android_os[:50],
                        app_version_code, app_version_name[:100],
                        review_created, None,
                        dev_reply_text[:5000] if dev_reply_text else None,
                        dev_replied_at,
                        json.dumps(r, ensure_ascii=False, default=str),
                    ))

                if rows:
                    execute_values(cur, """
                        INSERT INTO playstore_reviews (
                            id, package_name, author_name, review_text, star_rating,
                            language_code, device, android_os_version, app_version_code,
                            app_version_name, review_created_at, review_modified_at,
                            developer_reply, developer_replied_at, raw
                        ) VALUES %s ON CONFLICT (id) DO UPDATE SET
                          star_rating       = EXCLUDED.star_rating,
                          review_text       = EXCLUDED.review_text,
                          developer_reply   = EXCLUDED.developer_reply,
                          developer_replied_at = EXCLUDED.developer_replied_at,
                          collected_at      = NOW()
                    """, rows)
                    count    += len(rows)
                    inserted += len(rows)
                    cur.connection.commit()

                page_token = resp.get('tokenPagination', {}).get('nextPageToken')
                if not page_token:
                    break
                time.sleep(0.3)

            print(f'    {count} reviews')
        except Exception as e:
            cur.connection.rollback()
            print(f'    ✗ {str(e)[:200]}')
        time.sleep(0.5)

    return inserted


def collect_metrics(cur):
    """Collect Play Vitals metrics via Reporting API."""
    print('\n[metrics] Fetching daily metrics via Reporting API...')
    rep = get_reporting()
    inserted = 0

    end   = datetime.utcnow().date() - timedelta(days=5)  # API freshness ~5 days lag
    start = datetime(2024, 1, 1).date()

    # Each metric set has its own resource name suffix and metrics
    METRIC_SETS = [
        # (sub_path, [(api_resource, metric_name)])
        ('crashRateMetricSet', 'crashrate', [
            'userPerceivedCrashRate', 'crashRate', 'distinctUsers',
        ]),
        ('anrRateMetricSet', 'anrrate', [
            'userPerceivedAnrRate', 'anrRate', 'distinctUsers',
        ]),
        ('errorCountMetricSet', 'errorcountmetricset', [
            'errorReportCount', 'distinctUsers',
        ]),
        ('excessiveWakeupRateMetricSet', 'excessivewakeuprate', [
            'excessiveWakeupRate', 'distinctUsers',
        ]),
        ('slowStartRateMetricSet', 'slowstartrate', [
            'slowStartRate', 'distinctUsers',
        ]),
        ('slowRenderingRateMetricSet', 'slowrenderingrate', [
            'slowRenderingRate20Fps', 'slowRenderingRate30Fps', 'distinctUsers',
        ]),
        ('stuckBackgroundWakelockRateMetricSet', 'stuckbackgroundwakelockrate', [
            'stuckBgWakelockRate', 'distinctUsers',
        ]),
    ]

    for pkg, name in APPS.items():
        print(f'  {name}...')
        for sub_path, api_method, metrics in METRIC_SETS:
            try:
                # Some metric sets need MONTHLY aggregation (errorCount)
                aggregation = 'DAILY'
                if sub_path in ('errorCountMetricSet',):
                    aggregation = 'DAILY'

                # Need the Resource — use raw _resourceDesc to find correct method
                # Standard pattern: rep.vitals().XXX().query(...)
                method_collection = getattr(rep.vitals(), api_method, None)
                if not method_collection:
                    continue

                body = {
                    'timelineSpec': {
                        'aggregationPeriod': aggregation,
                        'startTime': {
                            'year': start.year, 'month': start.month, 'day': start.day,
                            'timeZone': {'id': 'America/Los_Angeles'},
                        },
                        'endTime': {
                            'year': end.year, 'month': end.month, 'day': end.day,
                            'timeZone': {'id': 'America/Los_Angeles'},
                        },
                    },
                    'metrics': metrics,
                }
                resp = method_collection().query(
                    name=f'apps/{pkg}/{sub_path}', body=body,
                ).execute()

                rows = []
                for row in resp.get('rows', []):
                    date_obj = row.get('startTime', {})
                    if not date_obj.get('year'):
                        continue
                    d = f"{date_obj['year']}-{date_obj.get('month',1):02d}-{date_obj.get('day',1):02d}"
                    for m in row.get('metrics', []):
                        v = m.get('decimalValue', {}).get('value')
                        if v is None:
                            continue
                        try:
                            rows.append((pkg, d, m['metric'], 'all', float(v)))
                        except (ValueError, TypeError):
                            pass
                if rows:
                    seen = {}
                    for r in rows:
                        seen[(r[0], r[1], r[2], r[3])] = r
                    execute_values(cur, """
                        INSERT INTO playstore_metrics
                          (package_name, date, metric, dimension_country, value)
                        VALUES %s ON CONFLICT (package_name, date, metric, dimension_country)
                        DO UPDATE SET value=EXCLUDED.value
                    """, list(seen.values()))
                    inserted += len(seen)
                    print(f'    {sub_path}: {len(seen)} rows')
                cur.connection.commit()
            except Exception as e:
                err = str(e)[:150]
                if '403' not in err and 'not found' not in err.lower():
                    print(f'    ✗ {sub_path}: {err}')
            time.sleep(0.3)

    return inserted


COLLECTORS = {
    'apps':    collect_app_metadata,
    'reviews': collect_reviews,
    'metrics': collect_metrics,
}


def main(resource_filter=None):
    conn = get_conn()
    cur  = conn.cursor()
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
            print(f'  ✓ {name}: {n} rows')
        except Exception as e:
            conn.rollback()
            print(f'  ✗ {name}: {e}')

    print('\n=== FINAL COUNTS ===')
    for t in ['playstore_apps', 'playstore_reviews', 'playstore_metrics']:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'  {t}: {cur.fetchone()[0]}')
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--resource', help='apps | reviews | metrics')
    args = parser.parse_args()
    main(args.resource)

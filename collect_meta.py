#!/usr/bin/env python3
"""
collect_meta.py — Aspire tout Meta (Facebook + Instagram) dans Railway Postgres.

Tables créées :
  meta_ig_posts             — posts Instagram
  meta_ig_post_insights     — stats par post (impressions, reach, saves, shares, plays)
  meta_ig_comments          — commentaires + réponses par post
  meta_ig_stories           — stories (actives seulement, expire 24h)
  meta_ig_mentions          — posts où BDouin est tagué/mentionné
  meta_ig_audience          — démographie followers (pays, ville, âge, genre)
  meta_ig_account_insights  — stats compte par jour
  meta_fb_posts             — posts Facebook page
  meta_fb_post_insights     — stats par post Facebook
  meta_fb_page_insights     — stats page par jour

Usage:
    python3 collect_meta.py
    python3 collect_meta.py --resource comments
"""

import json
import time
import psycopg2
import requests
from psycopg2.extras import execute_values

USER_TOKEN    = 'EAAwByXGLGTgBRRGOq06aHyJKWjrEdp6E6AJgiGMmZARxNT9ZB4xdCcK2ZB8KrArifUgq9wBxiGbLAm6fEjDhgAWi8gVGQipZAS5smqYVNyr3EqpDnJnH9DdYqLuZCPbQuZArYtQX1ZAgJdZCCsvQxvWZBgLPQnIICWW8UDqrmLYi3F77kkDSH2VxgLxKKojZA9EoNkF9J4CEUk9ioQFdgXOgrzqf2spFKeLoSbG19XQCcJ56zyUHkO'
PAGE_ID       = '105759559500937'
# Page token will be fetched dynamically from USER_TOKEN at runtime
PAGE_TOKEN    = None
IG_ID         = '17841405891623953'
API_BASE      = 'https://graph.facebook.com/v25.0'
DB_URL        = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
SLEEP         = 0.3


def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


def get_page_token():
    """Fetch fresh page token from user token."""
    global PAGE_TOKEN
    if PAGE_TOKEN:
        return PAGE_TOKEN
    r = requests.get(f'{API_BASE}/me/accounts', params={'access_token': USER_TOKEN}, timeout=30)
    r.raise_for_status()
    for p in r.json().get('data', []):
        if p['id'] == PAGE_ID:
            PAGE_TOKEN = p['access_token']
            return PAGE_TOKEN
    raise RuntimeError(f'Page {PAGE_ID} not accessible from USER_TOKEN')


def api(path, params=None, token=None):
    p = {'access_token': token or get_page_token(), **(params or {})}
    r = requests.get(f'{API_BASE}/{path}', params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def paginate(path, params=None, token=None):
    """Fetch all pages of a paginated endpoint."""
    results = []
    data = api(path, params, token)
    results.extend(data.get('data', []))
    while 'paging' in data and 'next' in data.get('paging', {}):
        r = requests.get(data['paging']['next'], timeout=30)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get('data', []))
        time.sleep(SLEEP)
    return results


DDL = """
CREATE TABLE IF NOT EXISTS meta_ig_posts (
    id                  TEXT PRIMARY KEY,
    timestamp           TIMESTAMP,
    media_type          TEXT,
    media_url           TEXT,
    permalink           TEXT,
    caption             TEXT,
    like_count          INTEGER,
    comments_count      INTEGER,
    is_shared_to_feed   BOOLEAN,
    collected_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta_ig_post_insights (
    post_id             TEXT,
    metric              TEXT,
    value               BIGINT,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (post_id, metric)
);

CREATE TABLE IF NOT EXISTS meta_ig_account_insights (
    date                DATE,
    metric              TEXT,
    value               BIGINT,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, metric)
);

CREATE TABLE IF NOT EXISTS meta_ig_audience (
    breakdown           TEXT,
    dimension_key       TEXT,
    value               BIGINT,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (breakdown, dimension_key)
);

CREATE TABLE IF NOT EXISTS meta_fb_posts (
    id                  TEXT PRIMARY KEY,
    message             TEXT,
    story               TEXT,
    created_time        TIMESTAMP,
    permalink_url       TEXT,
    full_picture        TEXT,
    likes_count         INTEGER,
    comments_count      INTEGER,
    shares_count        INTEGER,
    collected_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta_fb_post_insights (
    post_id             TEXT,
    metric              TEXT,
    value               BIGINT,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (post_id, metric)
);

CREATE TABLE IF NOT EXISTS meta_fb_page_insights (
    date                DATE,
    metric              TEXT,
    value               BIGINT,
    collected_at        TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, metric)
);

CREATE TABLE IF NOT EXISTS meta_ig_comments (
    id              TEXT PRIMARY KEY,
    post_id         TEXT,
    parent_id       TEXT,
    username        TEXT,
    text            TEXT,
    like_count      INTEGER,
    timestamp       TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta_ig_stories (
    id              TEXT PRIMARY KEY,
    timestamp       TIMESTAMP,
    media_type      TEXT,
    media_url       TEXT,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta_ig_mentions (
    id              TEXT PRIMARY KEY,
    timestamp       TIMESTAMP,
    caption         TEXT,
    media_url       TEXT,
    permalink       TEXT,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_meta_ig_posts_ts    ON meta_ig_posts(timestamp);
CREATE INDEX IF NOT EXISTS idx_meta_fb_posts_ts    ON meta_fb_posts(created_time);
CREATE INDEX IF NOT EXISTS idx_meta_ig_insights    ON meta_ig_account_insights(date);
CREATE INDEX IF NOT EXISTS idx_meta_fb_insights    ON meta_fb_page_insights(date);
CREATE INDEX IF NOT EXISTS idx_meta_ig_comments_post ON meta_ig_comments(post_id);
CREATE INDEX IF NOT EXISTS idx_meta_ig_comments_ts   ON meta_ig_comments(timestamp);
CREATE INDEX IF NOT EXISTS idx_meta_ig_mentions_ts   ON meta_ig_mentions(timestamp);
"""


# ─── Instagram ────────────────────────────────────────────────────────────────

def collect_ig_posts(cur):
    print('\n[ig_posts] Fetching all Instagram posts...')
    posts = paginate(f'{IG_ID}/media', {
        'fields': 'id,timestamp,media_type,media_url,permalink,caption,like_count,comments_count,is_shared_to_feed',
        'limit': 100,
    })
    print(f'  {len(posts)} posts found')

    rows = []
    for p in posts:
        rows.append((
            p['id'],
            p.get('timestamp'),
            p.get('media_type'),
            p.get('media_url', '')[:1000],
            p.get('permalink', '')[:500],
            p.get('caption', '')[:5000],
            int(p.get('like_count') or 0),
            int(p.get('comments_count') or 0),
            p.get('is_shared_to_feed', False),
        ))

    if rows:
        execute_values(cur, """
            INSERT INTO meta_ig_posts
              (id,timestamp,media_type,media_url,permalink,caption,like_count,comments_count,is_shared_to_feed)
            VALUES %s ON CONFLICT (id) DO UPDATE SET
              like_count=EXCLUDED.like_count, comments_count=EXCLUDED.comments_count
        """, rows)
    print(f'  {len(rows)} rows inserted')
    return len(rows)


def collect_ig_post_insights(cur):
    print('\n[ig_post_insights] Fetching insights per post...')
    cur.execute('SELECT id FROM meta_ig_posts ORDER BY timestamp')
    post_ids = [r[0] for r in cur.fetchall()]

    # Already collected
    cur.execute('SELECT DISTINCT post_id FROM meta_ig_post_insights')
    done = {r[0] for r in cur.fetchall()}
    post_ids = [p for p in post_ids if p not in done]
    print(f'  {len(post_ids)} posts to process')

    # Metrics depend on media type
    METRICS_IMAGE = 'impressions,reach,saved,profile_visits,follows,shares'
    METRICS_VIDEO = 'impressions,reach,saved,profile_visits,follows,shares,plays,ig_reels_avg_watch_time,ig_reels_video_view_total_time'

    inserted = 0
    for i, pid in enumerate(post_ids):
        # Get media type
        cur.execute('SELECT media_type FROM meta_ig_posts WHERE id=%s', (pid,))
        row = cur.fetchone()
        media_type = row[0] if row else 'IMAGE'

        metrics = METRICS_VIDEO if media_type in ('VIDEO', 'REELS') else METRICS_IMAGE

        try:
            data = api(f'{pid}/insights', {'metric': metrics, 'period': 'lifetime'})
            rows = []
            for m in data.get('data', []):
                rows.append((pid, m['name'], int(m['values'][0]['value'] if m.get('values') else m.get('value', 0))))
            if rows:
                execute_values(cur, """
                    INSERT INTO meta_ig_post_insights (post_id, metric, value)
                    VALUES %s ON CONFLICT (post_id, metric) DO UPDATE SET value=EXCLUDED.value
                """, rows)
                inserted += len(rows)
        except Exception as e:
            print(f'    ✗ {pid}: {e}')

        time.sleep(SLEEP)
        if (i+1) % 50 == 0:
            cur.connection.commit()
            print(f'  {i+1}/{len(post_ids)} — {inserted} metrics inserted')

    print(f'  ✓ {inserted} total metrics')
    return inserted


def collect_ig_account_insights(cur):
    print('\n[ig_account_insights] Fetching daily account stats...')
    # v25 API: metrics must be queried one by one, period=day
    metrics = [
        'reach', 'follower_count', 'profile_views',
        'website_clicks', 'accounts_engaged', 'total_interactions',
        'likes', 'comments', 'shares', 'saves',
    ]
    rows = []
    for metric in metrics:
        try:
            data = api(f'{IG_ID}/insights', {
                'metric': metric,
                'period': 'day',
                'since': '2020-01-01',
                'until': '2026-04-27',
            })
            for item in data.get('data', []):
                for point in item.get('values', []):
                    v = point.get('value', 0)
                    if isinstance(v, dict):
                        v = sum(v.values())
                    rows.append((point['end_time'][:10], item['name'], int(v or 0)))
        except Exception as e:
            print(f'    ✗ {metric}: {e}')
        time.sleep(SLEEP)

    if rows:
        # dedup
        seen = {}
        for r in rows:
            seen[(r[0], r[1])] = r
        rows = list(seen.values())
        execute_values(cur, """
            INSERT INTO meta_ig_account_insights (date, metric, value)
            VALUES %s ON CONFLICT (date, metric) DO UPDATE SET value=EXCLUDED.value
        """, rows)
    print(f'  {len(rows)} rows')
    return len(rows)


def collect_ig_audience(cur):
    print('\n[ig_audience] Fetching audience demographics...')
    breakdowns = {
        'country':      'audience_country',
        'city':         'audience_city',
        'age_gender':   'audience_gender_age',
    }
    rows = []
    for key, metric in breakdowns.items():
        try:
            data = api(f'{IG_ID}/insights', {'metric': metric, 'period': 'lifetime'})
            values = data.get('data', [{}])[0].get('values', [{}])[-1].get('value', {}) if data.get('data') else {}
            for dim, val in values.items():
                rows.append((key, dim[:200], int(val)))
        except Exception as e:
            print(f'    ✗ {key}: {e}')
        time.sleep(SLEEP)

    if rows:
        execute_values(cur, """
            INSERT INTO meta_ig_audience (breakdown, dimension_key, value)
            VALUES %s ON CONFLICT (breakdown, dimension_key) DO UPDATE SET value=EXCLUDED.value
        """, rows)
    print(f'  {len(rows)} audience dimensions')
    return len(rows)


def collect_ig_comments(cur):
    print('\n[ig_comments] Fetching comments for all posts...')
    cur.execute('SELECT id FROM meta_ig_posts ORDER BY timestamp DESC')
    post_ids = [r[0] for r in cur.fetchall()]

    cur.execute('SELECT DISTINCT post_id FROM meta_ig_comments')
    done = {r[0] for r in cur.fetchall()}
    post_ids = [p for p in post_ids if p not in done]
    print(f'  {len(post_ids)} posts to process')

    inserted = 0
    for i, pid in enumerate(post_ids):
        try:
            data = api(f'{pid}/comments', {
                'fields': 'id,text,username,timestamp,like_count,replies{id,text,username,timestamp,like_count}',
                'limit': 100,
            })
            rows = []
            for c in data.get('data', []):
                rows.append((c['id'], pid, None, c.get('username'), c.get('text','')[:2000],
                             int(c.get('like_count',0)), c.get('timestamp')))
                # replies
                for reply in c.get('replies', {}).get('data', []):
                    rows.append((reply['id'], pid, c['id'], reply.get('username'),
                                 reply.get('text','')[:2000], int(reply.get('like_count',0)),
                                 reply.get('timestamp')))
            if rows:
                execute_values(cur, """
                    INSERT INTO meta_ig_comments (id,post_id,parent_id,username,text,like_count,timestamp)
                    VALUES %s ON CONFLICT (id) DO UPDATE SET like_count=EXCLUDED.like_count
                """, rows)
                inserted += len(rows)
        except Exception as e:
            pass  # post may have comments disabled
        time.sleep(SLEEP)
        if (i+1) % 100 == 0:
            cur.connection.commit()
            print(f'  {i+1}/{len(post_ids)} — {inserted} comments')

    print(f'  ✓ {inserted} total comments')
    return inserted


def collect_ig_stories(cur):
    print('\n[ig_stories] Fetching active stories...')
    # Stories expire after 24h — only current ones available
    try:
        data = api(f'{IG_ID}/stories', {
            'fields': 'id,timestamp,media_type,media_url',
            'limit': 100,
        })
        rows = [(s['id'], s.get('timestamp'), s.get('media_type'), s.get('media_url','')[:1000])
                for s in data.get('data', [])]
        if rows:
            execute_values(cur, """
                INSERT INTO meta_ig_stories (id,timestamp,media_type,media_url)
                VALUES %s ON CONFLICT (id) DO NOTHING
            """, rows)
        print(f'  {len(rows)} active stories')
        return len(rows)
    except Exception as e:
        print(f'  ✗ {e}')
        return 0


def collect_ig_mentions(cur):
    print('\n[ig_mentions] Fetching posts where BDouin is tagged/mentioned...')
    try:
        mentions = paginate(f'{IG_ID}/tags', {
            'fields': 'id,timestamp,caption,media_url,permalink',
            'limit': 100,
        })
        rows = [(m['id'], m.get('timestamp'), m.get('caption','')[:2000],
                 m.get('media_url','')[:1000], m.get('permalink','')[:500])
                for m in mentions]
        if rows:
            execute_values(cur, """
                INSERT INTO meta_ig_mentions (id,timestamp,caption,media_url,permalink)
                VALUES %s ON CONFLICT (id) DO NOTHING
            """, rows)
        print(f'  {len(rows)} mentions')
        return len(rows)
    except Exception as e:
        print(f'  ✗ {e}')
        return 0


# ─── Facebook ─────────────────────────────────────────────────────────────────

def collect_fb_posts(cur):
    print('\n[fb_posts] Fetching all Facebook posts...')
    posts = paginate(f'{PAGE_ID}/posts', {
        'fields': 'id,message,story,created_time,permalink_url,full_picture,likes.summary(true),comments.summary(true),shares',
        'limit': 100,
    })
    print(f'  {len(posts)} posts found')

    rows = []
    for p in posts:
        rows.append((
            p['id'],
            p.get('message', '')[:5000],
            p.get('story', '')[:1000],
            p.get('created_time'),
            p.get('permalink_url', '')[:500],
            p.get('full_picture', '')[:1000],
            int(p.get('likes', {}).get('summary', {}).get('total_count', 0)),
            int(p.get('comments', {}).get('summary', {}).get('total_count', 0)),
            int(p.get('shares', {}).get('count', 0)),
        ))

    if rows:
        execute_values(cur, """
            INSERT INTO meta_fb_posts
              (id,message,story,created_time,permalink_url,full_picture,likes_count,comments_count,shares_count)
            VALUES %s ON CONFLICT (id) DO UPDATE SET
              likes_count=EXCLUDED.likes_count, comments_count=EXCLUDED.comments_count,
              shares_count=EXCLUDED.shares_count
        """, rows)
    print(f'  {len(rows)} rows')
    return len(rows)


def collect_fb_post_insights(cur):
    print('\n[fb_post_insights] Fetching insights per FB post...')
    cur.execute('SELECT id FROM meta_fb_posts ORDER BY created_time')
    post_ids = [r[0] for r in cur.fetchall()]
    cur.execute('SELECT DISTINCT post_id FROM meta_fb_post_insights')
    done = {r[0] for r in cur.fetchall()}
    post_ids = [p for p in post_ids if p not in done]
    print(f'  {len(post_ids)} posts to process')

    metrics = 'post_impressions,post_impressions_unique,post_engaged_users,post_clicks,post_reactions_by_type_total'
    inserted = 0
    for i, pid in enumerate(post_ids):
        try:
            data = api(f'{pid}/insights', {'metric': metrics, 'period': 'lifetime'})
            rows = []
            for m in data.get('data', []):
                v = m.get('values', [{}])[0].get('value', 0)
                if isinstance(v, dict):
                    for k, val in v.items():
                        rows.append((pid, f"{m['name']}_{k}", int(val)))
                else:
                    rows.append((pid, m['name'], int(v or 0)))
            if rows:
                execute_values(cur, """
                    INSERT INTO meta_fb_post_insights (post_id, metric, value)
                    VALUES %s ON CONFLICT (post_id, metric) DO UPDATE SET value=EXCLUDED.value
                """, rows)
                inserted += len(rows)
        except Exception as e:
            print(f'    ✗ {pid}: {e}')
        time.sleep(SLEEP)
        if (i+1) % 50 == 0:
            cur.connection.commit()
            print(f'  {i+1}/{len(post_ids)}')

    print(f'  ✓ {inserted} metrics')
    return inserted


def collect_fb_page_insights(cur):
    print('\n[fb_page_insights] Fetching daily page stats...')
    metrics = [
        'page_fans', 'page_impressions', 'page_impressions_unique',
        'page_engaged_users', 'page_views_total',
        'page_post_engagements', 'page_fan_adds', 'page_fan_removes',
    ]
    rows = []
    for metric in metrics:
        try:
            data = api(f'{PAGE_ID}/insights/{metric}/day', {
                'since': '2015-01-01',
                'until': '2026-04-27',
                'limit': 5000,
            })
            for point in data.get('data', []):
                rows.append((point['period'] if 'end_time' not in point else point['end_time'][:10],
                             metric, int(point.get('value', 0))))
            # Handle pagination
            while 'paging' in data and 'next' in data.get('paging', {}):
                r = requests.get(data['paging']['next'], timeout=30)
                data = r.json()
                for point in data.get('data', []):
                    rows.append((point['end_time'][:10], metric, int(point.get('value', 0))))
        except Exception as e:
            print(f'    ✗ {metric}: {e}')
        time.sleep(SLEEP)

    if rows:
        seen = {}
        for r in rows:
            seen[(r[0], r[1])] = r
        rows = list(seen.values())
        execute_values(cur, """
            INSERT INTO meta_fb_page_insights (date, metric, value)
            VALUES %s ON CONFLICT (date, metric) DO UPDATE SET value=EXCLUDED.value
        """, rows)
    print(f'  {len(rows)} rows')
    return len(rows)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    conn = get_conn()
    cur  = conn.cursor()
    print('Setting up tables...')
    cur.execute(DDL)
    conn.commit()
    print('✓ Tables ready')

    collectors = [
        ('ig_posts',            collect_ig_posts),
        ('ig_post_insights',    collect_ig_post_insights),
        ('ig_comments',         collect_ig_comments),
        ('ig_stories',          collect_ig_stories),
        ('ig_mentions',         collect_ig_mentions),
        ('ig_account_insights', collect_ig_account_insights),
        ('ig_audience',         collect_ig_audience),
        ('fb_posts',            collect_fb_posts),
        ('fb_post_insights',    collect_fb_post_insights),
        ('fb_page_insights',    collect_fb_page_insights),
    ]

    for name, fn in collectors:
        try:
            n = fn(cur)
            conn.commit()
            print(f'  ✓ {name}: {n} rows')
        except Exception as e:
            conn.rollback()
            print(f'  ✗ {name}: {e}')

    print('\n=== FINAL COUNTS ===')
    for t in ['meta_ig_posts', 'meta_ig_post_insights', 'meta_ig_comments',
              'meta_ig_stories', 'meta_ig_mentions', 'meta_ig_account_insights',
              'meta_ig_audience', 'meta_fb_posts', 'meta_fb_post_insights', 'meta_fb_page_insights']:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'  {t}: {cur.fetchone()[0]}')

    conn.close()
    print('\nDone!')


if __name__ == '__main__':
    main()

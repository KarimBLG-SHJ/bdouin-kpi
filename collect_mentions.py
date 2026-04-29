#!/usr/bin/env python3
"""
collect_mentions.py — Veille web : collecte mentions BDouin sur Reddit, YouTube,
                       et via Google Custom Search (general web).

Sources :
  - Reddit (public JSON, no auth)
  - YouTube Data API (existing service account)
  - Google Programmable Search (à configurer)

Stockage : table `web_mentions` (existante)

Usage:
    python3 collect_mentions.py
    python3 collect_mentions.py --source reddit
"""

import argparse
import hashlib
import json
import time
import psycopg2
import requests
from psycopg2.extras import execute_values

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# ─── Mots-clés de veille ──────────────────────────────────────────────────────

KEYWORDS = {
    # Marques
    'bdouin':              'brand',
    'awlad school':        'brand',
    'awlad quiz':          'brand',
    'hoopow':              'brand',
    'famille foulane':     'brand',
    'halua':               'brand',
    'muslim show':         'brand',
    'walad et binti':      'brand',
    'wldbnt':              'brand',
    'agence règle tout':   'brand',
    'amir of harlem':      'brand',
    # Auteurs
    'norédine allam':      'author',
    'noredine allam':      'author',
    'studio bdouin':       'author',
    # Séries / titres
    'recueil muslim show': 'title',
    'citadelle du petit muslim': 'title',
    'guide du hajj':       'title',
    'guide du super étudiant': 'title',
    'mois béni du ramadan': 'title',
    'recueil des bonnes actions': 'title',
    'walad découvre la mecque': 'title',
    'walad découvre médine': 'title',
}

USER_AGENT = 'BDouin-Watch/1.0 (web mentions collector)'


def get_conn():
    return psycopg2.connect(DB_URL)


def stable_id(source: str, url: str) -> str:
    return hashlib.sha256(f'{source}|{url}'.encode()).hexdigest()[:32]


def insert_mentions(cur, rows):
    """rows: list of dicts with id, source, url, title, snippet, author, mention_date, keyword, raw"""
    if not rows:
        return 0
    values = [(
        r['id'], r['source'], r['url'][:1000], r.get('title', '')[:500],
        r.get('snippet', '')[:2000], r.get('author', '')[:200] if r.get('author') else None,
        r.get('mention_date'), r['keyword'], None,  # sentiment NULL for now
        json.dumps(r.get('raw', {}), ensure_ascii=False, default=str),
    ) for r in rows]
    execute_values(cur, """
        INSERT INTO web_mentions
          (id, source, url, title, snippet, author, mention_date, keyword, sentiment, raw)
        VALUES %s ON CONFLICT (id) DO NOTHING
    """, values)
    return len(values)


# ─── Reddit ───────────────────────────────────────────────────────────────────

def search_reddit(keyword: str, limit: int = 100):
    """Search Reddit using public JSON, no auth. Returns list of mentions."""
    results = []
    after = None
    fetched = 0

    while fetched < limit:
        params = {
            'q': f'"{keyword}"',
            'limit': min(100, limit - fetched),
            'sort': 'new',
            't': 'all',
            'type': 'link',
        }
        if after:
            params['after'] = after

        try:
            r = requests.get('https://www.reddit.com/search.json',
                             params=params,
                             headers={'User-Agent': USER_AGENT},
                             timeout=20)
            if not r.ok:
                break
            data = r.json()
            children = data['data']['children']
            if not children:
                break

            for c in children:
                p = c['data']
                # Use Reddit's own ID if available
                rid = p.get('id') or stable_id('reddit', p.get('url', ''))
                # Date
                from datetime import datetime, timezone
                mdate = datetime.fromtimestamp(p.get('created_utc', 0),
                                                tz=timezone.utc).date() if p.get('created_utc') else None
                results.append({
                    'id':          stable_id('reddit', f"reddit_{rid}"),
                    'source':      'reddit',
                    'url':         f"https://www.reddit.com{p.get('permalink', '')}",
                    'title':       p.get('title', ''),
                    'snippet':     p.get('selftext', '')[:2000],
                    'author':      p.get('author', ''),
                    'mention_date': mdate,
                    'keyword':     keyword,
                    'raw': {
                        'subreddit':    p.get('subreddit'),
                        'score':        p.get('score'),
                        'num_comments': p.get('num_comments'),
                        'over_18':      p.get('over_18'),
                        'is_self':      p.get('is_self'),
                        'link_url':     p.get('url'),
                    },
                })

            fetched += len(children)
            after = data['data'].get('after')
            if not after:
                break
            time.sleep(2)  # respect Reddit rate limit
        except Exception as e:
            print(f'    ✗ {keyword}: {e}')
            break

    return results


def collect_reddit(cur):
    print('\n[reddit] Searching all keywords...')
    total = 0
    for kw in KEYWORDS:
        try:
            mentions = search_reddit(kw, limit=200)
            n = insert_mentions(cur, mentions)
            cur.connection.commit()
            total += n
            print(f'  "{kw}": {n} mentions')
            time.sleep(2)
        except Exception as e:
            cur.connection.rollback()
            print(f'  ✗ "{kw}": {e}')
    return total


# ─── YouTube ──────────────────────────────────────────────────────────────────

def collect_youtube(cur):
    """Search YouTube via Data API v3 — needs the API enabled in GCP."""
    print('\n[youtube] Searching videos...')
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds = service_account.Credentials.from_service_account_file(
            '/tmp/ga4_creds.json',
            scopes=['https://www.googleapis.com/auth/youtube.readonly']
        )
        yt = build('youtube', 'v3', credentials=creds)
    except Exception as e:
        print(f'  ✗ YouTube auth: {e}')
        return 0

    total = 0
    for kw in KEYWORDS:
        try:
            req = yt.search().list(
                q=f'"{kw}"',
                part='snippet',
                type='video',
                maxResults=50,
                order='date',
            )
            resp = req.execute()
            mentions = []
            for item in resp.get('items', []):
                vid = item['id']['videoId']
                sn  = item['snippet']
                mentions.append({
                    'id':           stable_id('youtube', f'video_{vid}'),
                    'source':       'youtube',
                    'url':          f'https://www.youtube.com/watch?v={vid}',
                    'title':        sn.get('title', ''),
                    'snippet':      sn.get('description', '')[:2000],
                    'author':       sn.get('channelTitle', ''),
                    'mention_date': sn.get('publishedAt', '')[:10] or None,
                    'keyword':      kw,
                    'raw': {
                        'channel_id':   sn.get('channelId'),
                        'thumbnails':   sn.get('thumbnails', {}).get('default', {}).get('url'),
                        'live_broadcast': sn.get('liveBroadcastContent'),
                    },
                })
            n = insert_mentions(cur, mentions)
            cur.connection.commit()
            total += n
            print(f'  "{kw}": {n} videos')
            time.sleep(0.5)
        except Exception as e:
            cur.connection.rollback()
            err = str(e)
            print(f'  ✗ "{kw}": {err[:100]}')
            if 'has not been used' in err or 'is disabled' in err:
                print('  → YouTube Data API v3 not enabled in GCP project')
                break
    return total


# ─── Google Custom Search (web général) ───────────────────────────────────────

def collect_google_cse(cur, api_key=None, cse_id=None):
    """Google Programmable Search — requires API key + CSE ID (free 100 q/jour)."""
    if not (api_key and cse_id):
        print('\n[google_cse] Skipping — needs GOOGLE_CSE_API_KEY + GOOGLE_CSE_ID')
        return 0
    print('\n[google_cse] Searching general web...')
    total = 0
    for kw in KEYWORDS:
        try:
            r = requests.get('https://www.googleapis.com/customsearch/v1', params={
                'key': api_key, 'cx': cse_id, 'q': f'"{kw}"', 'num': 10,
            }, timeout=20)
            if not r.ok:
                continue
            data = r.json()
            mentions = []
            for item in data.get('items', []):
                mentions.append({
                    'id':       stable_id('google_cse', item['link']),
                    'source':   'web_google',
                    'url':      item['link'],
                    'title':    item.get('title', ''),
                    'snippet':  item.get('snippet', ''),
                    'author':   item.get('displayLink', ''),
                    'keyword':  kw,
                    'raw': item,
                })
            total += insert_mentions(cur, mentions)
            cur.connection.commit()
            time.sleep(1)
        except Exception as e:
            print(f'  ✗ {kw}: {e}')
    return total


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(source_filter=None):
    conn = get_conn()
    cur  = conn.cursor()
    sources = {
        'reddit':    collect_reddit,
        'youtube':   collect_youtube,
    }
    targets = [source_filter] if source_filter else list(sources.keys())

    grand_total = 0
    for s in targets:
        if s not in sources:
            print(f'Unknown source: {s}')
            continue
        try:
            n = sources[s](cur)
            grand_total += n
            print(f'  ✓ {s}: {n} new mentions')
        except Exception as e:
            conn.rollback()
            print(f'  ✗ {s}: {e}')

    cur.execute('SELECT source, COUNT(*) FROM web_mentions GROUP BY source ORDER BY 2 DESC')
    print('\n=== TOTAL MENTIONS BY SOURCE ===')
    for r in cur.fetchall():
        print(f'  {r[0]}: {r[1]}')

    conn.close()
    print(f'\nDone! New: {grand_total}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', help='reddit | youtube')
    args = parser.parse_args()
    main(args.source)

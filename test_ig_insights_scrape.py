#!/usr/bin/env python3
"""
Pré-flight test : peut-on récupérer reach/impressions historiques d'IG via
l'endpoint creator interne avec un cookie sessionid muslimshow ?

Usage :
  IG_SESSIONID="..." python3 test_ig_insights_scrape.py
"""
import json
import os
import sys
import time
import requests

COOKIE_STR = os.environ.get('IG_COOKIE')
if not COOKIE_STR:
    print('✗ IG_COOKIE non défini. Coller la ligne Cookie complète depuis devtools Network → Request Headers.')
    sys.exit(1)

# Posts test
RECENT_POST = {'id': '18115727065683284', 'date': '2026-05-01', 'reach_db': 16195,
               'permalink': 'https://www.instagram.com/p/DXzQi5uDQ_F/'}
OLD_POST    = {'id': '18050888281135421', 'date': '2019-07-11', 'reach_db': 1216,
               'permalink': 'https://www.instagram.com/p/Bzx4DgwowvH/'}

# Endpoints à tester
ENDPOINTS = [
    'https://www.instagram.com/api/v1/insights/post_insights/?media_id={mid}',
    'https://i.instagram.com/api/v1/insights/post_insights/?media_id={mid}',
    'https://www.instagram.com/api/v1/insights/insights_facade/?media_id={mid}',
    'https://i.instagram.com/api/v1/media/{mid}/insights/',
]
APP_ID = '936619743392459'  # web app id


def parse_cookies(raw):
    out = {}
    for part in raw.split(';'):
        if '=' in part:
            k, v = part.strip().split('=', 1)
            out[k.strip()] = v.strip()
    return out


def build_session():
    cookies = parse_cookies(COOKIE_STR)
    print(f'  cookies parsées : {list(cookies.keys())}')
    s = requests.Session()
    for k, v in cookies.items():
        s.cookies.set(k, v, domain='.instagram.com', path='/')
    csrf = cookies.get('csrftoken', '')
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        'X-IG-App-ID': APP_ID,
        'X-ASBD-ID': '129477',
        'X-Requested-With': 'XMLHttpRequest',
        'X-CSRFToken': csrf,
        'Referer': 'https://www.instagram.com/muslimshow/',
        'Origin': 'https://www.instagram.com',
    })
    # warmup
    r = s.get('https://www.instagram.com/muslimshow/', timeout=15)
    logged_in = 'not-logged-in' not in r.text[:1500].lower()
    print(f'  warmup → status={r.status_code}, logged_in={logged_in}, csrftoken={"OK" if csrf else "MISSING"}')
    return s


def probe(s, post):
    print(f'\n→ POST {post["date"]} (media_id={post["id"]}, reach_db={post["reach_db"]})')
    print(f'  {post["permalink"]}')
    for tpl in ENDPOINTS:
        url = tpl.format(mid=post['id'])
        r = s.get(url, timeout=20, allow_redirects=False)
        ctype = r.headers.get('content-type', '')
        print(f'  → {tpl.split("//")[1][:50]}… HTTP {r.status_code} ct={ctype[:30]}')
        if r.status_code == 200 and 'json' in ctype.lower():
            try:
                data = r.json()
            except Exception as e:
                print(f'    ✗ JSON parse: {e} body[:200]={r.text[:200]}')
                continue
            flat = json.dumps(data)
            print(f'    JSON top keys: {list(data.keys())[:15]}')
            hits = [(k, v) for k, v in _walk(data)
                    if any(t in str(k).lower() for t in ('reach', 'impression', 'view'))]
            if hits:
                for k, v in hits[:25]:
                    print(f'      {k} = {v}')
            else:
                print(f'    preview: {flat[:400]}')
            return  # premier endpoint qui marche → stop
        elif r.status_code == 200:
            print(f'    body[:200]: {r.text[:200]}')
        else:
            print(f'    body[:200]: {r.text[:200]}')
        time.sleep(1)


def _walk(obj, prefix=''):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk(v, f'{prefix}.{k}' if prefix else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk(v, f'{prefix}[{i}]')
    else:
        yield prefix, obj


if __name__ == '__main__':
    print('=== Pré-flight scrape IG insights ===')
    s = build_session()
    probe(s, RECENT_POST)
    time.sleep(3)
    probe(s, OLD_POST)
    print('\n=== Fin ===')

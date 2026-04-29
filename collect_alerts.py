#!/usr/bin/env python3
"""
collect_alerts.py — Aspire les flux RSS Google Alerts dans web_mentions.

Sources : Google Alerts RSS feeds (un par mot-clé)
Stockage : web_mentions (source='google_alerts')

Usage:
    python3 collect_alerts.py
    python3 collect_alerts.py --keyword "bdouin"
"""

import argparse
import hashlib
import json
import time
import psycopg2
import requests
import re
from psycopg2.extras import execute_values
from email.utils import parsedate_to_datetime

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# ─── Mapping keyword → RSS URL ────────────────────────────────────────────────
# Remplis ce dict avec les URLs RSS de chaque alerte Google.
# Format URL : https://www.google.com/alerts/feeds/<USER_ID>/<ALERT_ID>

FEED_BASE = "https://www.google.com/alerts/feeds/00822420212869337701"
ALERTS = {
    'wldbnt':              f'{FEED_BASE}/8533362168436127113',
    'halua':               f'{FEED_BASE}/7862403233899347602',
    'walad et binti':      f'{FEED_BASE}/16747022149601389248',
    'agence règle tout':   f'{FEED_BASE}/1094261861674168185',
    'noredine allam':      f'{FEED_BASE}/16658229740996436699',
    'bdouin':              f'{FEED_BASE}/11991364051139922870',
    'famille foulane':     f'{FEED_BASE}/9446247396535431511',
    'muslim show':         f'{FEED_BASE}/11269546327643782880',
    'awlad':               f'{FEED_BASE}/14777545859567591304',
    'awlad school':        f'{FEED_BASE}/16320347802790698618',
    'amir of harlem':      f'{FEED_BASE}/8660395327288911444',
    'studio bdouin':       f'{FEED_BASE}/16775053692810814824',
}


def get_conn():
    return psycopg2.connect(DB_URL)


def stable_id(source, url):
    return hashlib.sha256(f'{source}|{url}'.encode()).hexdigest()[:32]


def strip_html(s):
    if not s:
        return ''
    s = re.sub(r'<[^>]+>', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def parse_atom(xml_text):
    """Lightweight Atom parser (Google Alerts uses Atom 1.0)."""
    import xml.etree.ElementTree as ET

    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    root = ET.fromstring(xml_text)
    entries = []

    for e in root.findall('atom:entry', ns):
        title = (e.findtext('atom:title', '', ns) or '').strip()
        # link: <link href="..."/>
        link_el = e.find('atom:link', ns)
        link = link_el.get('href') if link_el is not None else ''
        # Google wraps real URL in google redirect: extract `url=` param
        m = re.search(r'url=([^&]+)', link)
        if m:
            from urllib.parse import unquote
            link = unquote(m.group(1))
        published = e.findtext('atom:published', '', ns)
        author = e.findtext('atom:author/atom:name', '', ns) or ''
        content = e.findtext('atom:content', '', ns) or e.findtext('atom:summary', '', ns) or ''
        content = strip_html(content)
        title = strip_html(title)

        try:
            mention_date = parsedate_to_datetime(published).date() if published else None
        except Exception:
            mention_date = published[:10] if published else None

        entries.append({
            'title':        title,
            'url':          link,
            'snippet':      content[:2000],
            'author':       author[:200],
            'mention_date': mention_date,
        })
    return entries


def collect_keyword(cur, keyword, rss_url):
    try:
        r = requests.get(rss_url, timeout=20,
                         headers={'User-Agent': 'BDouin-Watch/1.0'})
        r.raise_for_status()
    except Exception as e:
        print(f'  ✗ {keyword}: {e}')
        return 0

    try:
        entries = parse_atom(r.text)
    except Exception as e:
        print(f'  ✗ {keyword} parse: {e}')
        return 0

    rows = []
    for entry in entries:
        if not entry['url']:
            continue
        rows.append((
            stable_id('google_alerts', entry['url']),
            'google_alerts',
            entry['url'][:1000],
            entry['title'][:500],
            entry['snippet'],
            entry['author'] or None,
            entry['mention_date'],
            keyword,
            None,
            json.dumps({'feed': rss_url}, ensure_ascii=False),
        ))

    if rows:
        execute_values(cur, """
            INSERT INTO web_mentions
              (id, source, url, title, snippet, author, mention_date, keyword, sentiment, raw)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
    return len(rows)


def main(keyword_filter=None):
    if not ALERTS:
        print('⚠️  Aucune alerte configurée. Édite ALERTS dans collect_alerts.py')
        return

    conn = get_conn()
    cur  = conn.cursor()

    targets = {keyword_filter: ALERTS[keyword_filter]} if keyword_filter else ALERTS
    total = 0

    print(f'[google_alerts] {len(targets)} feeds à traiter...')
    for kw, url in targets.items():
        n = collect_keyword(cur, kw, url)
        conn.commit()
        total += n
        print(f'  "{kw}": {n} mentions')
        time.sleep(1)

    cur.execute("SELECT COUNT(*) FROM web_mentions WHERE source='google_alerts'")
    print(f'\n  Total google_alerts: {cur.fetchone()[0]}')
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--keyword', help='Run only one keyword')
    args = parser.parse_args()
    main(args.keyword)

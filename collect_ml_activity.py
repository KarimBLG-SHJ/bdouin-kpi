"""
collect_ml_activity.py — Itère sur tous les subscribers ML et récupère leur
activity (sending/open/click) via /subscribers/{email}/activity (API v2 Classic).

Remplace l'ancien collect_mailerlite.py côté opens/clicks (endpoint
`/campaigns/{id}/reports/opens` retiré silencieusement par MailerLite).

Usage:
    python3 collect_ml_activity.py              # itère tous les subs (resume auto)
    python3 collect_ml_activity.py --status active  # actifs seulement
    python3 collect_ml_activity.py --limit 100  # test sur 100 subs

Resume: skip les subs dont l'id est déjà présent dans ml_campaign_opens
OU ml_campaign_clicks OU dans la table d'état ml_activity_done (subs traités
qui n'ont eu aucun open/click — sinon on les retraiterait à chaque run).
"""

import argparse
import os
import sys
import time
from urllib.parse import quote

import psycopg2
import requests
from psycopg2.extras import execute_values

ML_BASE    = "https://api.mailerlite.com/api/v2"
ML_KEY     = os.environ.get("ML_KEY", "24ccb3083f6ac63a01255b540f696703")
ML_HEADERS = {"X-MailerLite-ApiKey": ML_KEY, "Content-Type": "application/json"}
DB_URL     = "postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway"
SLEEP      = 0.7
TIMEOUT    = 30

DDL = """
CREATE TABLE IF NOT EXISTS ml_activity_done (
    subscriber_id BIGINT PRIMARY KEY,
    email         TEXT,
    events_count  INTEGER,
    has_opens     BOOLEAN,
    has_clicks    BOOLEAN,
    processed_at  TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ml_activity_done_email ON ml_activity_done(email);
"""


def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


SESSION = requests.Session()
SESSION.headers.update(ML_HEADERS)


def fetch_activity(email, retries=3):
    """GET /subscribers/{email}/activity. Returns list (possibly empty) or None on hard fail."""
    url = f"{ML_BASE}/subscribers/{quote(email, safe='')}/activity"
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = int(r.headers.get('X-RateLimit-Reset-After', '60'))
                print(f'    rate-limited, sleeping {wait}s')
                time.sleep(wait + 1)
                continue
            if r.status_code == 404:
                return []  # subscriber not found / no activity
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                print(f'    ✗ {email}: {e}')
                return None
            time.sleep(2 ** attempt)
    return None


def process_subscriber(cur, sub_id, email, name):
    """Fetch activity for one subscriber, insert opens/clicks, mark done."""
    events = fetch_activity(email)
    if events is None:
        return False  # hard failure, retry next run

    opens   = {}  # campaign_id -> count
    clicks  = {}
    for ev in events:
        t = ev.get('type')
        cid = ev.get('campaign_id')
        if not cid:
            continue
        if t == 'open':
            opens[cid] = opens.get(cid, 0) + 1
        elif t == 'click':
            clicks[cid] = clicks.get(cid, 0) + 1

    if opens:
        rows = [(cid, sub_id, email, name, n) for cid, n in opens.items()]
        execute_values(cur, """
            INSERT INTO ml_campaign_opens (campaign_id, subscriber_id, email, name, open_count)
            VALUES %s
            ON CONFLICT (campaign_id, subscriber_id)
            DO UPDATE SET open_count = EXCLUDED.open_count
        """, rows)

    if clicks:
        rows = [(cid, sub_id, email, name, n) for cid, n in clicks.items()]
        execute_values(cur, """
            INSERT INTO ml_campaign_clicks (campaign_id, subscriber_id, email, name, click_count)
            VALUES %s
            ON CONFLICT (campaign_id, subscriber_id)
            DO UPDATE SET click_count = EXCLUDED.click_count
        """, rows)

    cur.execute("""
        INSERT INTO ml_activity_done (subscriber_id, email, events_count, has_opens, has_clicks)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (subscriber_id) DO UPDATE SET
            events_count = EXCLUDED.events_count,
            has_opens    = EXCLUDED.has_opens,
            has_clicks   = EXCLUDED.has_clicks,
            processed_at = NOW()
    """, (sub_id, email, len(events), bool(opens), bool(clicks)))
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--status', default=None,
                    help='filter ml_subscribers.status (active/unsubscribed/bounced). default=all')
    ap.add_argument('--limit', type=int, default=None,
                    help='process at most N subscribers (test mode)')
    ap.add_argument('--commit-every', type=int, default=200,
                    help='DB commit batch size (default 200)')
    ap.add_argument('--partition', type=int, default=None,
                    help='partition index K when running parallel processes')
    ap.add_argument('--of', type=int, default=None,
                    help='total partitions N — script only handles subs where id %% N = K')
    args = ap.parse_args()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()

    where = "WHERE s.email IS NOT NULL"
    if args.status:
        where += f" AND s.status = '{args.status}'"
    if args.partition is not None and args.of:
        where += f" AND MOD(s.id, {args.of}) = {args.partition}"

    cur.execute(f"""
        SELECT s.id, s.email, s.name
        FROM ml_subscribers s
        LEFT JOIN ml_activity_done d ON d.subscriber_id = s.id
        {where}
          AND d.subscriber_id IS NULL
        ORDER BY s.id
        {'LIMIT ' + str(args.limit) if args.limit else ''}
    """)
    todo = cur.fetchall()
    print(f'{len(todo)} subscribers to process (status={args.status or "all"})')

    cur.execute("SELECT COUNT(*) FROM ml_activity_done")
    already = cur.fetchone()[0]
    print(f'(already done: {already})')

    t0 = time.time()
    processed = 0
    failed = 0
    opens_total = 0
    clicks_total = 0

    for i, (sid, email, name) in enumerate(todo):
        ok = process_subscriber(cur, sid, email, name or '')
        if ok:
            processed += 1
        else:
            failed += 1

        time.sleep(SLEEP)

        if (i + 1) % args.commit_every == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(todo) - (i + 1)) / rate if rate > 0 else 0
            print(f'  [{i+1}/{len(todo)}] ok={processed} failed={failed} '
                  f'rate={rate:.1f}/s eta={eta/3600:.1f}h')

    conn.commit()
    cur.execute("SELECT COUNT(*), COALESCE(SUM(open_count),0) FROM ml_campaign_opens")
    n_o, sum_o = cur.fetchone()
    cur.execute("SELECT COUNT(*), COALESCE(SUM(click_count),0) FROM ml_campaign_clicks")
    n_c, sum_c = cur.fetchone()
    print(f'\nDONE. processed={processed} failed={failed}')
    print(f'  ml_campaign_opens : {n_o} rows, {sum_o} total opens')
    print(f'  ml_campaign_clicks: {n_c} rows, {sum_c} total clicks')


if __name__ == '__main__':
    main()

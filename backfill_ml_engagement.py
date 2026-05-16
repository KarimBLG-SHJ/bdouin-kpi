"""
backfill_ml_engagement.py — récupère sent/opened/clicked/rates par subscriber
via /api/v2/subscribers (pagination batch de 1000) et UPDATE ml_subscribers.

Ces champs sont renvoyés par défaut par l'API mais n'étaient pas stockés
jusqu'ici. ~30 min pour 324k subscribers.
"""

import os
import time
import psycopg2
import requests
from psycopg2.extras import execute_values

ML_KEY  = os.environ.get("ML_KEY", "24ccb3083f6ac63a01255b540f696703")
HEADERS = {"X-MailerLite-ApiKey": ML_KEY}
DB_URL  = "postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway"
LIMIT   = 1000


def main():
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    offset      = 0
    total_seen  = 0
    total_upd   = 0
    t0          = time.time()

    while True:
        try:
            r = requests.get("https://api.mailerlite.com/api/v2/subscribers",
                             headers=HEADERS,
                             params={"limit": LIMIT, "offset": offset},
                             timeout=60)
        except Exception as e:
            print(f"  HTTP error at offset {offset}: {e} — retry in 10s")
            time.sleep(10)
            continue

        if r.status_code == 429:
            wait = int(r.headers.get("X-RateLimit-Reset-After", "60"))
            print(f"  rate-limited, sleeping {wait}s")
            time.sleep(wait + 1)
            continue

        if r.status_code != 200:
            print(f"  HTTP {r.status_code} at offset {offset}: {r.text[:200]}")
            break

        batch = r.json()
        if not batch:
            break

        rows = [(
            s["id"],
            s.get("sent"),
            s.get("opened"),
            s.get("clicked"),
            s.get("opened_rate"),
            s.get("clicked_rate"),
        ) for s in batch]

        execute_values(cur, """
            UPDATE ml_subscribers AS m
            SET sent         = v.sent,
                opened       = v.opened,
                clicked      = v.clicked,
                opened_rate  = v.opened_rate,
                clicked_rate = v.clicked_rate,
                updated_at   = NOW()
            FROM (VALUES %s) AS v(id, sent, opened, clicked, opened_rate, clicked_rate)
            WHERE m.id = v.id
        """, rows)
        conn.commit()
        total_upd += cur.rowcount
        total_seen += len(batch)

        elapsed = time.time() - t0
        rate = total_seen / elapsed if elapsed > 0 else 0
        print(f"  offset={offset:>7} batch={len(batch):>4} updated={cur.rowcount:>4} "
              f"total_upd={total_upd:>7} rate={rate:.0f}/s")

        if len(batch) < LIMIT:
            break
        offset += LIMIT

    print(f"\nDONE. seen={total_seen}, updated={total_upd}, elapsed={(time.time()-t0)/60:.1f} min")

    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE sent IS NOT NULL) AS with_sent,
               COUNT(*) FILTER (WHERE opened > 0)       AS with_opens,
               COUNT(*) FILTER (WHERE clicked > 0)      AS with_clicks,
               COALESCE(SUM(sent), 0)                   AS total_sent,
               COALESCE(SUM(opened), 0)                 AS total_opens,
               COALESCE(SUM(clicked), 0)                AS total_clicks
        FROM ml_subscribers
    """)
    r = cur.fetchone()
    print(f"  with_sent={r[0]:,} with_opens={r[1]:,} with_clicks={r[2]:,}")
    print(f"  total_sent={r[3]:,} total_opens={r[4]:,} total_clicks={r[5]:,}")

    conn.close()


if __name__ == "__main__":
    main()

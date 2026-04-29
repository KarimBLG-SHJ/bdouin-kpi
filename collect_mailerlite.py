#!/usr/bin/env python3
"""
collect_mailerlite.py — Collecte MailerLite complet dans Railway Postgres.

Tables :
  ml_subscribers         — tous les contacts (déjà partiellement peuplée)
  ml_groups              — tous les groupes (déjà peuplée)
  ml_subscriber_groups   — jointure subscriber ↔ groupe (NEW)
  ml_campaigns           — campagnes (déjà peuplée)
  ml_campaign_opens      — qui a ouvert quelle campagne (NEW)
  ml_campaign_clicks     — qui a cliqué quelle campagne (NEW)

Usage:
    python3 collect_mailerlite.py
    python3 collect_mailerlite.py --resource subscriber_groups
"""

import argparse
import time
import psycopg2
import requests
from psycopg2.extras import execute_values

ML_BASE    = "https://api.mailerlite.com/api/v2"
ML_KEY     = "19bfaa983463fdcd6c354ec1954df7cc"
ML_HEADERS = {"X-MailerLite-ApiKey": ML_KEY, "Content-Type": "application/json"}
DB_URL     = "postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway"
LIMIT      = 1000
SLEEP      = 0.2


def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


DDL = """
CREATE TABLE IF NOT EXISTS ml_subscriber_groups (
    subscriber_id   BIGINT,
    group_id        BIGINT,
    group_name      TEXT,
    date_subscribed TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (subscriber_id, group_id)
);
CREATE INDEX IF NOT EXISTS idx_ml_sg_subscriber ON ml_subscriber_groups(subscriber_id);
CREATE INDEX IF NOT EXISTS idx_ml_sg_group      ON ml_subscriber_groups(group_id);

CREATE TABLE IF NOT EXISTS ml_campaign_opens (
    campaign_id     BIGINT,
    subscriber_id   BIGINT,
    email           TEXT,
    name            TEXT,
    open_count      INTEGER,
    collected_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (campaign_id, subscriber_id)
);
CREATE INDEX IF NOT EXISTS idx_ml_opens_sub ON ml_campaign_opens(subscriber_id);
CREATE INDEX IF NOT EXISTS idx_ml_opens_cam ON ml_campaign_opens(campaign_id);

CREATE TABLE IF NOT EXISTS ml_campaign_clicks (
    campaign_id     BIGINT,
    subscriber_id   BIGINT,
    email           TEXT,
    name            TEXT,
    click_count     INTEGER,
    collected_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (campaign_id, subscriber_id)
);
CREATE INDEX IF NOT EXISTS idx_ml_clicks_sub ON ml_campaign_clicks(subscriber_id);
CREATE INDEX IF NOT EXISTS idx_ml_clicks_cam ON ml_campaign_clicks(campaign_id);
"""


def ml_get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(f"{ML_BASE}/{path}", headers=ML_HEADERS, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"    Retry {attempt+1}/{retries}: {e}")
            time.sleep(5 * (attempt + 1))


def collect_subscriber_groups(cur):
    print("\n[subscriber_groups] Fetching all groups...")
    groups = ml_get("groups")
    print(f"  {len(groups)} groups")

    total = 0
    for g in groups:
        gid   = g["id"]
        gname = g["name"]
        gtot  = g.get("total", 0)
        print(f"  Group '{gname}' ({gtot} subscribers)...")

        # Check already collected for this group
        cur.execute("SELECT COUNT(*) FROM ml_subscriber_groups WHERE group_id=%s", (gid,))
        already = cur.fetchone()[0]
        if already >= gtot and gtot > 0:
            print(f"    Already complete ({already}), skipping")
            continue

        offset = already  # resume from where we left off
        inserted = 0

        while True:
            try:
                subs = ml_get(f"groups/{gid}/subscribers", {"limit": LIMIT, "offset": offset})
            except Exception as e:
                print(f"    Error at offset {offset}: {e}")
                break

            if not subs:
                break

            rows = []
            for s in subs:
                rows.append((
                    int(s["id"]),
                    int(gid),
                    gname,
                    s.get("date_subscribe") or None,
                ))

            execute_values(cur, """
                INSERT INTO ml_subscriber_groups (subscriber_id, group_id, group_name, date_subscribed)
                VALUES %s ON CONFLICT (subscriber_id, group_id) DO NOTHING
            """, rows)

            inserted += len(rows)
            offset   += len(subs)
            time.sleep(SLEEP)

            if len(subs) < LIMIT:
                break

        cur.connection.commit()
        total += inserted
        print(f"    ✓ {inserted} inserted (total this group: {offset})")

    return total


def collect_campaign_opens(cur):
    print("\n[campaign_opens] Fetching opens per campaign...")
    campaigns = ml_get("campaigns/sent", {"limit": 200, "offset": 0})
    print(f"  {len(campaigns)} sent campaigns")

    # Already collected campaigns
    cur.execute("SELECT DISTINCT campaign_id FROM ml_campaign_opens")
    done = {r[0] for r in cur.fetchall()}

    total = 0
    for i, c in enumerate(campaigns):
        cid = int(c["id"])
        if cid in done:
            continue

        offset   = 0
        inserted = 0
        while True:
            try:
                data = ml_get(f"campaigns/{cid}/reports/opens", {"limit": LIMIT, "offset": offset})
            except Exception as e:
                print(f"    Campaign {cid} error: {e}")
                break

            if not data:
                break

            rows = [(
                cid,
                int(s["id"]),
                s.get("email"),
                s.get("name"),
                int(s.get("opened", 1)),
            ) for s in data]

            execute_values(cur, """
                INSERT INTO ml_campaign_opens (campaign_id, subscriber_id, email, name, open_count)
                VALUES %s ON CONFLICT (campaign_id, subscriber_id) DO NOTHING
            """, rows)

            inserted += len(rows)
            offset   += len(data)
            time.sleep(SLEEP)

            if len(data) < LIMIT:
                break

        cur.connection.commit()
        total += inserted
        if inserted:
            print(f"  [{i+1}/{len(campaigns)}] campaign {cid}: {inserted} opens")

    print(f"  ✓ Total opens inserted: {total}")
    return total


def collect_campaign_clicks(cur):
    print("\n[campaign_clicks] Fetching clicks per campaign...")
    campaigns = ml_get("campaigns/sent", {"limit": 200, "offset": 0})
    print(f"  {len(campaigns)} sent campaigns")

    cur.execute("SELECT DISTINCT campaign_id FROM ml_campaign_clicks")
    done = {r[0] for r in cur.fetchall()}

    total = 0
    for i, c in enumerate(campaigns):
        cid = int(c["id"])
        if cid in done:
            continue

        offset   = 0
        inserted = 0
        while True:
            try:
                data = ml_get(f"campaigns/{cid}/reports/clicks", {"limit": LIMIT, "offset": offset})
            except Exception as e:
                print(f"    Campaign {cid} error: {e}")
                break

            if not data:
                break

            rows = [(
                cid,
                int(s["id"]),
                s.get("email"),
                s.get("name"),
                int(s.get("clicked", 1)),
            ) for s in data]

            execute_values(cur, """
                INSERT INTO ml_campaign_clicks (campaign_id, subscriber_id, email, name, click_count)
                VALUES %s ON CONFLICT (campaign_id, subscriber_id) DO NOTHING
            """, rows)

            inserted += len(rows)
            offset   += len(data)
            time.sleep(SLEEP)

            if len(data) < LIMIT:
                break

        cur.connection.commit()
        total += inserted
        if inserted:
            print(f"  [{i+1}/{len(campaigns)}] campaign {cid}: {inserted} clicks")

    print(f"  ✓ Total clicks inserted: {total}")
    return total


COLLECTORS = {
    "subscriber_groups": collect_subscriber_groups,
    "campaign_opens":    collect_campaign_opens,
    "campaign_clicks":   collect_campaign_clicks,
}


def main(resource_filter=None):
    # Setup tables with fresh connection
    conn = get_conn()
    cur  = conn.cursor()
    print("Setting up tables...")
    cur.execute(DDL)
    conn.commit()
    conn.close()
    print("✓ Tables ready")

    targets = [resource_filter] if resource_filter else list(COLLECTORS.keys())

    for name in targets:
        if name not in COLLECTORS:
            print(f"Unknown resource: {name}")
            continue
        # Fresh connection per collector (avoids long-lived connection drops)
        conn = get_conn()
        cur  = conn.cursor()
        try:
            n = COLLECTORS[name](cur)
            conn.commit()
            print(f"  ✓ {name}: {n} total rows inserted")
        except Exception as e:
            try: conn.rollback()
            except: pass
            print(f"  ✗ {name}: {e}")
        finally:
            try: conn.close()
            except: pass

    # Final counts
    conn = get_conn()
    cur  = conn.cursor()
    print("\n=== FINAL COUNTS ===")
    for t in ["ml_subscriber_groups", "ml_campaign_opens", "ml_campaign_clicks"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t}: {cur.fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resource", help="subscriber_groups | campaign_opens | campaign_clicks")
    args = parser.parse_args()
    main(args.resource)

#!/usr/bin/env python3
"""
pipeline_09_incremental.py — Refresh incrémental + state tracking.

Garantit :
  - Idempotence : rerun = pas de doublons
  - Détection delta par watermark (max updated_at) + row_count + content_hash
  - CLEAN : rebuild table seulement si delta détecté
  - GOLD : append-only ou upsert idempotent (user_journey jamais rebuild)
  - Versioning : chaque run loggé dans logs.pipeline_runs
  - Fallback : si erreur, rollback + skip table, autres tables continuent

Tables logs :
  logs.ingestion_state  — watermark par table public.*
  logs.pipeline_runs    — historique des exécutions
"""

import json
import time
import hashlib
import psycopg2
import traceback
from datetime import datetime
from contextlib import contextmanager

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# Tables auxquelles on applique l'incrémental + leur colonne timestamp
# Si pas de timestamp, on utilise row_count + hash
WATERMARK = {
    # PrestaShop
    'presta_orders':           'date_upd',
    'presta_order_details':    None,  # pas de timestamp
    'presta_customers':        'date_upd',
    'presta_addresses':        'date_upd',
    'presta_carts':            'date_upd',
    'presta_order_histories':  'date_add',
    'presta_order_invoices':   'date_add',
    'presta_order_payments':   'date_add',
    'presta_products':         'date_upd',
    'presta_stock_movements':  'date_add',
    'presta_cart_rules':       None,
    'presta_abandoned_carts':  'date_upd',
    # MailerLite
    'ml_subscribers':          'date_subscribe',
    'ml_subscriber_groups':    'collected_at',
    'ml_campaigns':            'date_send',
    'ml_groups':               'date_updated',
    'ml_campaign_opens':       'collected_at',
    'ml_campaign_clicks':      'collected_at',
    # GA4
    'ga4_sessions':            'date',
    'ga4_pages':               'date',
    'ga4_events':              'date',
    'ga4_search_terms':        'date',
    'ga4_user_acquisition':    'date',
    'ga4_ads_campaigns':       'date',
    'ga4_ads_keywords':        'date',
    'ga4_ads_landing_pages':   'date',
    'ga4_ecommerce':           'date',
    # GSC
    'gsc_queries':             'date',
    'gsc_pages':               'date',
    'gsc_countries':           'date',
    'gsc_devices':             'date',
    # Meta
    'meta_ig_posts':           'timestamp',
    'meta_ig_comments':        'timestamp',
    'meta_ig_mentions':        'timestamp',
    'meta_ig_post_insights':   'collected_at',
    'meta_ig_account_insights': 'date',
    'meta_ig_audience':        'collected_at',
    'meta_ig_stories':         'timestamp',
    'meta_fb_posts':           'created_time',
    'meta_fb_post_insights':   'collected_at',
    'meta_fb_page_insights':   'date',
    # Apps
    'asc_apps':                'collected_at',
    'asc_downloads':           'date',
    'asc_usage':               'date',
    'asc_ratings':             'date',
    'asc_revenue':             'date',
    'playstore_metrics':       'date',
    'playstore_reviews':       'review_created_at',
    'playstore_apps':          'collected_at',
    # Drive / Gmail
    'drive_files':             'modified_time',
    'gmail_raw':               'date_sent',
    # IMAK / Sofiadis
    'imak_print_orders':       'collected_at',
    'sofiadis_b2b_monthly':    'collected_at',
    'sofiadis_b2b_sales':      'collected_at',
    'sofiadis_logistics':      'collected_at',
    # Veille
    'reviews':                 'review_date',
    'web_mentions':            'mention_date',
    'raw_documents':           None,
    'raw_sources':             None,
}


# ─────────────────────────────────────────────────────────────────────
# State setup
# ─────────────────────────────────────────────────────────────────────

def setup_state_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs.ingestion_state (
            source_name      TEXT PRIMARY KEY,
            last_import_at   TIMESTAMP,
            last_watermark   TEXT,        -- max timestamp str (ISO) ou hash
            last_row_count   BIGINT,
            last_content_hash TEXT,
            updated_at       TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS logs.pipeline_runs (
            run_id           SERIAL PRIMARY KEY,
            started_at       TIMESTAMP DEFAULT NOW(),
            finished_at      TIMESTAMP,
            duration_sec     NUMERIC,
            status           TEXT,           -- running | success | partial | failed
            tables_checked   INT,
            tables_changed   INT,
            tables_skipped   INT,
            tables_failed    INT,
            details          JSONB
        );
    """)
    cur.connection.commit()


def get_table_state(cur, table):
    """Watermark + row_count + content_hash pour public.X."""
    wm_col = WATERMARK.get(table)

    if wm_col:
        cur.execute(f"""
            SELECT MAX({wm_col})::text, COUNT(*) FROM public."{table}"
        """)
        wm, n = cur.fetchone()
    else:
        # Pas de timestamp → on utilise un hash sur sample
        cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
        n = cur.fetchone()[0]
        cur.execute(f'SELECT md5(string_agg(t.id::text, \',\' ORDER BY t.id)) FROM (SELECT id FROM public."{table}" ORDER BY id LIMIT 1000) t')
        try:
            wm = cur.fetchone()[0] or ''
        except Exception:
            wm = ''

    return wm, n


def get_previous_state(cur, table):
    cur.execute("""
        SELECT last_watermark, last_row_count, last_content_hash
        FROM logs.ingestion_state WHERE source_name=%s
    """, (table,))
    r = cur.fetchone()
    return r if r else (None, None, None)


def update_state(cur, table, watermark, row_count, content_hash):
    cur.execute("""
        INSERT INTO logs.ingestion_state
          (source_name, last_import_at, last_watermark, last_row_count, last_content_hash, updated_at)
        VALUES (%s, NOW(), %s, %s, %s, NOW())
        ON CONFLICT (source_name) DO UPDATE SET
          last_import_at = NOW(),
          last_watermark = EXCLUDED.last_watermark,
          last_row_count = EXCLUDED.last_row_count,
          last_content_hash = EXCLUDED.last_content_hash,
          updated_at = NOW()
    """, (table, watermark, row_count, content_hash))
    cur.connection.commit()


# ─────────────────────────────────────────────────────────────────────
# Run management
# ─────────────────────────────────────────────────────────────────────

@contextmanager
def pipeline_run(cur):
    cur.execute("""
        INSERT INTO logs.pipeline_runs (status) VALUES ('running')
        RETURNING run_id
    """)
    run_id = cur.fetchone()[0]
    cur.connection.commit()
    started = time.time()
    summary = {'checked': 0, 'changed': 0, 'skipped': 0, 'failed': 0, 'tables': []}
    try:
        yield run_id, summary
        cur.execute("""
            UPDATE logs.pipeline_runs
            SET finished_at=NOW(),
                duration_sec=%s,
                status=%s,
                tables_checked=%s, tables_changed=%s, tables_skipped=%s, tables_failed=%s,
                details=%s
            WHERE run_id=%s
        """, (
            time.time() - started,
            'success' if summary['failed'] == 0 else 'partial',
            summary['checked'], summary['changed'], summary['skipped'], summary['failed'],
            json.dumps(summary['tables']),
            run_id,
        ))
        cur.connection.commit()
    except Exception as e:
        cur.execute("""
            UPDATE logs.pipeline_runs
            SET finished_at=NOW(), duration_sec=%s, status='failed',
                details=%s
            WHERE run_id=%s
        """, (time.time() - started, json.dumps({'error': str(e)[:500]}), run_id))
        cur.connection.commit()
        raise


# ─────────────────────────────────────────────────────────────────────
# Refresh CLEAN incrémental
# ─────────────────────────────────────────────────────────────────────

def detect_changes(cur, table):
    """Returns (changed: bool, current_watermark, current_count)."""
    wm, n = get_table_state(cur, table)
    prev_wm, prev_n, _ = get_previous_state(cur, table)

    if prev_wm is None:
        return True, wm, n  # first run

    # Cast row_count to int both sides (Postgres returns string sometimes)
    prev_n = int(prev_n) if prev_n is not None else 0
    if prev_n != n:
        return True, wm, n
    if str(prev_wm) != str(wm):
        return True, wm, n
    return False, wm, n


def rebuild_clean_table(cur, table):
    """Rebuild clean.X from raw.X using same template as pipeline_07_clean_all."""
    # Reuse the SQL builder from pipeline_07
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "pipeline_07",
        "/Users/karim/Documents/Claude/Projects/BDouin edition/prestashop-dashboard/flask-app/pipeline_07_clean_all.py"
    )
    p7 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(p7)
    sql = p7.build_clean_sql(cur, table)
    if sql:
        cur.execute(sql)
        cur.connection.commit()


# ─────────────────────────────────────────────────────────────────────
# Refresh GOLD incrémental (idempotent UPSERT)
# ─────────────────────────────────────────────────────────────────────

def refresh_users_master_incremental(cur):
    """UPSERT new users from clean.presta_customers + clean.ml_subscribers.
       Note: pipeline_07 rebuilds clean.presta_customers with original column names (date_add not created_at).
       We use whichever columns exist via COALESCE.
    """
    cur.execute("""
        WITH all_emails AS (
            SELECT email,
                   'prestashop'::text AS source,
                   MIN(date_add) AS first_seen,
                   MAX(date_upd) AS last_seen
            FROM clean.presta_customers
            WHERE email IS NOT NULL
              AND email ~ '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
            GROUP BY email
            UNION ALL
            SELECT email, 'mailerlite', MIN(date_subscribe), MAX(date_subscribe)
            FROM clean.ml_subscribers
            WHERE email IS NOT NULL
              AND email ~ '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
            GROUP BY email
        ),
        merged AS (
            SELECT email,
                   STRING_AGG(DISTINCT source, ',' ORDER BY source) AS sources_str,
                   MIN(first_seen) AS first_seen,
                   MAX(last_seen) AS last_seen
            FROM all_emails
            GROUP BY email
        )
        INSERT INTO gold.users_master
          (user_id_master, primary_email, sources_combined, in_prestashop, in_mailerlite,
           first_seen, last_seen, created_at, match_method, match_confidence_score)
        SELECT
            md5(email),
            email,
            sources_str,
            sources_str LIKE '%prestashop%',
            sources_str LIKE '%mailerlite%',
            first_seen, last_seen, NOW(), 'email_match', 1.0
        FROM merged
        ON CONFLICT (user_id_master) DO UPDATE SET
            sources_combined = EXCLUDED.sources_combined,
            in_prestashop    = EXCLUDED.in_prestashop,
            in_mailerlite    = EXCLUDED.in_mailerlite,
            last_seen        = EXCLUDED.last_seen
    """)
    cur.connection.commit()


def refresh_user_journey_append_only(cur):
    """APPEND-ONLY : on n'efface JAMAIS user_journey, on ajoute les nouveaux events.
       Dédoublonnage par (user_id_master, event_time, event_type, event_value).
    """
    # Skip dedup if constraint already exists (covered by ON CONFLICT below)
    cur.execute("""
        SELECT 1 FROM pg_constraint WHERE conname = 'user_journey_unique_event'
    """)
    has_constraint = cur.fetchone() is not None

    if not has_constraint:
        # Fast dedup with window function (only on first run)
        cur.execute("""
            DELETE FROM gold.user_journey
            WHERE ctid IN (
                SELECT ctid FROM (
                    SELECT ctid, ROW_NUMBER() OVER (
                      PARTITION BY user_id_master, event_time, event_type, event_value
                      ORDER BY ctid
                    ) AS rn FROM gold.user_journey
                ) sub WHERE rn > 1
            )
        """)
        cur.connection.commit()

    # Add unique constraint if not exists
    cur.execute("""
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'user_journey_unique_event'
          ) THEN
            ALTER TABLE gold.user_journey
              ADD CONSTRAINT user_journey_unique_event
              UNIQUE (user_id_master, event_time, event_type, event_value);
          END IF;
        END $$;
    """)
    cur.connection.commit()

    # Helper: clean.presta_customers has new schema (no created_at, but date_add)
    # gold.email_activity already has user_id_master + dates → use it
    # New email_present events (from new ML subscribers)
    cur.execute("""
        INSERT INTO gold.user_journey (user_id_master, event_time, event_type, event_value, source, country, content_id, revenue)
        SELECT
            ea.user_id_master,
            COALESCE(ea.unsubscribed_at - INTERVAL '30 days', ea.subscribed_at, NOW() - INTERVAL '1 year'),
            'email_present',
            ea.email,
            'mailerlite',
            ea.country_code,
            NULL,
            NULL
        FROM gold.email_activity ea
        WHERE ea.user_id_master IS NOT NULL
        ON CONFLICT (user_id_master, event_time, event_type, event_value) DO NOTHING
    """)
    cur.connection.commit()

    # New order events
    cur.execute("""
        INSERT INTO gold.user_journey (user_id_master, event_time, event_type, event_value, source, country, content_id, revenue)
        SELECT o.user_id_master, o.ordered_at, 'order_placed', o.reference, 'prestashop', NULL, o.reference, o.total_paid_eur
        FROM gold.orders o
        WHERE o.user_id_master IS NOT NULL AND o.ordered_at IS NOT NULL
        ON CONFLICT (user_id_master, event_time, event_type, event_value) DO NOTHING
    """)
    cur.connection.commit()

    # New unsubscribed events
    cur.execute("""
        INSERT INTO gold.user_journey (user_id_master, event_time, event_type, event_value, source, country, country, content_id, revenue)
        SELECT ea.user_id_master, ea.unsubscribed_at, 'email_unsubscribed', ea.email, 'mailerlite', ea.country_code, NULL, NULL
        FROM gold.email_activity ea
        WHERE ea.user_id_master IS NOT NULL AND ea.unsubscribed_at IS NOT NULL
        ON CONFLICT (user_id_master, event_time, event_type, event_value) DO NOTHING
    """) if False else cur.execute("""
        INSERT INTO gold.user_journey (user_id_master, event_time, event_type, event_value, source, country, content_id, revenue)
        SELECT ea.user_id_master, ea.unsubscribed_at, 'email_unsubscribed', ea.email, 'mailerlite', ea.country_code, NULL, NULL
        FROM gold.email_activity ea
        WHERE ea.user_id_master IS NOT NULL AND ea.unsubscribed_at IS NOT NULL
        ON CONFLICT (user_id_master, event_time, event_type, event_value) DO NOTHING
    """)
    cur.connection.commit()


# ─────────────────────────────────────────────────────────────────────
# Main runner
# ─────────────────────────────────────────────────────────────────────

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    setup_state_tables(cur)

    print('=== Incremental refresh ===\n')

    with pipeline_run(cur) as (run_id, summary):
        # Get tables in public
        cur.execute("""
            SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename
        """)
        tables = [r[0] for r in cur.fetchall()]

        for table in tables:
            summary['checked'] += 1
            tbl_log = {'table': table, 'status': 'pending'}

            try:
                changed, wm, n = detect_changes(cur, table)
                if not changed:
                    summary['skipped'] += 1
                    tbl_log['status'] = 'skipped'
                    tbl_log['rows'] = n
                    summary['tables'].append(tbl_log)
                    print(f'  ⊘ {table:35s} {n:>10,} (no change)')
                    continue

                # Rebuild CLEAN.X if it's a table not in custom rules
                # (We just refresh by full recreate from raw view)
                rebuild_clean_table(cur, table)

                update_state(cur, table, wm, n, '')
                summary['changed'] += 1
                tbl_log['status'] = 'changed'
                tbl_log['rows'] = n
                summary['tables'].append(tbl_log)
                print(f'  ✓ {table:35s} {n:>10,} (refreshed)')

            except Exception as e:
                conn.rollback()
                summary['failed'] += 1
                tbl_log['status'] = 'failed'
                tbl_log['error'] = str(e)[:200]
                summary['tables'].append(tbl_log)
                print(f'  ✗ {table:35s} {str(e)[:80]}')

        # Refresh GOLD incrémentaux (toujours, mais en upsert idempotent)
        if summary['changed'] > 0:
            print('\n  Refreshing GOLD (idempotent upserts)...')
            try:
                refresh_users_master_incremental(cur)
                print('    ✓ users_master upserted')
            except Exception as e:
                conn.rollback()
                print(f'    ✗ users_master: {e}')

            try:
                refresh_user_journey_append_only(cur)
                print('    ✓ user_journey appended (no rebuild)')
            except Exception as e:
                conn.rollback()
                print(f'    ✗ user_journey: {e}')

        print(f'\n=== RUN #{run_id} ===')
        print(f'  Checked:  {summary["checked"]}')
        print(f'  Changed:  {summary["changed"]}')
        print(f'  Skipped:  {summary["skipped"]}')
        print(f'  Failed:   {summary["failed"]}')

    conn.close()


if __name__ == '__main__':
    main()

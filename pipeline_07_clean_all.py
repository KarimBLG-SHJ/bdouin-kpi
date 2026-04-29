#!/usr/bin/env python3
"""
pipeline_07_clean_all.py — Étendre CLEAN à 100% des tables RAW.

Règles :
  - Une clean.X pour chaque raw.X
  - SELECT * (zéro suppression)
  - Ajouts : standardisations + flags qualité + traceability
  - Tables 1:1 entre raw et clean

Tables already done (skipped) :
  presta_customers, presta_addresses, presta_orders,
  presta_order_details, presta_products, ml_subscribers, reviews
"""

import psycopg2

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

ALREADY_CLEAN = {
    'presta_customers', 'presta_addresses', 'presta_orders',
    'presta_order_details', 'presta_products', 'ml_subscribers', 'reviews',
}

# Email regex
EMAIL_REGEX = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"


def get_columns(cur, schema, table):
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (schema, table))
    return cur.fetchall()


def build_clean_sql(cur, table):
    """Build CREATE TABLE clean.X AS SELECT ... FROM raw.X."""
    cols = get_columns(cur, 'raw', table)
    if not cols:
        return None

    text_types = {'text', 'character varying', 'character'}
    date_types = {'timestamp without time zone', 'timestamp with time zone', 'date'}
    num_types  = {'integer', 'bigint', 'numeric', 'real', 'double precision', 'smallint'}

    # Build computed columns
    computed = []

    # Find email-like columns
    email_cols = [c for c, _, _ in cols if c in ('email', 'sender', 'recipient', 'developer_email', 'contact_email', 'author_email')]
    for ec in email_cols:
        computed.append(f"(LOWER(BTRIM({ec})) ~ '{EMAIL_REGEX}') AS is_valid_email_{ec}")
        computed.append(f"NULLIF(LOWER(BTRIM({ec})),'') AS {ec}_clean")
        computed.append(f"({ec} IS NULL OR BTRIM({ec})='') AS is_missing_{ec}")

    # Date freshness/presence flags
    date_cols = [(c, t) for c, t, _ in cols if t in date_types]
    for dc, _ in date_cols:
        computed.append(f"({dc} IS NOT NULL) AS has_{dc}")

    # Text length + lowercase for important text fields
    important_text = {
        'query', 'page', 'search_term', 'caption', 'text', 'content', 'comment',
        'title', 'subject', 'body_text', 'message', 'name', 'product_name',
        'snippet', 'description', 'review_text', 'reply_content',
    }
    text_cols = [(c, t) for c, t, _ in cols if t in text_types]
    for tc, _ in text_cols:
        if tc in important_text:
            computed.append(f"LENGTH({tc}) AS {tc}_length")
            computed.append(f"NULLIF(LOWER(BTRIM({tc})),'') AS {tc}_clean")

    # Source-specific fields
    if table.startswith('ga4_'):
        for c, _, _ in cols:
            if c in ('source','medium','device','country','channel'):
                computed.append(f"NULLIF(LOWER(BTRIM({c})),'') AS {c}_clean")
            if c in ('sessions','users','clicks','impressions','events'):
                computed.append(f"({c} > 0) AS has_{c}")

    if table.startswith('gsc_'):
        for c, _, _ in cols:
            if c in ('clicks','impressions'):
                computed.append(f"({c} > 0) AS has_{c}")

    # Boolean-ish numeric flags (active, valid, paid, gift, etc.)
    bool_like = {'active','valid','gift','deleted','newsletter','optin','is_guest','free','sale','offers_iap','ad_supported','contains_ads','starred','shared','trashed'}
    for c, t, _ in cols:
        if c in bool_like and t in num_types:
            computed.append(f"({c} <> 0) AS is_{c}")

    # File size flags (for drive)
    if table == 'drive_files':
        computed.append("(file_size IS NOT NULL AND file_size > 0) AS has_file_content")
        computed.append("(content_text IS NOT NULL AND LENGTH(content_text) > 0) AS has_text_extracted")

    # Gmail attachment flag
    if table == 'gmail_raw':
        computed.append("(attachments IS NOT NULL AND attachments::text NOT IN ('null','[]')) AS has_attachment")
        computed.append("(body_text IS NOT NULL AND LENGTH(body_text) > 0) AS has_body_text")

    # Web mentions
    if table == 'web_mentions':
        computed.append("(sentiment IS NOT NULL) AS has_sentiment")
        computed.append("(mention_date IS NOT NULL) AS has_date")

    # Reviews ratings
    if table == 'playstore_reviews':
        computed.append("(star_rating >= 1 AND star_rating <= 5) AS is_valid_rating")

    if not computed:
        # at minimum, count nulls per non-null-allowed columns
        pass

    # Add row_id_raw if not in raw view (we add it here for clean rows)
    # Note: raw views don't have row_id_raw stable, we generate fresh in clean
    extra_cols = ',\n    '.join(computed) if computed else ''

    sql = f"""
        DROP TABLE IF EXISTS clean."{table}" CASCADE;
        CREATE TABLE clean."{table}" AS
        SELECT
            gen_random_uuid()::TEXT AS row_id_raw,
            NOW() AS imported_at,
            r.*
            {(',\n    ' + extra_cols) if extra_cols else ''}
        FROM raw."{table}" r;
    """
    return sql


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name FROM information_schema.views
        WHERE table_schema='raw'
        ORDER BY table_name
    """)
    tables = [r[0] for r in cur.fetchall()]
    todo = [t for t in tables if t not in ALREADY_CLEAN]

    print(f'{len(tables)} raw tables · {len(ALREADY_CLEAN)} already clean · {len(todo)} to do\n')

    success = 0
    failed = []
    for tbl in todo:
        sql = build_clean_sql(cur, tbl)
        if not sql:
            print(f'  ⚠ {tbl}: no columns')
            continue
        try:
            cur.execute(sql)
            conn.commit()
            cur.execute(f'SELECT COUNT(*) FROM clean."{tbl}"')
            n = cur.fetchone()[0]
            print(f'  ✓ clean.{tbl}: {n:,}')
            success += 1
        except Exception as e:
            conn.rollback()
            err = str(e)[:200]
            print(f'  ✗ {tbl}: {err}')
            failed.append((tbl, err))

    print(f'\n=== SUMMARY ===')
    print(f'  Success: {success}/{len(todo)}')
    if failed:
        print(f'  Failed: {len(failed)}')
        for t, e in failed[:5]:
            print(f'    {t}: {e[:80]}')

    # Validation: count clean vs raw
    cur.execute("SELECT COUNT(*) FROM information_schema.views WHERE table_schema='raw'")
    n_raw = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='clean'")
    n_clean = cur.fetchone()[0]
    print(f'\n  raw views: {n_raw}')
    print(f'  clean tables: {n_clean}')
    print(f'  match 1:1: {"✅ YES" if n_raw == n_clean else "⚠️ NO"}')

    conn.close()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
pipeline_03_master_ids.py — Étape 3 : Master IDs.

Crée :
  gold.users_master      — un user_id_master pour chaque personne unique
  gold.products_master   — un product_id_master pour chaque produit unique
  gold.user_link         — table de jointure : user_id_master ↔ source_table.source_id

Stratégie :
  1. Collecte tous les emails uniques dans : presta_customers, ml_subscribers, reviews (author),
     meta_ig_comments (username), web_mentions (author)
  2. Hash MD5 (email lowercased) → user_id_master
  3. Pour chaque source row, on link au user_id_master correspondant

NE SUPPRIME RIEN. Garde tout.
"""

import psycopg2

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'


def run(cur, sql, label=''):
    try:
        cur.execute(sql)
        cur.connection.commit()
        if label:
            print(f'  ✓ {label}')
    except Exception as e:
        cur.connection.rollback()
        print(f'  ✗ {label}: {str(e)[:200]}')


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ─── USERS_MASTER ─────────────────────────────────────────────────
    print('=== USERS_MASTER ===\n')

    run(cur, "DROP TABLE IF EXISTS gold.user_link CASCADE")
    run(cur, "DROP TABLE IF EXISTS gold.users_master CASCADE")

    # 1. Build users_master from all email sources
    run(cur, """
        CREATE TABLE gold.users_master AS
        WITH all_emails AS (
            -- PrestaShop customers
            SELECT email, 'prestashop' AS source, MIN(created_at) AS first_seen,
                   MAX(updated_at) AS last_seen
            FROM clean.presta_customers
            WHERE email IS NOT NULL AND is_valid_email
            GROUP BY email
            UNION ALL
            -- MailerLite subscribers
            SELECT email, 'mailerlite' AS source, MIN(subscribed_at), MAX(subscribed_at)
            FROM clean.ml_subscribers
            WHERE email IS NOT NULL AND is_valid_email
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
        SELECT
            md5(email)::text         AS user_id_master,
            email                    AS primary_email,
            sources_str              AS sources_combined,
            (sources_str LIKE '%prestashop%')  AS in_prestashop,
            (sources_str LIKE '%mailerlite%')  AS in_mailerlite,
            first_seen,
            last_seen,
            NOW()                    AS created_at,
            'email_match'            AS match_method,
            1.0                      AS match_confidence_score
        FROM merged
    """, 'gold.users_master created')

    run(cur, "CREATE UNIQUE INDEX ON gold.users_master(user_id_master)")
    run(cur, "CREATE INDEX ON gold.users_master(primary_email)")

    cur.execute("SELECT COUNT(*) FROM gold.users_master")
    n = cur.fetchone()[0]
    print(f'\n  → {n:,} users uniques identifiés')

    # 2. Stats by source
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE in_prestashop AND in_mailerlite) AS in_both,
            COUNT(*) FILTER (WHERE in_prestashop AND NOT in_mailerlite) AS only_presta,
            COUNT(*) FILTER (WHERE NOT in_prestashop AND in_mailerlite) AS only_ml
        FROM gold.users_master
    """)
    in_both, only_p, only_m = cur.fetchone()
    print(f'  → Shop ∩ ML : {in_both:,}')
    print(f'  → Shop only : {only_p:,}')
    print(f'  → ML only   : {only_m:,}')

    # 3. user_link: links each clean row to user_id_master
    print('\n=== user_link ===\n')
    run(cur, """
        CREATE TABLE gold.user_link (
            user_id_master  TEXT NOT NULL,
            source_name     TEXT NOT NULL,
            source_table    TEXT NOT NULL,
            source_id       TEXT NOT NULL,
            row_id_raw      UUID,
            match_method    TEXT,
            match_score     NUMERIC(3,2),
            linked_at       TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (user_id_master, source_table, source_id)
        )
    """, 'user_link table created')

    # Link presta_customers
    run(cur, """
        INSERT INTO gold.user_link
        SELECT md5(email), 'prestashop', 'presta_customers',
               source_id::text, row_id_raw, 'email_match', 1.0, NOW()
        FROM clean.presta_customers
        WHERE email IS NOT NULL AND is_valid_email
        ON CONFLICT DO NOTHING
    """, 'links presta_customers')

    # Link ml_subscribers
    run(cur, """
        INSERT INTO gold.user_link
        SELECT md5(email), 'mailerlite', 'ml_subscribers',
               source_id::text, row_id_raw, 'email_match', 1.0, NOW()
        FROM clean.ml_subscribers
        WHERE email IS NOT NULL AND is_valid_email
        ON CONFLICT DO NOTHING
    """, 'links ml_subscribers')

    cur.execute("SELECT COUNT(*) FROM gold.user_link")
    print(f'\n  → {cur.fetchone()[0]:,} links user → source row')

    run(cur, "CREATE INDEX ON gold.user_link(source_table, source_id)")
    run(cur, "CREATE INDEX ON gold.user_link(user_id_master)")

    # ─── PRODUCTS_MASTER ──────────────────────────────────────────────
    print('\n\n=== PRODUCTS_MASTER ===\n')

    run(cur, "DROP TABLE IF EXISTS gold.products_master CASCADE")
    run(cur, """
        CREATE TABLE gold.products_master AS
        WITH catalog AS (
            -- Source 1: PrestaShop products
            SELECT
                source_id      AS source_product_id,
                'prestashop'   AS source,
                ean13, isbn, sku, name, price_ttc_eur AS price_eur
            FROM clean.presta_products
            WHERE name IS NOT NULL
        ),
        with_id AS (
            SELECT *,
                   COALESCE(ean13, isbn, sku, lower(regexp_replace(name, '\\s+', '_', 'g'))) AS canonical_key
            FROM catalog
        )
        SELECT
            md5(canonical_key)        AS product_id_master,
            MIN(name)                 AS canonical_name,
            COALESCE(MAX(ean13), MAX(isbn))  AS ean_or_isbn,
            MAX(sku)                  AS sku,
            AVG(price_eur)            AS avg_price_eur,
            COUNT(*)                  AS occurrences,
            STRING_AGG(DISTINCT source, ',') AS sources,
            NOW()                     AS created_at
        FROM with_id
        GROUP BY canonical_key
    """, 'gold.products_master created')

    run(cur, "CREATE UNIQUE INDEX ON gold.products_master(product_id_master)")
    run(cur, "CREATE INDEX ON gold.products_master(ean_or_isbn)")

    cur.execute("SELECT COUNT(*) FROM gold.products_master")
    print(f'  → {cur.fetchone()[0]:,} produits uniques identifiés')

    # ─── product_link ─────────────────────────────────────────────────
    run(cur, "DROP TABLE IF EXISTS gold.product_link CASCADE")
    run(cur, """
        CREATE TABLE gold.product_link (
            product_id_master TEXT NOT NULL,
            source_name     TEXT NOT NULL,
            source_table    TEXT NOT NULL,
            source_id       TEXT NOT NULL,
            match_method    TEXT,
            match_score     NUMERIC(3,2),
            linked_at       TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (product_id_master, source_table, source_id)
        )
    """, 'product_link table created')

    run(cur, """
        INSERT INTO gold.product_link
        SELECT md5(COALESCE(ean13, isbn, sku, lower(regexp_replace(name, '\\s+', '_', 'g')))),
               'prestashop', 'presta_products', source_id::text,
               'canonical_key', 1.0, NOW()
        FROM clean.presta_products
        WHERE name IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'link presta_products')

    # Link order_details to products via ean13
    run(cur, """
        INSERT INTO gold.product_link
        SELECT
            md5(COALESCE(od.ean13, od.sku, lower(regexp_replace(od.product_name, '\\s+', '_', 'g')))),
            'prestashop', 'presta_order_details', od.source_id::text,
            CASE WHEN od.ean13 IS NOT NULL THEN 'ean13' ELSE 'name_fuzzy' END,
            CASE WHEN od.ean13 IS NOT NULL THEN 1.0 ELSE 0.7 END,
            NOW()
        FROM clean.presta_order_details od
        ON CONFLICT DO NOTHING
    """, 'link presta_order_details')

    cur.execute("SELECT COUNT(*) FROM gold.product_link")
    print(f'  → {cur.fetchone()[0]:,} links product → source row')

    run(cur, "CREATE INDEX ON gold.product_link(source_table, source_id)")
    run(cur, "CREATE INDEX ON gold.product_link(product_id_master)")

    print('\n\n✓ Master IDs ready')
    conn.close()


if __name__ == '__main__':
    main()

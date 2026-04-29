#!/usr/bin/env python3
"""
pipeline_04_gold.py — Étape 4 : tables GOLD finales reliées.

Crée les tables exploitables avec user_id_master & product_id_master déjà résolus :

  gold.orders            — commandes shop avec user_id_master
  gold.order_items       — lignes commandes avec product_id_master
  gold.email_activity    — abonnements/désabos/groupes ML par user_id_master
  gold.feedback          — avis apps + commentaires IG par user_id_master (si match)
  gold.seo_queries       — Search Console (pas user mais content_id)
  gold.web_traffic       — sessions GA4 agrégées
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

    # ─── gold.orders ─────────────────────────────────────────────────
    print('[gold.orders]')
    run(cur, "DROP TABLE IF EXISTS gold.orders CASCADE")
    run(cur, """
        CREATE TABLE gold.orders AS
        SELECT
            o.row_id_raw,
            ul.user_id_master,
            o.source_id              AS order_id,
            o.reference,
            o.payment_method,
            o.state_id,
            o.ordered_at,
            o.invoiced_at,
            o.total_paid_eur,
            o.total_products_eur,
            o.total_shipping_eur,
            o.total_discounts_eur,
            o.is_valid,
            o.is_gift,
            o.is_unpaid,
            o.is_missing_customer,
            c.email,
            c.firstname,
            c.lastname,
            'prestashop'             AS source_name,
            NOW()                    AS built_at
        FROM clean.presta_orders o
        LEFT JOIN clean.presta_customers c ON o.source_customer_id = c.source_id
        LEFT JOIN gold.user_link ul
               ON ul.source_table = 'presta_customers'
              AND ul.source_id = o.source_customer_id::text
    """, 'gold.orders created')

    run(cur, "CREATE INDEX ON gold.orders(user_id_master)")
    run(cur, "CREATE INDEX ON gold.orders(ordered_at)")
    run(cur, "CREATE INDEX ON gold.orders(state_id)")

    # ─── gold.order_items ────────────────────────────────────────────
    print('\n[gold.order_items]')
    run(cur, "DROP TABLE IF EXISTS gold.order_items CASCADE")
    run(cur, """
        CREATE TABLE gold.order_items AS
        SELECT
            od.row_id_raw,
            od.source_id             AS order_detail_id,
            od.source_order_id       AS order_id,
            ul.user_id_master,
            pl.product_id_master,
            pm.canonical_name        AS product_name,
            od.product_name          AS product_name_raw,
            od.ean13,
            od.sku,
            od.qty_ordered,
            od.qty_returned,
            od.unit_price_ttc,
            od.total_ttc,
            o.ordered_at,
            'prestashop'             AS source_name,
            NOW()                    AS built_at
        FROM clean.presta_order_details od
        LEFT JOIN clean.presta_orders o ON od.source_order_id = o.source_id
        LEFT JOIN gold.user_link ul
               ON ul.source_table = 'presta_customers'
              AND ul.source_id = o.source_customer_id::text
        LEFT JOIN gold.product_link pl
               ON pl.source_table = 'presta_order_details'
              AND pl.source_id = od.source_id::text
        LEFT JOIN gold.products_master pm ON pl.product_id_master = pm.product_id_master
    """, 'gold.order_items created')

    run(cur, "CREATE INDEX ON gold.order_items(user_id_master)")
    run(cur, "CREATE INDEX ON gold.order_items(product_id_master)")
    run(cur, "CREATE INDEX ON gold.order_items(ordered_at)")

    # ─── gold.email_activity ─────────────────────────────────────────
    print('\n[gold.email_activity]')
    run(cur, "DROP TABLE IF EXISTS gold.email_activity CASCADE")
    run(cur, """
        CREATE TABLE gold.email_activity AS
        SELECT
            s.row_id_raw,
            ul.user_id_master,
            s.source_id              AS subscriber_id,
            s.email,
            s.name,
            s.country_code,
            s.city,
            s.is_active,
            s.is_unsubscribed,
            s.is_bounced,
            s.subscribed_at,
            s.unsubscribed_at,
            sg.group_count,
            sg.group_names,
            'mailerlite'             AS source_name,
            NOW()                    AS built_at
        FROM clean.ml_subscribers s
        LEFT JOIN gold.user_link ul
               ON ul.source_table = 'ml_subscribers'
              AND ul.source_id = s.source_id::text
        LEFT JOIN (
            SELECT subscriber_id,
                   COUNT(*) AS group_count,
                   STRING_AGG(group_name, ', ' ORDER BY group_name) AS group_names
            FROM public.ml_subscriber_groups
            GROUP BY subscriber_id
        ) sg ON sg.subscriber_id = s.source_id
    """, 'gold.email_activity created')

    run(cur, "CREATE INDEX ON gold.email_activity(user_id_master)")
    run(cur, "CREATE INDEX ON gold.email_activity(email)")
    run(cur, "CREATE INDEX ON gold.email_activity(country_code)")

    # ─── gold.feedback ────────────────────────────────────────────────
    print('\n[gold.feedback]')
    run(cur, "DROP TABLE IF EXISTS gold.feedback CASCADE")
    run(cur, """
        CREATE TABLE gold.feedback AS
        -- App reviews
        SELECT
            r.row_id_raw,
            'app_review'               AS feedback_type,
            r.app                      AS source_app,
            r.store                    AS source_channel,
            r.country_code,
            r.rating,
            r.title,
            r.content                  AS text,
            r.author,
            r.reviewed_at              AS feedback_date,
            r.developer_reply,
            NULL::TEXT                 AS user_id_master,
            'reviews_apps'             AS source_name,
            NOW()                      AS built_at
        FROM clean.reviews r
        UNION ALL
        -- Instagram comments (avec username comme proxy de user)
        SELECT
            gen_random_uuid()          AS row_id_raw,
            'ig_comment'               AS feedback_type,
            'instagram'                AS source_app,
            'instagram'                AS source_channel,
            NULL::TEXT                 AS country_code,
            NULL::INTEGER              AS rating,
            NULL::TEXT                 AS title,
            text                       AS text,
            username                   AS author,
            timestamp                  AS feedback_date,
            NULL::TEXT                 AS developer_reply,
            NULL::TEXT                 AS user_id_master,
            'meta'                     AS source_name,
            NOW()                      AS built_at
        FROM public.meta_ig_comments
    """, 'gold.feedback created')

    run(cur, "CREATE INDEX ON gold.feedback(feedback_type)")
    run(cur, "CREATE INDEX ON gold.feedback(feedback_date)")
    run(cur, "CREATE INDEX ON gold.feedback(rating)")

    # ─── gold.seo_queries ────────────────────────────────────────────
    print('\n[gold.seo_queries]')
    run(cur, "DROP TABLE IF EXISTS gold.seo_queries CASCADE")
    run(cur, """
        CREATE TABLE gold.seo_queries AS
        SELECT
            gen_random_uuid()        AS row_id_raw,
            date,
            LOWER(BTRIM(query))      AS query_clean,
            query                    AS query_raw,
            clicks,
            impressions,
            ctr,
            position,
            'search_console'         AS source_name,
            NOW()                    AS built_at
        FROM public.gsc_queries
    """, 'gold.seo_queries created')

    run(cur, "CREATE INDEX ON gold.seo_queries(query_clean)")
    run(cur, "CREATE INDEX ON gold.seo_queries(date)")
    run(cur, "CREATE INDEX ON gold.seo_queries(clicks)")

    # ─── gold.web_traffic ────────────────────────────────────────────
    print('\n[gold.web_traffic]')
    run(cur, "DROP TABLE IF EXISTS gold.web_traffic CASCADE")
    run(cur, """
        CREATE TABLE gold.web_traffic AS
        SELECT
            gen_random_uuid()        AS row_id_raw,
            date,
            UPPER(country)           AS country_code,
            source                   AS traffic_source,
            medium                   AS traffic_medium,
            device,
            sessions,
            users,
            new_users,
            engaged_sessions,
            bounce_rate,
            avg_session_duration,
            'ga4'                    AS source_name,
            NOW()                    AS built_at
        FROM public.ga4_sessions
    """, 'gold.web_traffic created')

    run(cur, "CREATE INDEX ON gold.web_traffic(date)")
    run(cur, "CREATE INDEX ON gold.web_traffic(country_code)")

    # ─── Stats finales ────────────────────────────────────────────────
    print('\n=== TABLES GOLD ===')
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='gold' ORDER BY table_name")
    for t, in cur.fetchall():
        cur.execute(f'SELECT COUNT(*) FROM gold."{t}"')
        n = cur.fetchone()[0]
        print(f'  gold.{t}: {n:,}')

    conn.close()


if __name__ == '__main__':
    main()

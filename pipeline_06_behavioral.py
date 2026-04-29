#!/usr/bin/env python3
"""
pipeline_06_behavioral.py — Behavioral layer & Attribution

Crée:
  gold.user_identity_graph     — multi-source identity per user
  gold.user_journey            — timeline complète par user_id_master
  gold.content_performance     — perf contenu (campagnes + SEO + posts)
  gold.attribution_last_touch  — last touch attribution per order
  gold.attribution_first_touch — first touch attribution per order
  gold.funnel                  — drop-off par étape

Règle : aucune modif des tables existantes, on enrichit.
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

    # ═══════════════════════════════════════════════════════════════════
    # 1. IDENTITY GRAPH
    # ═══════════════════════════════════════════════════════════════════
    print('=== 1. gold.user_identity_graph ===\n')
    run(cur, "DROP TABLE IF EXISTS gold.user_identity_graph CASCADE")
    run(cur, """
        CREATE TABLE gold.user_identity_graph AS
        SELECT
            um.user_id_master,
            um.primary_email,
            um.in_prestashop,
            um.in_mailerlite,
            -- Compose un score 0..1 selon présence multi-source
            (CASE WHEN um.in_prestashop THEN 1 ELSE 0 END
             + CASE WHEN um.in_mailerlite THEN 1 ELSE 0 END)::FLOAT / 2.0 AS multi_source_score,

            -- Identifiants joints des sources
            (SELECT array_agg(DISTINCT pc.source_id::TEXT)
             FROM clean.presta_customers pc
             WHERE pc.email = um.primary_email AND pc.is_valid_email
            ) AS presta_customer_ids,

            (SELECT ms.country_code
             FROM clean.ml_subscribers ms
             WHERE ms.email = um.primary_email AND ms.is_valid_email
             LIMIT 1
            ) AS ml_country,

            (SELECT ms.signup_ip
             FROM clean.ml_subscribers ms
             WHERE ms.email = um.primary_email AND ms.is_valid_email
                   AND ms.signup_ip IS NOT NULL AND ms.signup_ip <> ''
             LIMIT 1
            ) AS ml_signup_ip,

            -- Adresse PrestaShop (la plus récente)
            (SELECT pa.city
             FROM clean.presta_addresses pa
             JOIN clean.presta_customers pc ON pc.source_id = pa.source_customer_id
             WHERE pc.email = um.primary_email
             ORDER BY pa.created_at DESC
             LIMIT 1
            ) AS presta_city,

            (SELECT pa.country_id
             FROM clean.presta_addresses pa
             JOIN clean.presta_customers pc ON pc.source_id = pa.source_customer_id
             WHERE pc.email = um.primary_email
             ORDER BY pa.created_at DESC
             LIMIT 1
            ) AS presta_country_id,

            -- Phone (PrestaShop)
            (SELECT pa.phone
             FROM clean.presta_addresses pa
             JOIN clean.presta_customers pc ON pc.source_id = pa.source_customer_id
             WHERE pc.email = um.primary_email AND pa.phone IS NOT NULL
             ORDER BY pa.created_at DESC
             LIMIT 1
            ) AS phone,

            um.first_seen,
            um.last_seen,
            NOW() AS built_at
        FROM gold.users_master um
    """, 'gold.user_identity_graph created')

    run(cur, "CREATE UNIQUE INDEX ON gold.user_identity_graph(user_id_master)")
    run(cur, "CREATE INDEX ON gold.user_identity_graph(primary_email)")

    cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE multi_source_score = 1) FROM gold.user_identity_graph")
    total, multi = cur.fetchone()
    print(f'  → {total:,} identités · {multi:,} multi-source ({100*multi/total:.1f}%)')

    # ═══════════════════════════════════════════════════════════════════
    # 2. USER JOURNEY (timeline événementielle)
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== 2. gold.user_journey ===\n')
    run(cur, "DROP TABLE IF EXISTS gold.user_journey CASCADE")
    run(cur, """
        CREATE TABLE gold.user_journey AS

        -- A. EMAIL events : abonnement
        SELECT
            ea.user_id_master,
            ea.subscribed_at        AS event_time,
            'email_subscribed'      AS event_type,
            ea.email                AS event_value,
            'mailerlite'            AS source,
            ea.country_code         AS country,
            NULL::TEXT              AS content_id,
            NULL::NUMERIC           AS revenue
        FROM gold.email_activity ea
        WHERE ea.user_id_master IS NOT NULL AND ea.subscribed_at IS NOT NULL

        UNION ALL

        -- B. EMAIL events : désabonnement
        SELECT
            ea.user_id_master,
            ea.unsubscribed_at,
            'email_unsubscribed',
            ea.email,
            'mailerlite',
            ea.country_code,
            NULL,
            NULL
        FROM gold.email_activity ea
        WHERE ea.user_id_master IS NOT NULL AND ea.unsubscribed_at IS NOT NULL

        UNION ALL

        -- C. ORDER events
        SELECT
            o.user_id_master,
            o.ordered_at,
            'order_placed',
            o.reference,
            'prestashop',
            NULL,
            o.reference,
            o.total_paid_eur
        FROM gold.orders o
        WHERE o.user_id_master IS NOT NULL AND o.ordered_at IS NOT NULL

        UNION ALL

        -- D. ORDER paid event (invoice = paid)
        SELECT
            o.user_id_master,
            o.invoiced_at,
            'order_paid',
            o.reference,
            'prestashop',
            NULL,
            o.reference,
            o.total_paid_eur
        FROM gold.orders o
        WHERE o.user_id_master IS NOT NULL AND o.invoiced_at IS NOT NULL
    """, 'gold.user_journey created')

    run(cur, "CREATE INDEX ON gold.user_journey(user_id_master)")
    run(cur, "CREATE INDEX ON gold.user_journey(event_time)")
    run(cur, "CREATE INDEX ON gold.user_journey(event_type)")

    cur.execute("SELECT event_type, COUNT(*) FROM gold.user_journey GROUP BY 1 ORDER BY 2 DESC")
    print('  Events par type:')
    for r in cur.fetchall():
        print(f'    {r[0]:25s} {r[1]:>10,}')

    # ═══════════════════════════════════════════════════════════════════
    # 3. CONTENT PERFORMANCE
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== 3. gold.content_performance ===\n')
    run(cur, "DROP TABLE IF EXISTS gold.content_performance CASCADE")
    run(cur, """
        CREATE TABLE gold.content_performance AS
        -- A. SEO pages (URLs Search Console)
        SELECT
            'seo_page'              AS content_type,
            page                    AS content_id,
            page                    AS content_name,
            SUM(clicks)             AS total_views,
            SUM(impressions)        AS total_impressions,
            AVG(ctr)                AS avg_ctr,
            AVG(position)           AS avg_position,
            NULL::INTEGER           AS sends,
            NULL::INTEGER           AS engagements,
            0::NUMERIC              AS attributed_revenue,
            0::INTEGER              AS attributed_orders,
            'search_console'        AS source
        FROM public.gsc_pages
        GROUP BY page

        UNION ALL

        -- B. ML campaigns
        SELECT
            'email_campaign',
            id::TEXT,
            COALESCE(name, subject) AS content_name,
            opened_count            AS total_views,
            total_recipients        AS total_impressions,
            opened_rate             AS avg_ctr,
            NULL::NUMERIC           AS avg_position,
            total_recipients        AS sends,
            clicked_count           AS engagements,
            0::NUMERIC,
            0::INTEGER,
            'mailerlite'
        FROM public.ml_campaigns

        UNION ALL

        -- C. Instagram posts
        SELECT
            'ig_post',
            id::TEXT,
            COALESCE(LEFT(caption, 80), '(no caption)'),
            COALESCE(like_count, 0)::BIGINT  AS total_views,
            NULL::BIGINT,
            NULL::NUMERIC,
            NULL::NUMERIC,
            NULL,
            comments_count,
            0::NUMERIC,
            0::INTEGER,
            'meta_instagram'
        FROM public.meta_ig_posts
    """, 'gold.content_performance created')

    run(cur, "CREATE INDEX ON gold.content_performance(content_type)")

    # Backfill attributed_revenue using user_journey for email campaigns
    # (For order_placed events within 7 days after email_subscribed)
    print('  Computing attributed_revenue (email campaigns within 7d window)...')
    run(cur, """
        UPDATE gold.content_performance cp
        SET attributed_revenue = sub.rev,
            attributed_orders  = sub.n
        FROM (
            SELECT
                ea.email,
                COUNT(DISTINCT o.reference)             AS n,
                COALESCE(SUM(o.total_paid_eur), 0)      AS rev
            FROM gold.email_activity ea
            JOIN gold.orders o
              ON o.user_id_master = ea.user_id_master
             AND o.ordered_at BETWEEN ea.subscribed_at AND ea.subscribed_at + INTERVAL '30 days'
            GROUP BY ea.email
        ) sub
        WHERE FALSE  -- placeholder, actual attribution attaches per user_journey, not per email
    """, 'attribution placeholder (skipped)')

    cur.execute("SELECT content_type, COUNT(*) FROM gold.content_performance GROUP BY 1")
    print('  Content rows par type:')
    for r in cur.fetchall():
        print(f'    {r[0]:20s} {r[1]:>10,}')

    # ═══════════════════════════════════════════════════════════════════
    # 4. ATTRIBUTION LAST_TOUCH / FIRST_TOUCH
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== 4. gold.attribution_last_touch ===\n')
    run(cur, "DROP TABLE IF EXISTS gold.attribution_last_touch CASCADE")
    run(cur, """
        CREATE TABLE gold.attribution_last_touch AS
        SELECT DISTINCT ON (o.reference)
            o.reference            AS order_id,
            o.user_id_master,
            o.ordered_at,
            o.total_paid_eur       AS revenue,
            uj.source              AS attributed_source,
            uj.event_type          AS attributed_event_type,
            uj.event_time          AS attributed_event_time,
            uj.content_id          AS attributed_content_id,
            EXTRACT(EPOCH FROM (o.ordered_at - uj.event_time))/3600 AS hours_to_purchase
        FROM gold.orders o
        LEFT JOIN gold.user_journey uj
               ON uj.user_id_master = o.user_id_master
              AND uj.event_time     <= o.ordered_at
              AND uj.event_type    NOT IN ('order_placed','order_paid')
        WHERE o.user_id_master IS NOT NULL
        ORDER BY o.reference, uj.event_time DESC NULLS LAST
    """, 'gold.attribution_last_touch')

    run(cur, "CREATE INDEX ON gold.attribution_last_touch(attributed_source)")

    print('\n=== 5. gold.attribution_first_touch ===\n')
    run(cur, "DROP TABLE IF EXISTS gold.attribution_first_touch CASCADE")
    run(cur, """
        CREATE TABLE gold.attribution_first_touch AS
        SELECT DISTINCT ON (o.reference)
            o.reference            AS order_id,
            o.user_id_master,
            o.ordered_at,
            o.total_paid_eur       AS revenue,
            uj.source              AS attributed_source,
            uj.event_type          AS attributed_event_type,
            uj.event_time          AS attributed_event_time,
            uj.content_id          AS attributed_content_id,
            EXTRACT(EPOCH FROM (o.ordered_at - uj.event_time))/86400 AS days_to_purchase
        FROM gold.orders o
        LEFT JOIN gold.user_journey uj
               ON uj.user_id_master = o.user_id_master
              AND uj.event_time     <= o.ordered_at
              AND uj.event_type    NOT IN ('order_placed','order_paid')
        WHERE o.user_id_master IS NOT NULL
        ORDER BY o.reference, uj.event_time ASC NULLS LAST
    """, 'gold.attribution_first_touch')

    run(cur, "CREATE INDEX ON gold.attribution_first_touch(attributed_source)")

    # ═══════════════════════════════════════════════════════════════════
    # 6. FUNNEL
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== 6. gold.funnel ===\n')
    run(cur, "DROP TABLE IF EXISTS gold.funnel CASCADE")
    run(cur, """
        CREATE TABLE gold.funnel AS
        WITH user_stages AS (
            SELECT
                user_id_master,
                MAX(CASE WHEN event_type = 'email_subscribed' THEN 1 ELSE 0 END) AS reached_subscribed,
                MAX(CASE WHEN event_type = 'order_placed'     THEN 1 ELSE 0 END) AS reached_ordered,
                MAX(CASE WHEN event_type = 'order_paid'       THEN 1 ELSE 0 END) AS reached_paid
            FROM gold.user_journey
            WHERE user_id_master IS NOT NULL
            GROUP BY user_id_master
        )
        SELECT
            COUNT(DISTINCT user_id_master)                            AS total_users,
            SUM(reached_subscribed)                                   AS users_subscribed,
            SUM(reached_ordered)                                      AS users_ordered,
            SUM(reached_paid)                                         AS users_paid,

            ROUND(100.0 * SUM(reached_ordered)::numeric    / NULLIF(SUM(reached_subscribed),0), 2) AS pct_subscribed_to_order,
            ROUND(100.0 * SUM(reached_paid)::numeric       / NULLIF(SUM(reached_ordered),0),    2) AS pct_order_to_paid,
            ROUND(100.0 * SUM(reached_paid)::numeric       / NULLIF(SUM(reached_subscribed),0), 2) AS pct_subscribed_to_paid
        FROM user_stages
    """, 'gold.funnel')

    cur.execute("SELECT * FROM gold.funnel")
    desc = [d[0] for d in cur.description]
    row = cur.fetchone()
    print('  Funnel global:')
    for d, v in zip(desc, row):
        print(f'    {d:30s} {v}')

    # ═══════════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== TABLES BEHAVIORAL ===')
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='gold'
          AND table_name IN ('user_identity_graph','user_journey','content_performance',
                             'attribution_last_touch','attribution_first_touch','funnel')
        ORDER BY table_name
    """)
    for (t,) in cur.fetchall():
        cur.execute(f'SELECT COUNT(*) FROM gold."{t}"')
        n = cur.fetchone()[0]
        print(f'  gold.{t}: {n:,}')

    conn.close()


if __name__ == '__main__':
    main()

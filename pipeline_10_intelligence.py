#!/usr/bin/env python3
"""
pipeline_10_intelligence.py — BDouin Data Intelligence Tool

Architecture :
    GOLD → intelligence.features_* → patterns/anomalies/opportunities

Phase 1 (ce script) :
  1. Schema intelligence + features_user (RFM, multi-source)
  2. features_product (velocity, returns, cross-channel)
  3. features_query (SEO opportunity score)
  4. features_content (perf simple)
  5. patterns_detected (market basket : items achetés ensemble)
  6. anomalies (z-score sur séries temporelles GSC + orders + reviews)
  7. opportunities (rules-based scoring synthétisant le tout)

Phase 2 (à venir) : clustering ML, topic modeling, forecast.
"""

import psycopg2
from datetime import datetime

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'


def run(cur, sql, label=''):
    try:
        cur.execute(sql)
        cur.connection.commit()
        if label:
            cur.execute(f"SELECT pg_total_relation_size('{label}')") if label.startswith('intelligence.') else None
            print(f'  ✓ {label}')
    except Exception as e:
        cur.connection.rollback()
        print(f'  ✗ {label}: {str(e)[:200]}')


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # ─── Schema ──────────────────────────────────────────────────────
    print('Creating intelligence schema...')
    cur.execute("CREATE SCHEMA IF NOT EXISTS intelligence")
    conn.commit()

    # ═══════════════════════════════════════════════════════════════════
    # 1. features_user — RFM + multi-source presence + scoring
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== features_user ===')
    run(cur, "DROP TABLE IF EXISTS intelligence.features_user CASCADE")
    run(cur, """
        CREATE TABLE intelligence.features_user AS
        WITH order_stats AS (
            SELECT
                user_id_master,
                COUNT(*)                                      AS nb_orders,
                COALESCE(SUM(total_paid_eur), 0)::float      AS total_spent,
                MAX(ordered_at)                               AS last_order_at,
                MIN(ordered_at)                               AS first_order_at,
                EXTRACT(DAY FROM NOW() - MAX(ordered_at))     AS days_since_last_order,
                EXTRACT(DAY FROM MAX(ordered_at) - MIN(ordered_at)) AS lifetime_days
            FROM gold.orders
            WHERE user_id_master IS NOT NULL AND is_valid AND NOT is_unpaid
            GROUP BY user_id_master
        ),
        ml_data AS (
            SELECT
                user_id_master,
                country_code,
                city,
                group_count,
                group_names,
                is_active,
                is_unsubscribed,
                is_bounced
            FROM gold.email_activity
            WHERE user_id_master IS NOT NULL
        ),
        email_engagement AS (
            SELECT
                ul.user_id_master,
                s.sent::int          AS sent_total,
                s.opened::int        AS opens_total,
                s.clicked::int       AS clicks_total,
                s.opened_rate        AS open_rate,
                s.clicked_rate       AS click_rate
            FROM public.ml_subscribers s
            JOIN gold.user_link ul
              ON ul.source_table = 'ml_subscribers'
             AND ul.source_id    = s.id::text
        ),
        email_last_open AS (
            SELECT
                ul.user_id_master,
                MAX(c.date_send) AS last_open_at
            FROM public.ml_campaign_opens o
            JOIN gold.user_link ul
              ON ul.source_table = 'ml_subscribers'
             AND ul.source_id    = o.subscriber_id::text
            LEFT JOIN public.ml_campaigns c ON c.id = o.campaign_id
            GROUP BY ul.user_id_master
        ),
        -- Abonnés payants à l'app (groupe ML "Abonnés Payants HooPow" id=111009680)
        app_premium AS (
            SELECT DISTINCT ul.user_id_master
            FROM public.ml_subscriber_groups sg
            JOIN gold.user_link ul
              ON ul.source_table = 'ml_subscribers'
             AND ul.source_id    = sg.subscriber_id::text
            WHERE sg.group_id = 111009680
        ),
        -- Opt-in newsletter (vérité = consentement explicite côté shop si client,
        -- sinon présumé OK si inscrit direct via formulaire ML actif)
        shop_newsletter AS (
            SELECT
                ul.user_id_master,
                BOOL_OR(pc.is_newsletter) AS shop_optin
            FROM clean.presta_customers pc
            JOIN gold.user_link ul
              ON ul.source_table = 'presta_customers'
             AND ul.source_id    = pc.id::text
            GROUP BY ul.user_id_master
        )
        SELECT
            um.user_id_master,
            um.primary_email,
            um.in_prestashop,
            um.in_mailerlite,

            -- RFM
            COALESCE(os.nb_orders, 0)               AS frequency,
            COALESCE(os.total_spent, 0)             AS monetary,
            os.days_since_last_order                AS recency_days,
            os.first_order_at,
            os.last_order_at,
            os.lifetime_days,

            -- ML
            ml.country_code,
            ml.city,
            ml.group_count,
            ml.group_names,
            ml.is_active                            AS ml_active,
            ml.is_unsubscribed                      AS ml_unsubscribed,
            ml.is_bounced                           AS ml_bounced,

            -- Email engagement (Phase 1 scoring)
            -- Source: ml_subscribers.{sent,opened,clicked,opened_rate,clicked_rate}
            -- (champs renvoyés par l'API v2, backfill via backfill_ml_engagement.py)
            COALESCE(ee.opens_total, 0)             AS email_opens_total,
            COALESCE(ee.clicks_total, 0)            AS email_clicks_total,
            ee.open_rate                            AS email_open_rate,
            ee.click_rate                           AS email_click_rate,
            elo.last_open_at                        AS email_last_open_at,

            -- Phase A : flags structurants
            (ap.user_id_master IS NOT NULL)         AS is_app_premium,
            -- Opt-in NL : status ML actif ET (pas client shop OU shop a coché la case)
            CASE
                WHEN ml.is_active IS TRUE
                 AND (NOT um.in_prestashop OR sn.shop_optin IS TRUE)
                THEN TRUE
                ELSE FALSE
            END                                     AS is_newsletter_optin,

            -- Scoring R/F/M (0-5 chacun, 15=top)
            CASE
                WHEN os.days_since_last_order IS NULL THEN 0
                WHEN os.days_since_last_order <= 30 THEN 5
                WHEN os.days_since_last_order <= 90 THEN 4
                WHEN os.days_since_last_order <= 180 THEN 3
                WHEN os.days_since_last_order <= 365 THEN 2
                ELSE 1
            END AS rfm_recency,

            CASE
                WHEN COALESCE(os.nb_orders,0) = 0 THEN 0
                WHEN os.nb_orders >= 10 THEN 5
                WHEN os.nb_orders >= 5  THEN 4
                WHEN os.nb_orders >= 3  THEN 3
                WHEN os.nb_orders >= 2  THEN 2
                ELSE 1
            END AS rfm_frequency,

            CASE
                WHEN COALESCE(os.total_spent,0) = 0 THEN 0
                WHEN os.total_spent >= 500 THEN 5
                WHEN os.total_spent >= 200 THEN 4
                WHEN os.total_spent >= 100 THEN 3
                WHEN os.total_spent >= 50  THEN 2
                ELSE 1
            END AS rfm_monetary,

            -- Segment automatique
            CASE
                WHEN os.nb_orders IS NULL AND ml.is_active THEN 'fan_no_purchase'
                WHEN os.nb_orders IS NULL AND ml.is_unsubscribed THEN 'lost_subscriber'
                WHEN os.nb_orders IS NULL THEN 'inactive_subscriber'
                WHEN os.nb_orders = 1 AND os.days_since_last_order > 365 THEN 'churned_one_off'
                WHEN os.nb_orders = 1 THEN 'one_time_buyer'
                WHEN os.nb_orders >= 5 AND os.days_since_last_order <= 90 THEN 'champion'
                WHEN os.nb_orders >= 3 AND os.days_since_last_order <= 180 THEN 'loyal_customer'
                WHEN os.nb_orders >= 2 AND os.days_since_last_order <= 365 THEN 'regular'
                WHEN os.days_since_last_order > 365 THEN 'churned'
                ELSE 'occasional'
            END AS segment,

            NOW() AS computed_at
        FROM gold.users_master um
        LEFT JOIN order_stats        os  ON os.user_id_master  = um.user_id_master
        LEFT JOIN ml_data            ml  ON ml.user_id_master  = um.user_id_master
        LEFT JOIN email_engagement   ee  ON ee.user_id_master  = um.user_id_master
        LEFT JOIN email_last_open    elo ON elo.user_id_master = um.user_id_master
        LEFT JOIN app_premium        ap  ON ap.user_id_master  = um.user_id_master
        LEFT JOIN shop_newsletter    sn  ON sn.user_id_master  = um.user_id_master
    """, 'intelligence.features_user')

    run(cur, "CREATE INDEX ON intelligence.features_user(segment)")
    run(cur, "CREATE INDEX ON intelligence.features_user(country_code)")
    run(cur, "CREATE INDEX ON intelligence.features_user(rfm_recency, rfm_frequency, rfm_monetary)")
    run(cur, "CREATE INDEX ON intelligence.features_user(user_id_master)")

    # Score composite + tier (Phase C — barème : CA + opens + clicks)
    print('  Phase C: score + tier...')
    run(cur, """
        ALTER TABLE intelligence.features_user
          ADD COLUMN score INT,
          ADD COLUMN tier  TEXT
    """)
    run(cur, """
        UPDATE intelligence.features_user
        SET score = (
            CASE
                WHEN monetary > 150 THEN 30
                WHEN monetary > 80  THEN 20
                WHEN monetary > 30  THEN 10
                ELSE 0
            END
          + CASE
                WHEN email_opens_total >= 20 THEN 20
                WHEN email_opens_total >= 10 THEN 10
                ELSE 0
            END
          + CASE
                WHEN email_clicks_total >= 5 THEN 15
                WHEN email_clicks_total >= 2 THEN 8
                ELSE 0
            END
        )
    """)
    run(cur, """
        UPDATE intelligence.features_user SET tier = CASE
            WHEN score >= 50 THEN 'hardcore'
            WHEN score >= 25 THEN 'segment_2'
            WHEN score >= 10 THEN 'engaged'
            ELSE 'passive'
        END
    """)
    run(cur, "CREATE INDEX ON intelligence.features_user(tier)")
    run(cur, "CREATE INDEX ON intelligence.features_user(score)")

    cur.execute("""
        SELECT segment, COUNT(*), ROUND(AVG(monetary)::numeric,2)
        FROM intelligence.features_user GROUP BY segment ORDER BY 2 DESC
    """)
    print('\n  Segments :')
    for s, n, avg in cur.fetchall():
        print(f'    {s:25s} {n:>8,} users  · avg {avg}€')

    cur.execute("""
        SELECT tier, COUNT(*),
               ROUND(AVG(score)::numeric, 1) AS avg_score,
               ROUND(AVG(monetary)::numeric, 1) AS avg_ca
        FROM intelligence.features_user
        GROUP BY tier
        ORDER BY avg_score DESC
    """)
    print('\n  Tiers (Phase C) :')
    for t, n, s, ca in cur.fetchall():
        print(f'    {t:12s} {n:>8,} users  · score {s}  · avg {ca}€')

    # ═══════════════════════════════════════════════════════════════════
    # 2. features_product — velocity, returns, cross-channel sales
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== features_product ===')
    run(cur, "DROP TABLE IF EXISTS intelligence.features_product CASCADE")
    run(cur, """
        CREATE TABLE intelligence.features_product AS
        WITH shop_sales AS (
            SELECT
                product_id_master,
                COUNT(DISTINCT order_id)              AS shop_orders,
                COUNT(DISTINCT user_id_master)        AS shop_unique_buyers,
                SUM(qty_ordered)                       AS shop_qty_sold,
                SUM(qty_returned)                      AS shop_qty_returned,
                SUM(total_ttc)                         AS shop_revenue,
                MIN(ordered_at)                        AS first_sale,
                MAX(ordered_at)                        AS last_sale,
                COUNT(*) FILTER (WHERE ordered_at >= NOW() - INTERVAL '90 days') AS shop_orders_90d,
                SUM(qty_ordered) FILTER (WHERE ordered_at >= NOW() - INTERVAL '90 days') AS shop_qty_90d
            FROM gold.order_items
            WHERE product_id_master IS NOT NULL
            GROUP BY product_id_master
        ),
        b2b_sales AS (
            SELECT
                LOWER(BTRIM(title)) AS title_clean,
                SUM(net_qty)         AS b2b_qty_sold,
                SUM(total_ht)        AS b2b_revenue,
                COUNT(*)             AS b2b_invoices
            FROM public.sofiadis_b2b_sales
            WHERE net_qty > 0
            GROUP BY LOWER(BTRIM(title))
        ),
        imak_costs AS (
            SELECT
                LOWER(BTRIM(title)) AS title_clean,
                SUM(qty)             AS imak_qty_printed,
                SUM(total_cost_eur)  AS imak_total_cost,
                AVG(unit_cost_eur)   AS imak_avg_unit_cost
            FROM public.imak_print_orders
            GROUP BY LOWER(BTRIM(title))
        )
        SELECT
            pm.product_id_master,
            pm.canonical_name              AS product_name,
            pm.ean_or_isbn,
            pm.sku,
            pm.avg_price_eur,

            -- Shop
            COALESCE(ss.shop_orders, 0)    AS shop_orders,
            COALESCE(ss.shop_unique_buyers, 0) AS shop_unique_buyers,
            COALESCE(ss.shop_qty_sold, 0)  AS shop_qty_sold,
            COALESCE(ss.shop_qty_returned, 0) AS shop_qty_returned,
            COALESCE(ss.shop_revenue, 0)::float AS shop_revenue,
            COALESCE(ss.shop_qty_90d, 0)   AS shop_qty_90d,
            ss.first_sale, ss.last_sale,

            -- Return rate
            CASE
                WHEN COALESCE(ss.shop_qty_sold, 0) > 0
                THEN ROUND(100.0 * ss.shop_qty_returned / ss.shop_qty_sold, 2)
                ELSE 0
            END AS return_rate_pct,

            -- B2B (matché par fuzzy name)
            COALESCE(b.b2b_qty_sold, 0)    AS b2b_qty_sold,
            COALESCE(b.b2b_revenue, 0)::float AS b2b_revenue,

            -- IMAK costs (matché par fuzzy name)
            COALESCE(i.imak_qty_printed, 0) AS imak_qty_printed,
            COALESCE(i.imak_total_cost, 0)::float AS imak_total_cost,
            COALESCE(i.imak_avg_unit_cost, 0)::float AS imak_avg_unit_cost,

            -- Velocity (qty/jour shop sur 90j)
            CASE WHEN ss.shop_qty_90d > 0
                 THEN ROUND(ss.shop_qty_90d::numeric / 90.0, 2)
                 ELSE 0 END AS velocity_per_day,

            -- Marge brute estimée (shop)
            CASE
                WHEN COALESCE(ss.shop_qty_sold,0) > 0 AND COALESCE(i.imak_avg_unit_cost,0) > 0
                THEN ROUND(((ss.shop_revenue / NULLIF(ss.shop_qty_sold,0)) - i.imak_avg_unit_cost)::numeric, 2)
                ELSE NULL
            END AS estimated_unit_margin_eur,

            NOW() AS computed_at
        FROM gold.products_master pm
        LEFT JOIN shop_sales ss ON ss.product_id_master = pm.product_id_master
        LEFT JOIN b2b_sales  b  ON LOWER(BTRIM(pm.canonical_name)) = b.title_clean
        LEFT JOIN imak_costs i  ON LOWER(BTRIM(pm.canonical_name)) = i.title_clean
    """, 'intelligence.features_product')

    run(cur, "CREATE INDEX ON intelligence.features_product(velocity_per_day DESC)")
    run(cur, "CREATE INDEX ON intelligence.features_product(shop_revenue DESC)")

    # ═══════════════════════════════════════════════════════════════════
    # 3. features_query — SEO opportunity scoring
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== features_query ===')
    run(cur, "DROP TABLE IF EXISTS intelligence.features_query CASCADE")
    run(cur, """
        CREATE TABLE intelligence.features_query AS
        SELECT
            LOWER(BTRIM(query))                 AS query_clean,
            SUM(clicks)                          AS total_clicks,
            SUM(impressions)                     AS total_impressions,
            ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions),0) * 100), 2)::float AS ctr_pct,
            ROUND(AVG(position)::numeric, 1)::float AS avg_position,
            COUNT(DISTINCT date)                 AS days_seen,
            MIN(date)                            AS first_seen,
            MAX(date)                            AS last_seen,

            -- Opportunity score : haute impression + position 5-15 + faible CTR = on peut gagner
            CASE
                WHEN AVG(position) BETWEEN 5 AND 15
                 AND SUM(impressions) > 100
                 AND (SUM(clicks)::numeric / NULLIF(SUM(impressions),0)) < 0.05
                THEN ROUND(SUM(impressions) * (15 - AVG(position))::numeric / 15, 0)
                ELSE 0
            END AS opportunity_score,

            -- Categorize
            CASE
                WHEN AVG(position) <= 3 THEN 'top_3'
                WHEN AVG(position) <= 10 THEN 'first_page'
                WHEN AVG(position) <= 20 THEN 'second_page'
                ELSE 'far'
            END AS rank_bucket,

            NOW() AS computed_at
        FROM public.gsc_queries
        WHERE date >= (CURRENT_DATE - INTERVAL '180 days')::date
        GROUP BY LOWER(BTRIM(query))
        HAVING SUM(impressions) > 0
    """, 'intelligence.features_query')

    run(cur, "CREATE INDEX ON intelligence.features_query(opportunity_score DESC)")
    run(cur, "CREATE INDEX ON intelligence.features_query(total_clicks DESC)")
    run(cur, "CREATE INDEX ON intelligence.features_query(rank_bucket)")

    cur.execute("SELECT COUNT(*) FROM intelligence.features_query WHERE opportunity_score > 0")
    print(f'  → {cur.fetchone()[0]:,} requêtes avec opportunity_score > 0')

    # ═══════════════════════════════════════════════════════════════════
    # 4. features_content
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== features_content ===')
    run(cur, "DROP TABLE IF EXISTS intelligence.features_content CASCADE")
    run(cur, """
        CREATE TABLE intelligence.features_content AS
        SELECT
            cm.content_id_master,
            cm.content_type,
            cm.content_title,
            cm.content_url,
            cm.first_seen_at,
            cm.last_seen_at,

            -- Pour les pages : SEO perf
            seo.total_clicks,
            seo.total_impressions,
            seo.avg_position,

            -- Pour les emails : opens/clicks
            email.opened_count,
            email.clicked_count,
            email.total_recipients,

            -- Pour les posts IG : engagement
            ig.like_count,
            ig.comments_count,

            NOW() AS computed_at
        FROM gold.content_master cm
        LEFT JOIN (
            SELECT bdouin_path(page) AS p,
                   SUM(clicks) total_clicks, SUM(impressions) total_impressions,
                   AVG(position) avg_position
            FROM public.gsc_pages
            WHERE date >= (CURRENT_DATE - INTERVAL '90 days')::date
            GROUP BY bdouin_path(page)
        ) seo ON cm.canonical_key = seo.p
        LEFT JOIN public.ml_campaigns email ON cm.canonical_key = 'email:' || email.id::text
        LEFT JOIN public.meta_ig_posts ig   ON cm.canonical_key = 'ig:' || ig.id
    """, 'intelligence.features_content')

    run(cur, "CREATE INDEX ON intelligence.features_content(content_type)")

    # ═══════════════════════════════════════════════════════════════════
    # 5. patterns_detected — market basket (livres achetés ensemble)
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== patterns_detected (market basket) ===')
    run(cur, "DROP TABLE IF EXISTS intelligence.patterns_detected CASCADE")
    run(cur, """
        CREATE TABLE intelligence.patterns_detected AS
        WITH order_products AS (
            SELECT DISTINCT order_id, product_name_raw
            FROM gold.order_items
            WHERE product_name_raw IS NOT NULL AND order_id IS NOT NULL
        ),
        pairs AS (
            SELECT
                LEAST(a.product_name_raw, b.product_name_raw)    AS product_a,
                GREATEST(a.product_name_raw, b.product_name_raw) AS product_b,
                COUNT(DISTINCT a.order_id)                        AS co_occurrences
            FROM order_products a
            JOIN order_products b
              ON a.order_id = b.order_id
             AND a.product_name_raw < b.product_name_raw
            GROUP BY LEAST(a.product_name_raw, b.product_name_raw),
                     GREATEST(a.product_name_raw, b.product_name_raw)
            HAVING COUNT(DISTINCT a.order_id) >= 5
        ),
        product_totals AS (
            SELECT product_name_raw, COUNT(DISTINCT order_id) AS n
            FROM order_products GROUP BY product_name_raw
        ),
        total_orders AS (
            SELECT COUNT(DISTINCT order_id)::float AS n FROM order_products
        )
        SELECT
            'market_basket'                       AS pattern_type,
            p.product_a,
            p.product_b,
            p.co_occurrences,

            -- Support : % de toutes les commandes contenant les deux
            ROUND((100.0 * p.co_occurrences / (SELECT n FROM total_orders))::numeric, 3) AS support_pct,

            -- Confidence : proba d'acheter B si on a A
            ROUND((100.0 * p.co_occurrences / pa.n)::numeric, 2) AS confidence_a_to_b,

            -- Lift : indépendance corrigée (>1 = corrélation positive)
            ROUND((p.co_occurrences::numeric / (pa.n * pb.n / (SELECT n FROM total_orders)))::numeric, 2) AS lift,

            NOW() AS computed_at
        FROM pairs p
        JOIN product_totals pa ON pa.product_name_raw = p.product_a
        JOIN product_totals pb ON pb.product_name_raw = p.product_b
        ORDER BY p.co_occurrences DESC
    """, 'intelligence.patterns_detected')

    cur.execute("SELECT COUNT(*) FROM intelligence.patterns_detected")
    print(f'  → {cur.fetchone()[0]:,} paires de livres achetés ensemble (>= 5 fois)')

    # ═══════════════════════════════════════════════════════════════════
    # 6. anomalies — z-score sur séries temporelles
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== anomalies ===')
    run(cur, "DROP TABLE IF EXISTS intelligence.anomalies CASCADE")
    run(cur, """
        CREATE TABLE intelligence.anomalies AS
        -- Anomalies de clics SEO journaliers
        WITH seo_daily AS (
            SELECT date, SUM(clicks) AS clicks
            FROM public.gsc_queries
            WHERE date >= (CURRENT_DATE - INTERVAL '180 days')::date
            GROUP BY date
        ),
        seo_stats AS (
            SELECT
                AVG(clicks) AS mean_clicks,
                STDDEV(clicks) AS std_clicks
            FROM seo_daily
        )
        SELECT
            'seo_clicks_daily' AS metric,
            sd.date::text       AS period,
            sd.clicks::float    AS observed_value,
            ss.mean_clicks::float AS expected_value,
            ROUND(((sd.clicks - ss.mean_clicks) / NULLIF(ss.std_clicks,0))::numeric, 2)::float AS z_score,
            CASE
                WHEN sd.clicks < ss.mean_clicks - 2 * ss.std_clicks THEN 'drop'
                WHEN sd.clicks > ss.mean_clicks + 2 * ss.std_clicks THEN 'spike'
                ELSE 'normal'
            END AS anomaly_type,
            NOW() AS detected_at
        FROM seo_daily sd, seo_stats ss
        WHERE ABS((sd.clicks - ss.mean_clicks) / NULLIF(ss.std_clicks,0)) > 2

        UNION ALL

        -- Anomalies de commandes journalières
        SELECT
            'orders_daily',
            DATE_TRUNC('day', ordered_at)::text,
            COUNT(*)::float,
            (SELECT AVG(daily_n) FROM (
                SELECT DATE_TRUNC('day', ordered_at) d, COUNT(*) daily_n
                FROM gold.orders
                WHERE ordered_at >= NOW() - INTERVAL '180 days'
                GROUP BY 1
            ) sub)::float,
            ROUND(((COUNT(*) - (SELECT AVG(daily_n) FROM (
                SELECT DATE_TRUNC('day', ordered_at) d, COUNT(*) daily_n
                FROM gold.orders
                WHERE ordered_at >= NOW() - INTERVAL '180 days'
                GROUP BY 1
            ) sub)) / NULLIF((SELECT STDDEV(daily_n) FROM (
                SELECT DATE_TRUNC('day', ordered_at) d, COUNT(*) daily_n
                FROM gold.orders
                WHERE ordered_at >= NOW() - INTERVAL '180 days'
                GROUP BY 1
            ) sub), 0))::numeric, 2)::float,
            CASE
                WHEN COUNT(*) > (SELECT AVG(daily_n) + 2*STDDEV(daily_n) FROM (
                    SELECT DATE_TRUNC('day', ordered_at) d, COUNT(*) daily_n
                    FROM gold.orders
                    WHERE ordered_at >= NOW() - INTERVAL '180 days'
                    GROUP BY 1) sub) THEN 'spike'
                WHEN COUNT(*) < (SELECT AVG(daily_n) - 2*STDDEV(daily_n) FROM (
                    SELECT DATE_TRUNC('day', ordered_at) d, COUNT(*) daily_n
                    FROM gold.orders
                    WHERE ordered_at >= NOW() - INTERVAL '180 days'
                    GROUP BY 1) sub) THEN 'drop'
                ELSE 'normal'
            END,
            NOW()
        FROM gold.orders
        WHERE ordered_at >= NOW() - INTERVAL '180 days'
        GROUP BY DATE_TRUNC('day', ordered_at)
        HAVING ABS((COUNT(*) - (SELECT AVG(daily_n) FROM (
            SELECT DATE_TRUNC('day', ordered_at) d, COUNT(*) daily_n
            FROM gold.orders
            WHERE ordered_at >= NOW() - INTERVAL '180 days'
            GROUP BY 1
        ) sub)) / NULLIF((SELECT STDDEV(daily_n) FROM (
            SELECT DATE_TRUNC('day', ordered_at) d, COUNT(*) daily_n
            FROM gold.orders
            WHERE ordered_at >= NOW() - INTERVAL '180 days'
            GROUP BY 1
        ) sub), 0)) > 2
    """, 'intelligence.anomalies')

    cur.execute("SELECT anomaly_type, COUNT(*) FROM intelligence.anomalies GROUP BY 1")
    for at, n in cur.fetchall():
        print(f'  → {at}: {n}')

    # ═══════════════════════════════════════════════════════════════════
    # 7. opportunities — synthèse rule-based
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== opportunities ===')
    run(cur, "DROP TABLE IF EXISTS intelligence.opportunities CASCADE")
    run(cur, """
        CREATE TABLE intelligence.opportunities AS
        (
        -- A. SEO opportunities (top 50 by score)
        SELECT
            'seo_keyword'::text     AS opportunity_type,
            query_clean             AS title,
            CONCAT('Position ', avg_position, ' — ', total_impressions, ' imprs / ', total_clicks, ' clics')::text AS description,
            opportunity_score::numeric AS score,
            jsonb_build_object(
                'query', query_clean,
                'position', avg_position,
                'impressions', total_impressions,
                'clicks', total_clicks,
                'ctr_pct', ctr_pct
            ) AS details,
            'push_seo_or_create_content'::text AS action_suggested,
            NOW() AS computed_at
        FROM intelligence.features_query
        WHERE opportunity_score > 0
        ORDER BY opportunity_score DESC
        LIMIT 50
        )
        UNION ALL
        (
        -- B. Top produits à pousser (forte velocity)
        SELECT
            'product_to_push'::text,
            product_name,
            CONCAT(velocity_per_day, ' ventes/jour · marge ', estimated_unit_margin_eur, '€')::text,
            (velocity_per_day * COALESCE(estimated_unit_margin_eur,1))::numeric * 30,
            jsonb_build_object(
                'product', product_name,
                'shop_qty_90d', shop_qty_90d,
                'velocity', velocity_per_day,
                'margin', estimated_unit_margin_eur,
                'b2b_qty', b2b_qty_sold
            ),
            'increase_marketing_pressure'::text,
            NOW()
        FROM intelligence.features_product
        WHERE velocity_per_day > 0.5
        ORDER BY velocity_per_day * COALESCE(estimated_unit_margin_eur, 1) DESC
        LIMIT 30
        )
        UNION ALL
        (
        -- C. Segments à relancer
        SELECT
            'segment_to_activate'::text,
            segment::text,
            CONCAT(COUNT(*), ' users · ', COALESCE(country_code, 'mixed pays'))::text,
            (COUNT(*) * 5)::numeric,
            jsonb_build_object(
                'segment', segment,
                'count', COUNT(*),
                'avg_groups', AVG(group_count),
                'top_country', MODE() WITHIN GROUP (ORDER BY country_code)
            ),
            'send_targeted_campaign'::text,
            NOW()
        FROM intelligence.features_user
        WHERE segment IN ('fan_no_purchase', 'churned_one_off', 'inactive_subscriber')
        GROUP BY segment, country_code
        HAVING COUNT(*) > 100
        ORDER BY COUNT(*) DESC
        LIMIT 30
        )
        UNION ALL
        (
        -- D. Cross-sell
        SELECT
            'cross_sell_bundle'::text,
            CONCAT(LEFT(product_a, 40), ' + ', LEFT(product_b, 40))::text,
            CONCAT(co_occurrences, ' achats simultanés · lift ', lift)::text,
            (co_occurrences * lift)::numeric,
            jsonb_build_object(
                'a', product_a, 'b', product_b,
                'co_occurrences', co_occurrences,
                'support', support_pct,
                'confidence', confidence_a_to_b,
                'lift', lift
            ),
            'create_bundle_offer'::text,
            NOW()
        FROM intelligence.patterns_detected
        WHERE lift > 1.5 AND co_occurrences >= 10
        ORDER BY co_occurrences * lift DESC
        LIMIT 30
        )
        UNION ALL
        (
        -- E. Anomalies récentes
        SELECT
            CONCAT('alert_', anomaly_type)::text,
            CONCAT(metric, ' anomaly on ', period)::text,
            CONCAT('Z-score: ', z_score, ' (', anomaly_type, ')')::text,
            ABS(z_score)::numeric,
            jsonb_build_object(
                'metric', metric,
                'period', period,
                'observed', observed_value,
                'expected', expected_value,
                'z', z_score
            ),
            'investigate'::text,
            NOW()
        FROM intelligence.anomalies
        WHERE detected_at >= NOW() - INTERVAL '30 days'
          AND ABS(z_score) > 2
        ORDER BY ABS(z_score) DESC
        LIMIT 20
        )
    """, 'intelligence.opportunities')

    run(cur, "CREATE INDEX ON intelligence.opportunities(opportunity_type)")
    run(cur, "CREATE INDEX ON intelligence.opportunities(score DESC)")

    cur.execute("""
        SELECT opportunity_type, COUNT(*), ROUND(AVG(score)::numeric,1)
        FROM intelligence.opportunities
        GROUP BY 1 ORDER BY 2 DESC
    """)
    print('\n  Opportunities détectées :')
    for ot, n, avg in cur.fetchall():
        print(f'    {ot:25s} {n:>5} · avg score {avg}')

    # ═══════════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== INTELLIGENCE LAYER ===')
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='intelligence' ORDER BY table_name
    """)
    for (t,) in cur.fetchall():
        cur.execute(f'SELECT COUNT(*) FROM intelligence."{t}"')
        n = cur.fetchone()[0]
        print(f'  intelligence.{t}: {n:,}')

    conn.close()


if __name__ == '__main__':
    main()

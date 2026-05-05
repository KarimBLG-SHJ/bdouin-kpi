#!/usr/bin/env python3
"""
pipeline_11_demand_radar.py — DEMAND RADAR

Détecte les sujets qui MONTENT (week-over-week growth).

Sources :
  - GSC queries (Google Search) — volume + position
  - GA4 search_terms (recherches internes shop)
  - IG comments (mots-clés cités)
  - Reviews apps (sujets demandés)

Tables créées :
  intelligence.demand_radar       — top trending par source
  intelligence.demand_themes       — agrégat par thème (livre / sujet)
  intelligence.demand_radar_alerts — alertes hebdomadaires
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
    # 1. demand_radar — week-over-week pour chaque source
    # ═══════════════════════════════════════════════════════════════════
    print('=== demand_radar (week-over-week growth) ===\n')
    run(cur, "DROP TABLE IF EXISTS intelligence.demand_radar CASCADE")

    run(cur, """
        CREATE TABLE intelligence.demand_radar AS
        WITH params AS (
            SELECT
                (CURRENT_DATE - INTERVAL '7 days')::date  AS week_start_recent,
                (CURRENT_DATE - INTERVAL '14 days')::date AS week_start_previous,
                CURRENT_DATE                                AS now_date
        ),

        -- A. GSC queries
        gsc_recent AS (
            SELECT LOWER(BTRIM(query)) AS term,
                   SUM(clicks) AS clicks_recent,
                   SUM(impressions) AS imprs_recent,
                   AVG(position) AS pos_recent
            FROM public.gsc_queries q, params p
            WHERE q.date >= p.week_start_recent AND q.date < p.now_date
            GROUP BY LOWER(BTRIM(query))
        ),
        gsc_previous AS (
            SELECT LOWER(BTRIM(query)) AS term,
                   SUM(clicks) AS clicks_prev,
                   SUM(impressions) AS imprs_prev,
                   AVG(position) AS pos_prev
            FROM public.gsc_queries q, params p
            WHERE q.date >= p.week_start_previous AND q.date < p.week_start_recent
            GROUP BY LOWER(BTRIM(query))
        ),
        gsc_radar AS (
            SELECT
                'gsc'::text                       AS source,
                COALESCE(r.term, p.term)          AS term,
                COALESCE(r.imprs_recent, 0)      AS volume_recent,
                COALESCE(p.imprs_prev, 0)         AS volume_previous,
                COALESCE(r.clicks_recent, 0)     AS clicks_recent,
                COALESCE(p.clicks_prev, 0)        AS clicks_previous,
                ROUND(COALESCE(r.pos_recent, 0)::numeric, 1) AS pos_recent,
                ROUND(COALESCE(p.pos_prev, 0)::numeric, 1)   AS pos_previous,
                CASE
                    WHEN COALESCE(p.imprs_prev,0) = 0 AND COALESCE(r.imprs_recent,0) > 0
                        THEN 999.99
                    WHEN COALESCE(p.imprs_prev,0) > 0
                        THEN ROUND(((COALESCE(r.imprs_recent,0) - p.imprs_prev) * 100.0 / p.imprs_prev)::numeric, 1)
                    ELSE 0
                END AS growth_pct,
                (COALESCE(r.imprs_recent,0) - COALESCE(p.imprs_prev,0)) AS volume_delta
            FROM gsc_recent r
            FULL OUTER JOIN gsc_previous p USING (term)
        ),

        -- B. GA4 internal search terms
        ga4_search AS (
            SELECT
                'ga4_internal'::text AS source,
                LOWER(BTRIM(search_term)) AS term,
                SUM(event_count) FILTER (WHERE date >= (SELECT week_start_recent FROM params)) AS volume_recent,
                SUM(event_count) FILTER (WHERE date >= (SELECT week_start_previous FROM params)
                                          AND date <  (SELECT week_start_recent FROM params)) AS volume_previous,
                NULL::integer AS clicks_recent,
                NULL::integer AS clicks_previous,
                NULL::numeric AS pos_recent,
                NULL::numeric AS pos_previous,
                CASE
                    WHEN COALESCE(SUM(event_count) FILTER (WHERE date >= (SELECT week_start_previous FROM params)
                                                            AND date <  (SELECT week_start_recent FROM params)),0) = 0
                         AND COALESCE(SUM(event_count) FILTER (WHERE date >= (SELECT week_start_recent FROM params)),0) > 0
                        THEN 999.99
                    WHEN COALESCE(SUM(event_count) FILTER (WHERE date >= (SELECT week_start_previous FROM params)
                                                            AND date <  (SELECT week_start_recent FROM params)),0) > 0
                        THEN ROUND((
                            (COALESCE(SUM(event_count) FILTER (WHERE date >= (SELECT week_start_recent FROM params)),0) -
                             SUM(event_count) FILTER (WHERE date >= (SELECT week_start_previous FROM params)
                                                       AND date <  (SELECT week_start_recent FROM params))) * 100.0
                            / NULLIF(SUM(event_count) FILTER (WHERE date >= (SELECT week_start_previous FROM params)
                                                               AND date <  (SELECT week_start_recent FROM params)),0)
                        )::numeric, 1)
                    ELSE 0
                END AS growth_pct,
                (COALESCE(SUM(event_count) FILTER (WHERE date >= (SELECT week_start_recent FROM params)),0) -
                 COALESCE(SUM(event_count) FILTER (WHERE date >= (SELECT week_start_previous FROM params)
                                                    AND date <  (SELECT week_start_recent FROM params)),0)) AS volume_delta
            FROM public.ga4_search_terms
            WHERE date >= (SELECT week_start_previous FROM params)
              AND search_term IS NOT NULL AND BTRIM(search_term) <> ''
            GROUP BY LOWER(BTRIM(search_term))
        )

        SELECT * FROM gsc_radar
        WHERE volume_recent > 5  -- au moins 5 imprs cette semaine pour être pertinent
        UNION ALL
        SELECT * FROM ga4_search
        WHERE COALESCE(volume_recent,0) > 1
    """, 'intelligence.demand_radar')

    run(cur, "CREATE INDEX ON intelligence.demand_radar(source)")
    run(cur, "CREATE INDEX ON intelligence.demand_radar(growth_pct DESC)")
    run(cur, "CREATE INDEX ON intelligence.demand_radar(volume_delta DESC)")

    cur.execute("SELECT source, COUNT(*) FROM intelligence.demand_radar GROUP BY 1")
    print('\n  Termes radar par source :')
    for s, n in cur.fetchall():
        print(f'    {s:15s} {n:>8,}')

    # ═══════════════════════════════════════════════════════════════════
    # 2. demand_themes — agréger par thème (livre / mot-clé business)
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== demand_themes (par thème business) ===\n')
    run(cur, "DROP TABLE IF EXISTS intelligence.demand_themes CASCADE")

    # On définit des thèmes mappant à des keywords business
    THEMES = {
        'hajj_omra':         "term ILIKE '%hajj%' OR term ILIKE '%omra%' OR term ILIKE '%umra%' OR term ILIKE '%mecque%' OR term ILIKE '%medine%'",
        'ramadan':           "term ILIKE '%ramadan%' OR term ILIKE '%jeûne%' OR term ILIKE '%iftar%' OR term ILIKE '%suhur%'",
        'awlad_school':      "term ILIKE '%awlad%' OR term ILIKE '%apprendre arabe%' OR term ILIKE '%vocabulaire arabe%' OR term ILIKE '%écriture arabe%'",
        'famille_foulane':   "term ILIKE '%foulane%' OR term ILIKE '%famille foulane%'",
        'walad_binti':       "term ILIKE '%walad%' OR term ILIKE '%binti%' OR term ILIKE '%agence règle%' OR term ILIKE '%manga%'",
        'muslim_show':       "term ILIKE '%muslim show%' OR term ILIKE '%bdouin%' OR term ILIKE '%recueil%'",
        'ablutions_salat':   "term ILIKE '%ablution%' OR term ILIKE '%salat%' OR term ILIKE '%prière%' OR term ILIKE '%wudu%'",
        'bonnes_actions':    "term ILIKE '%bonnes actions%' OR term ILIKE '%bonnes manieres%' OR term ILIKE '%hadith%' OR term ILIKE '%comportement%'",
        'super_etudiant':    "term ILIKE '%super etudiant%' OR term ILIKE '%super étudiant%' OR term ILIKE '%étude%'",
        'citadelle':         "term ILIKE '%citadelle%' OR term ILIKE '%hisn%' OR term ILIKE '%doua%'",
        'awlad_quiz':        "term ILIKE '%awlad quiz%' OR term ILIKE '%quiz islam%'",
        'hoopow':            "term ILIKE '%hoopow%'",
        'amir_harlem':       "term ILIKE '%amir%harlem%' OR term ILIKE '%amir of%'",
    }

    parts = []
    for theme, sql_filter in THEMES.items():
        parts.append(f"""
            SELECT
                '{theme}'::text                       AS theme,
                source,
                SUM(volume_recent)::int               AS volume_recent,
                SUM(volume_previous)::int             AS volume_previous,
                SUM(volume_delta)::int                AS volume_delta,
                COUNT(*)                              AS terms_count,
                CASE
                    WHEN SUM(volume_previous) = 0 AND SUM(volume_recent) > 0 THEN 999.99
                    WHEN SUM(volume_previous) > 0
                        THEN ROUND(((SUM(volume_recent) - SUM(volume_previous)) * 100.0 / SUM(volume_previous))::numeric, 1)
                    ELSE 0
                END                                   AS growth_pct
            FROM intelligence.demand_radar
            WHERE {sql_filter}
            GROUP BY source
        """)

    run(cur, f"""
        CREATE TABLE intelligence.demand_themes AS
        SELECT *, NOW() AS computed_at
        FROM ({" UNION ALL ".join(parts)}) sub
        WHERE volume_recent > 0
    """, 'intelligence.demand_themes')

    run(cur, "CREATE INDEX ON intelligence.demand_themes(theme)")
    run(cur, "CREATE INDEX ON intelligence.demand_themes(growth_pct DESC)")

    cur.execute("""
        SELECT theme, source, volume_recent, volume_previous, growth_pct
        FROM intelligence.demand_themes
        ORDER BY growth_pct DESC, volume_recent DESC
        LIMIT 20
    """)
    print('  Top thèmes en croissance (week-over-week) :')
    print(f"  {'Theme':22s} {'Source':12s} {'V.now':>7s} {'V.prev':>7s} {'Growth':>9s}")
    for r in cur.fetchall():
        print(f'    {r[0]:20s} {r[1]:12s} {r[2]:>7,} {r[3]:>7,} {r[4]:>7.1f}%')

    # ═══════════════════════════════════════════════════════════════════
    # 3. demand_radar_alerts — top 30 mouvements significatifs
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== demand_radar_alerts (top 30 mouvements) ===\n')
    run(cur, "DROP TABLE IF EXISTS intelligence.demand_radar_alerts CASCADE")
    run(cur, """
        CREATE TABLE intelligence.demand_radar_alerts AS
        (
            SELECT
                'rising_query'::text  AS alert_type,
                source,
                term,
                volume_recent,
                volume_previous,
                volume_delta,
                growth_pct,
                pos_recent,
                CONCAT('+', volume_delta, ' (', growth_pct, '% WoW) - position ', pos_recent)::text AS summary,
                NOW() AS detected_at
            FROM intelligence.demand_radar
            WHERE volume_recent >= 20
              AND growth_pct >= 50
              AND volume_delta >= 10
            ORDER BY volume_delta DESC
            LIMIT 20
        )
        UNION ALL
        (
            SELECT
                'falling_query'::text,
                source,
                term,
                volume_recent,
                volume_previous,
                volume_delta,
                growth_pct,
                pos_recent,
                CONCAT(volume_delta, ' (', growth_pct, '% WoW) - position ', pos_recent)::text,
                NOW()
            FROM intelligence.demand_radar
            WHERE volume_previous >= 50
              AND growth_pct <= -50
              AND volume_delta < 0
            ORDER BY volume_delta ASC
            LIMIT 10
        )
    """, 'intelligence.demand_radar_alerts')

    cur.execute("""
        SELECT alert_type, term, volume_recent, growth_pct, pos_recent
        FROM intelligence.demand_radar_alerts
        ORDER BY ABS(volume_delta) DESC LIMIT 15
    """)
    print('  Top mouvements de la semaine :')
    for r in cur.fetchall():
        sign = '⬆' if r[0]=='rising_query' else '⬇'
        print(f"    {sign} {r[1][:50]:50s}  {r[2]:>5} imprs  {r[3]:>+7.1f}%  pos {r[4]}")

    # ═══════════════════════════════════════════════════════════════════
    # 4. Synchroniser dans gold.opportunities (alertes radar)
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== Sync vers intelligence.opportunities ===\n')
    run(cur, """
        INSERT INTO intelligence.opportunities
          (opportunity_type, title, description, score, details, action_suggested, computed_at)
        SELECT
            CONCAT('demand_', alert_type)::text,
            term,
            summary,
            ABS(volume_delta)::numeric,
            jsonb_build_object(
                'source', source, 'term', term,
                'volume_recent', volume_recent,
                'volume_previous', volume_previous,
                'growth_pct', growth_pct,
                'position', pos_recent
            ),
            CASE alert_type
                WHEN 'rising_query'  THEN 'create_content_or_push'
                WHEN 'falling_query' THEN 'investigate_seo_drop'
            END,
            NOW()
        FROM intelligence.demand_radar_alerts
    """, 'opportunities synced')

    print('\n=== INTELLIGENCE LAYER updated ===')
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

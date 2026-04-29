#!/usr/bin/env python3
"""
pipeline_08_content_master.py — Master CONTENT_ID

Crée :
  gold.content_master  — un content_id_master pour chaque pièce de contenu
  gold.content_link    — jointure avec match_method + match_confidence
  logs.content_quality — métriques de matching

Sources unifiées :
  web_page       : GSC pages, GA4 pages, GA4 ads landing pages
  email_campaign : ML campaigns
  social_post    : Instagram posts
  document       : Drive files
  gmail_thread   : Gmail emails

Stratégie :
  - URL canonicalisée (lower, no params, no fragment, no trailing /)
  - Pour bdouin.com : path-only (uniformise GSC↔GA4)
  - md5(canonical_key) → content_id_master
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

    # ─── Helper SQL functions ────────────────────────────────────────
    print('Setting up canonical URL function...')
    cur.execute("""
        CREATE OR REPLACE FUNCTION canonical_url(url TEXT) RETURNS TEXT AS $$
            SELECT NULLIF(
                BTRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    LOWER(BTRIM(url)),
                                    '^https?://(www\\.)?', ''     -- strip protocol + www
                                ),
                                '#.*$', ''                          -- strip fragment
                            ),
                            '\\?.*$', ''                            -- strip query params
                        ),
                        '/$', ''                                    -- strip trailing slash
                    )
                ),
                ''
            )
        $$ LANGUAGE sql IMMUTABLE;

        CREATE OR REPLACE FUNCTION bdouin_path(url TEXT) RETURNS TEXT AS $$
            SELECT
                CASE
                    WHEN canonical_url(url) LIKE 'bdouin.com%'
                        THEN REGEXP_REPLACE(canonical_url(url), '^bdouin\\.com', '')
                    ELSE canonical_url(url)
                END
        $$ LANGUAGE sql IMMUTABLE;
    """)
    conn.commit()
    print('✓ Functions ready\n')

    # ─── 1. gold.content_master ──────────────────────────────────────
    print('=== gold.content_master ===\n')
    run(cur, "DROP TABLE IF EXISTS gold.content_link CASCADE")
    run(cur, "DROP TABLE IF EXISTS gold.content_master CASCADE")

    run(cur, """
        CREATE TABLE gold.content_master (
            content_id_master TEXT PRIMARY KEY,
            content_type      TEXT NOT NULL,
            content_title     TEXT,
            content_url       TEXT,
            content_url_raw   TEXT,
            canonical_key     TEXT,
            content_source    TEXT,
            first_seen_at     TIMESTAMP,
            last_seen_at      TIMESTAMP,
            created_at        TIMESTAMP DEFAULT NOW()
        )
    """, 'gold.content_master schema')

    # ─── 1.A. Web pages — union GSC + GA4 ───────────────────────────
    run(cur, """
        INSERT INTO gold.content_master
          (content_id_master, content_type, content_title, content_url, content_url_raw, canonical_key, content_source, first_seen_at, last_seen_at)
        SELECT
            md5(canonical) AS content_id_master,
            'web_page'     AS content_type,
            NULL           AS content_title,
            'bdouin.com' || canonical AS content_url,
            MAX(raw_url)   AS content_url_raw,
            canonical      AS canonical_key,
            STRING_AGG(DISTINCT src, ',' ORDER BY src) AS content_source,
            MIN(first_seen) AS first_seen_at,
            MAX(last_seen)  AS last_seen_at
        FROM (
            -- GSC pages
            SELECT bdouin_path(page) AS canonical, page AS raw_url, 'gsc' AS src,
                   MIN(date)::timestamp AS first_seen, MAX(date)::timestamp AS last_seen
            FROM public.gsc_pages
            WHERE page IS NOT NULL
            GROUP BY bdouin_path(page), page
            UNION ALL
            -- GA4 pages
            SELECT bdouin_path(page_path), page_path, 'ga4',
                   MIN(date)::timestamp, MAX(date)::timestamp
            FROM public.ga4_pages
            WHERE page_path IS NOT NULL
            GROUP BY bdouin_path(page_path), page_path
            UNION ALL
            -- GA4 Ads landing pages
            SELECT bdouin_path(landing_page), landing_page, 'ga4_ads',
                   MIN(date)::timestamp, MAX(date)::timestamp
            FROM public.ga4_ads_landing_pages
            WHERE landing_page IS NOT NULL
            GROUP BY bdouin_path(landing_page), landing_page
        ) all_urls
        WHERE canonical IS NOT NULL
        GROUP BY canonical
        ON CONFLICT DO NOTHING
    """, 'web_page entries inserted')

    cur.execute("SELECT COUNT(*) FROM gold.content_master WHERE content_type='web_page'")
    print(f'  → web_page: {cur.fetchone()[0]:,}')

    # ─── 1.B. Email campaigns — ML ───────────────────────────────────
    run(cur, """
        INSERT INTO gold.content_master
          (content_id_master, content_type, content_title, content_url, content_url_raw, canonical_key, content_source, first_seen_at, last_seen_at)
        SELECT
            md5('email:' || id::TEXT) AS content_id_master,
            'email_campaign',
            COALESCE(name, subject)  AS content_title,
            NULL                      AS content_url,
            NULL                      AS content_url_raw,
            'email:' || id::TEXT     AS canonical_key,
            'mailerlite'             AS content_source,
            date_send::timestamp     AS first_seen,
            date_send::timestamp     AS last_seen
        FROM public.ml_campaigns
        WHERE id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'email_campaign entries inserted')

    cur.execute("SELECT COUNT(*) FROM gold.content_master WHERE content_type='email_campaign'")
    print(f'  → email_campaign: {cur.fetchone()[0]:,}')

    # ─── 1.C. Social posts — Instagram ───────────────────────────────
    run(cur, """
        INSERT INTO gold.content_master
          (content_id_master, content_type, content_title, content_url, content_url_raw, canonical_key, content_source, first_seen_at, last_seen_at)
        SELECT
            md5('ig:' || id) AS content_id_master,
            'social_post',
            LEFT(caption, 200) AS content_title,
            permalink         AS content_url,
            permalink         AS content_url_raw,
            'ig:' || id      AS canonical_key,
            'instagram'      AS content_source,
            timestamp        AS first_seen,
            timestamp        AS last_seen
        FROM public.meta_ig_posts
        WHERE id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'social_post entries inserted')

    cur.execute("SELECT COUNT(*) FROM gold.content_master WHERE content_type='social_post'")
    print(f'  → social_post: {cur.fetchone()[0]:,}')

    # ─── 1.D. Documents — Drive ──────────────────────────────────────
    run(cur, """
        INSERT INTO gold.content_master
          (content_id_master, content_type, content_title, content_url, content_url_raw, canonical_key, content_source, first_seen_at, last_seen_at)
        SELECT
            md5('drive:' || id) AS content_id_master,
            'document',
            title                AS content_title,
            web_url              AS content_url,
            web_url              AS content_url_raw,
            'drive:' || id      AS canonical_key,
            'google_drive'      AS content_source,
            created_time         AS first_seen,
            modified_time        AS last_seen
        FROM public.drive_files
        WHERE id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'document entries inserted')

    cur.execute("SELECT COUNT(*) FROM gold.content_master WHERE content_type='document'")
    print(f'  → document: {cur.fetchone()[0]:,}')

    # ─── 1.E. Gmail threads ──────────────────────────────────────────
    run(cur, """
        INSERT INTO gold.content_master
          (content_id_master, content_type, content_title, content_url, content_url_raw, canonical_key, content_source, first_seen_at, last_seen_at)
        SELECT
            md5('gmail:' || message_id) AS content_id_master,
            'gmail_thread',
            subject                      AS content_title,
            NULL                         AS content_url,
            NULL                         AS content_url_raw,
            'gmail:' || message_id      AS canonical_key,
            'gmail'                      AS content_source,
            date_sent                    AS first_seen,
            date_sent                    AS last_seen
        FROM public.gmail_raw
        WHERE message_id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'gmail_thread entries inserted')

    cur.execute("SELECT COUNT(*) FROM gold.content_master WHERE content_type='gmail_thread'")
    print(f'  → gmail_thread: {cur.fetchone()[0]:,}')

    # Indexes
    run(cur, "CREATE INDEX ON gold.content_master(content_type)")
    run(cur, "CREATE INDEX ON gold.content_master(content_url)")
    run(cur, "CREATE INDEX ON gold.content_master(canonical_key)")

    # ═══════════════════════════════════════════════════════════════════
    # 2. gold.content_link
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== gold.content_link ===\n')
    run(cur, """
        CREATE TABLE gold.content_link (
            content_id_master TEXT NOT NULL,
            source_name       TEXT NOT NULL,
            source_table      TEXT NOT NULL,
            source_id         TEXT NOT NULL,
            row_id_raw        TEXT,
            match_method      TEXT,
            match_confidence  NUMERIC(3,2),
            linked_at         TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (content_id_master, source_table, source_id)
        )
    """, 'gold.content_link schema')

    # Link GSC pages
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5(bdouin_path(page)),
               'search_console', 'gsc_pages', page::TEXT, NULL,
               'url_path_match', 1.0, NOW()
        FROM (SELECT DISTINCT page FROM public.gsc_pages WHERE page IS NOT NULL) p
        ON CONFLICT DO NOTHING
    """, 'links gsc_pages')

    # Link GSC queries → page
    run(cur, """
        INSERT INTO gold.content_link
        SELECT DISTINCT
               md5(bdouin_path(p.page)),
               'search_console', 'gsc_queries',
               q.query || '|' || q.date::TEXT, NULL,
               'query_to_page_join', 0.7, NOW()
        FROM public.gsc_queries q
        JOIN public.gsc_pages p ON p.date = q.date
        WHERE q.query IS NOT NULL AND p.page IS NOT NULL
        LIMIT 100000
        ON CONFLICT DO NOTHING
    """, 'links gsc_queries (sampled)')

    # Link GA4 pages
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5(bdouin_path(page_path)),
               'ga4', 'ga4_pages', page_path::TEXT, NULL,
               'url_path_match', 1.0, NOW()
        FROM (SELECT DISTINCT page_path FROM public.ga4_pages WHERE page_path IS NOT NULL) p
        ON CONFLICT DO NOTHING
    """, 'links ga4_pages')

    # Link GA4 ads landing pages
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5(bdouin_path(landing_page)),
               'ga4', 'ga4_ads_landing_pages', landing_page::TEXT, NULL,
               'url_path_match', 1.0, NOW()
        FROM (SELECT DISTINCT landing_page FROM public.ga4_ads_landing_pages WHERE landing_page IS NOT NULL) p
        ON CONFLICT DO NOTHING
    """, 'links ga4_ads_landing_pages')

    # Link ML campaigns
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('email:' || id::TEXT),
               'mailerlite', 'ml_campaigns', id::TEXT, NULL,
               'campaign_id', 1.0, NOW()
        FROM public.ml_campaigns
        WHERE id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'links ml_campaigns')

    # Link ML opens / clicks → campaign
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('email:' || campaign_id::TEXT),
               'mailerlite', 'ml_campaign_opens',
               campaign_id::TEXT || '|' || subscriber_id::TEXT, NULL,
               'campaign_id_join', 1.0, NOW()
        FROM public.ml_campaign_opens
        ON CONFLICT DO NOTHING
    """, 'links ml_campaign_opens')

    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('email:' || campaign_id::TEXT),
               'mailerlite', 'ml_campaign_clicks',
               campaign_id::TEXT || '|' || subscriber_id::TEXT, NULL,
               'campaign_id_join', 1.0, NOW()
        FROM public.ml_campaign_clicks
        ON CONFLICT DO NOTHING
    """, 'links ml_campaign_clicks')

    # Link IG posts
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('ig:' || id),
               'instagram', 'meta_ig_posts', id, NULL,
               'post_id', 1.0, NOW()
        FROM public.meta_ig_posts
        WHERE id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'links meta_ig_posts')

    # Link IG comments → post
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('ig:' || post_id),
               'instagram', 'meta_ig_comments', id, NULL,
               'post_id_join', 1.0, NOW()
        FROM public.meta_ig_comments
        WHERE post_id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'links meta_ig_comments')

    # Link IG insights → post
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('ig:' || post_id),
               'instagram', 'meta_ig_post_insights',
               post_id || '|' || metric, NULL,
               'post_id_join', 1.0, NOW()
        FROM public.meta_ig_post_insights
        WHERE post_id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'links meta_ig_post_insights')

    # Link Drive files
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('drive:' || id),
               'google_drive', 'drive_files', id, NULL,
               'file_id', 1.0, NOW()
        FROM public.drive_files
        WHERE id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'links drive_files')

    # Link Gmail
    run(cur, """
        INSERT INTO gold.content_link
        SELECT md5('gmail:' || message_id),
               'gmail', 'gmail_raw', message_id, NULL,
               'message_id', 1.0, NOW()
        FROM public.gmail_raw
        WHERE message_id IS NOT NULL
        ON CONFLICT DO NOTHING
    """, 'links gmail_raw')

    run(cur, "CREATE INDEX ON gold.content_link(content_id_master)")
    run(cur, "CREATE INDEX ON gold.content_link(source_table, source_id)")

    # ═══════════════════════════════════════════════════════════════════
    # 3. CONTENT QUALITY METRICS
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== Quality metrics ===\n')

    # GA4 ↔ GSC overlap
    cur.execute("""
        WITH ga4 AS (
            SELECT DISTINCT bdouin_path(page_path) AS p FROM public.ga4_pages WHERE page_path IS NOT NULL
        ),
        gsc AS (
            SELECT DISTINCT bdouin_path(page) AS p FROM public.gsc_pages WHERE page IS NOT NULL
        )
        SELECT
            (SELECT COUNT(*) FROM ga4) AS ga4_unique,
            (SELECT COUNT(*) FROM gsc) AS gsc_unique,
            (SELECT COUNT(*) FROM ga4 INTER JOIN gsc USING(p)) AS overlap
    """) if False else cur.execute("""
        WITH ga4 AS (SELECT DISTINCT bdouin_path(page_path) AS p FROM public.ga4_pages WHERE page_path IS NOT NULL),
             gsc AS (SELECT DISTINCT bdouin_path(page) AS p FROM public.gsc_pages WHERE page IS NOT NULL)
        SELECT
            (SELECT COUNT(*) FROM ga4) ,
            (SELECT COUNT(*) FROM gsc) ,
            (SELECT COUNT(*) FROM ga4 a JOIN gsc g ON a.p = g.p)
    """)
    ga4_n, gsc_n, overlap = cur.fetchone()
    pct = round(100.0 * overlap / max(ga4_n, 1), 2)
    print(f'  Web pages (GA4 ↔ GSC):')
    print(f'    GA4 unique:  {ga4_n:,}')
    print(f'    GSC unique:  {gsc_n:,}')
    print(f'    Overlap:     {overlap:,}  ({pct}% des GA4)')

    # Email open/click coverage
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM public.ml_campaigns),
            (SELECT COUNT(DISTINCT campaign_id) FROM public.ml_campaign_opens),
            (SELECT COUNT(DISTINCT campaign_id) FROM public.ml_campaign_clicks)
    """)
    n_camp, n_with_open, n_with_click = cur.fetchone()
    print(f'\n  Email campaigns:')
    print(f'    Total: {n_camp}')
    print(f'    With opens tracked: {n_with_open}')
    print(f'    With clicks tracked: {n_with_click}')

    # IG posts vs comments coverage
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM public.meta_ig_posts),
            (SELECT COUNT(DISTINCT post_id) FROM public.meta_ig_comments)
    """)
    posts, posts_with_comments = cur.fetchone()
    print(f'\n  Instagram:')
    print(f'    Total posts: {posts}')
    print(f'    Posts with comments collected: {posts_with_comments}')

    # Save quality report
    cur.execute("""
        INSERT INTO logs.data_quality_report (run_at, metrics)
        VALUES (NOW() + INTERVAL '1 second', %s)
    """, (
        '{"content_id_master_quality": {' +
        f'"ga4_unique": {ga4_n}, "gsc_unique": {gsc_n}, "overlap": {overlap}, "overlap_pct_of_ga4": {pct},' +
        f'"ml_campaigns": {n_camp}, "ml_with_opens": {n_with_open}, "ml_with_clicks": {n_with_click},' +
        f'"ig_posts": {posts}, "ig_posts_with_comments": {posts_with_comments}' +
        '}}',
    ))
    conn.commit()

    # ═══════════════════════════════════════════════════════════════════
    # 4. SUMMARY
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== gold.content_master summary ===')
    cur.execute("""
        SELECT content_type, COUNT(*) FROM gold.content_master
        GROUP BY content_type ORDER BY 2 DESC
    """)
    total = 0
    for ct, n in cur.fetchall():
        print(f'  {ct:20s} {n:>10,}')
        total += n
    print(f'  {"TOTAL":20s} {total:>10,}')

    print('\n=== gold.content_link summary ===')
    cur.execute("""
        SELECT source_table, COUNT(*) FROM gold.content_link
        GROUP BY source_table ORDER BY 2 DESC
    """)
    for st, n in cur.fetchall():
        print(f'  {st:30s} {n:>10,}')

    print('\n✓ Master CONTENT_ID ready')
    conn.close()


if __name__ == '__main__':
    main()

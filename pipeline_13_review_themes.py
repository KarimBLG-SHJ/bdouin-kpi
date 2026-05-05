#!/usr/bin/env python3
"""
pipeline_13_review_themes.py — Review themes & sentiment

Extrait les sujets récurrents des 37 267 avis apps + 36k commentaires IG.

Approche :
  1. Keyword-based theming (multi-langues : FR, EN, AR translit)
  2. Sentiment polarity (rating + keyword refinement)
  3. Theme × app × time pour détecter les sujets qui montent
  4. Top issues récurrents par app

Tables créées :
  intelligence.review_themes        — theme tags par review
  intelligence.review_sentiment     — sentiment par review
  intelligence.theme_trending       — themes en hausse cette semaine
  intelligence.theme_summary        — agrégat global par app/theme
"""

import psycopg2
import json

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# Thèmes : keyword patterns (regex Postgres ILIKE)
THEMES = {
    'bug_crash':           ["crash", "plante", "ferme", "ne marche", "ne fonctionne", "doesn't work", "doesnt work", "stops working", "freezes", "fige"],
    'app_loading':         ["chargement", "ne charge", "loading", "won't load", "ne s'ouvre", "doesn't open"],
    'pricing':             ["trop cher", "trop chère", "expensive", "abonnement", "subscription", "payer", "paying", "price", "prix"],
    'content_arabic':      ["arabe", "arabic", "vocabulaire", "vocabulary", "écriture", "alphabet", "lettres"],
    'content_islam':       ["islam", "muslim", "musulman", "coran", "quran", "salat", "ramadan", "prière", "prayer", "doua", "rappel"],
    'audio_quality':       ["voix", "voice", "audio", "son", "sound", "prononciation", "pronunciation"],
    'ads_complaint':       ["publicité", "publicités", "pub ", "ads ", "annonces", "intrusive", "trop de pub"],
    'ux_praise':           ["facile", "easy", "simple", "intuitif", "intuitive", "j'aime", "love", "great", "génial", "super"],
    'ux_complaint':        ["compliqué", "complicated", "confus", "confusing", "pas clair", "unclear"],
    'kid_friendly':        ["enfant", "kids", "child", "petit", "fils", "fille", "daughter", "son"],
    'progression':         ["niveau", "level", "progresser", "progress", "difficile", "difficult", "trop dur"],
    'feature_request':     ["j'aimerais", "il manque", "ajoutez", "please add", "could add", "wish", "souhaite", "voudrais"],
    'crash_after_update':  ["depuis la mise à jour", "since update", "since the update", "après update", "depuis l'update"],
    'login_issue':         ["connexion", "login", "se connecter", "can't log", "se connecte pas", "compte"],
    'bug_general':         ["bug", "buggé", "buggy", "défaut", "défauts"],
    'recommendation':      ["je recommande", "i recommend", "à recommander", "must have", "indispensable"],
    'gratitude':           ["merci", "thank you", "thanks", "qu'allah", "may allah", "barakAllah", "barakallah"],
    'translation':         ["traduction", "translation", "translate", "anglais", "english", "français"],
}

# Sentiment keywords (forts indicateurs)
POSITIVE = ["excellent", "parfait", "super", "génial", "love", "best", "merveilleux",
            "incroyable", "j'adore", "i love", "wonderful", "amazing", "the best",
            "5 étoiles", "5 stars", "indispensable", "à recommander", "must have"]
NEGATIVE = ["nul", "horrible", "déçu", "deception", "déception", "n'aime pas",
            "don't like", "do not recommend", "ne recommande pas", "1 étoile",
            "1 star", "useless", "inutile", "pourri", "buggé", "broken"]


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ═══════════════════════════════════════════════════════════════════
    # 1. review_themes — un theme par review
    # ═══════════════════════════════════════════════════════════════════
    print('=== review_themes ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.review_themes CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.review_themes (
            review_id    TEXT,
            app          TEXT,
            store        TEXT,
            country      TEXT,
            rating       INTEGER,
            review_date  DATE,
            theme        TEXT,
            keyword_hit  TEXT,
            PRIMARY KEY (review_id, theme)
        )
    """)
    conn.commit()

    # For each theme, INSERT matched reviews (parametrized to escape quotes)
    total_themes = 0
    for theme, keywords in THEMES.items():
        # Use ANY() with array of patterns — fully parametrized
        patterns = [f"%{kw.lower()}%" for kw in keywords]
        cur.execute("""
            INSERT INTO intelligence.review_themes
              (review_id, app, store, country, rating, review_date, theme, keyword_hit)
            SELECT
                id, app, store, country, rating, review_date,
                %s,
                (SELECT kw FROM unnest(%s::text[]) kw
                 WHERE LOWER(content) LIKE kw LIMIT 1)
            FROM public.reviews
            WHERE content IS NOT NULL AND content <> ''
              AND LOWER(content) LIKE ANY(%s::text[])
            ON CONFLICT DO NOTHING
        """, (theme, patterns, patterns))
        n = cur.rowcount
        conn.commit()
        total_themes += n
        if n > 0:
            print(f'  {theme:25s} {n:>6,} reviews')
    print(f'\n  Total tags: {total_themes:,}')

    cur.execute("CREATE INDEX ON intelligence.review_themes(theme)")
    cur.execute("CREATE INDEX ON intelligence.review_themes(app, theme)")
    cur.execute("CREATE INDEX ON intelligence.review_themes(review_date)")
    conn.commit()

    # ═══════════════════════════════════════════════════════════════════
    # 2. review_sentiment
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== review_sentiment ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.review_sentiment CASCADE")

    pos_patterns = [f"%{kw.lower()}%" for kw in POSITIVE]
    neg_patterns = [f"%{kw.lower()}%" for kw in NEGATIVE]

    cur.execute("""
        CREATE TABLE intelligence.review_sentiment AS
        SELECT
            id           AS review_id,
            app, store, country, rating, review_date,
            content,
            -- Compound score : star rating + text signals
            CASE
                WHEN rating >= 4 OR LOWER(content) LIKE ANY(%s::text[]) THEN 'positive'
                WHEN rating <= 2 OR LOWER(content) LIKE ANY(%s::text[]) THEN 'negative'
                ELSE 'neutral'
            END AS sentiment,
            -- Numeric sentiment score 1=most positive, -1=most negative
            CASE
                WHEN rating = 5 THEN 1.0
                WHEN rating = 4 THEN 0.5
                WHEN rating = 3 THEN 0.0
                WHEN rating = 2 THEN -0.5
                WHEN rating = 1 THEN -1.0
                ELSE 0.0
            END AS sentiment_score,
            (LOWER(content) LIKE ANY(%s::text[]))::int AS has_positive_keyword,
            (LOWER(content) LIKE ANY(%s::text[]))::int AS has_negative_keyword,
            NOW() AS computed_at
        FROM public.reviews
        WHERE content IS NOT NULL AND content <> ''
    """, (pos_patterns, neg_patterns, pos_patterns, neg_patterns))
    conn.commit()
    cur.execute("CREATE INDEX ON intelligence.review_sentiment(sentiment)")
    cur.execute("CREATE INDEX ON intelligence.review_sentiment(app, sentiment)")
    cur.execute("CREATE INDEX ON intelligence.review_sentiment(review_date)")
    conn.commit()

    cur.execute("""
        SELECT sentiment, COUNT(*), ROUND(AVG(rating)::numeric, 2)
        FROM intelligence.review_sentiment GROUP BY sentiment ORDER BY 2 DESC
    """)
    print('  Sentiment distribution :')
    for s, n, avg in cur.fetchall():
        print(f'    {s:12s} {n:>7,}  avg rating {avg}')

    # ═══════════════════════════════════════════════════════════════════
    # 3. theme_summary — agrégat par app × theme
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== theme_summary ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.theme_summary CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.theme_summary AS
        SELECT
            rt.app,
            rt.theme,
            COUNT(*)                                  AS n_reviews,
            ROUND(AVG(rt.rating)::numeric, 2)        AS avg_rating,
            COUNT(*) FILTER (WHERE rt.rating <= 2)   AS n_negative,
            COUNT(*) FILTER (WHERE rt.rating >= 4)   AS n_positive,
            ROUND(100.0 * COUNT(*) FILTER (WHERE rt.rating <= 2)::numeric / COUNT(*), 1) AS pct_negative,
            MIN(rt.review_date)                      AS first_seen,
            MAX(rt.review_date)                      AS last_seen,
            COUNT(*) FILTER (WHERE rt.review_date >= CURRENT_DATE - INTERVAL '30 days') AS n_recent_30d,
            COUNT(*) FILTER (WHERE rt.review_date >= CURRENT_DATE - INTERVAL '7 days')  AS n_recent_7d,
            NOW() AS computed_at
        FROM intelligence.review_themes rt
        GROUP BY rt.app, rt.theme
    """)
    conn.commit()
    cur.execute("CREATE INDEX ON intelligence.theme_summary(app, theme)")
    cur.execute("CREATE INDEX ON intelligence.theme_summary(pct_negative DESC)")
    conn.commit()

    # Top issues (pct_negative haute)
    print('  Top issues (themes avec >20% reviews négatives) :')
    cur.execute("""
        SELECT app, theme, n_reviews, pct_negative, avg_rating
        FROM intelligence.theme_summary
        WHERE n_reviews >= 20 AND pct_negative >= 20
        ORDER BY pct_negative DESC, n_reviews DESC LIMIT 15
    """)
    for r in cur.fetchall():
        print(f'    {r[0][:25]:25s} {r[1]:22s} {r[2]:>5}  {r[3]:>5.1f}% neg  ★{r[4]}')

    # ═══════════════════════════════════════════════════════════════════
    # 4. theme_trending — themes en hausse semaine vs précédente
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== theme_trending (week-over-week) ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.theme_trending CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.theme_trending AS
        WITH current_week AS (
            SELECT app, theme, COUNT(*) AS n_recent
            FROM intelligence.review_themes
            WHERE review_date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY app, theme
        ),
        previous_week AS (
            SELECT app, theme, COUNT(*) AS n_previous
            FROM intelligence.review_themes
            WHERE review_date >= CURRENT_DATE - INTERVAL '14 days'
              AND review_date <  CURRENT_DATE - INTERVAL '7 days'
            GROUP BY app, theme
        )
        SELECT
            COALESCE(c.app, p.app)         AS app,
            COALESCE(c.theme, p.theme)     AS theme,
            COALESCE(c.n_recent, 0)        AS n_recent,
            COALESCE(p.n_previous, 0)      AS n_previous,
            (COALESCE(c.n_recent, 0) - COALESCE(p.n_previous, 0)) AS delta,
            CASE
                WHEN COALESCE(p.n_previous, 0) = 0 AND COALESCE(c.n_recent, 0) > 0 THEN 999.99
                WHEN COALESCE(p.n_previous, 0) > 0
                    THEN ROUND(((COALESCE(c.n_recent,0) - p.n_previous) * 100.0 / p.n_previous)::numeric, 1)
                ELSE 0
            END AS growth_pct,
            NOW() AS computed_at
        FROM current_week c
        FULL OUTER JOIN previous_week p USING (app, theme)
    """)
    conn.commit()

    cur.execute("""
        SELECT app, theme, n_recent, n_previous, growth_pct
        FROM intelligence.theme_trending
        WHERE n_recent >= 3 AND growth_pct >= 50
        ORDER BY n_recent DESC, growth_pct DESC LIMIT 10
    """)
    print('  Themes qui montent cette semaine :')
    for r in cur.fetchall():
        print(f'    ⬆ {r[0][:25]:25s} {r[1]:25s} {r[3]:>3} → {r[2]:>3}  {r[4]:>+7.1f}%')

    # ═══════════════════════════════════════════════════════════════════
    # 5. Sync vers opportunities
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== Sync vers opportunities ===\n')

    # Issues critiques : >20% negative + volume
    cur.execute("""
        INSERT INTO intelligence.opportunities
          (opportunity_type, title, description, score, details, action_suggested, computed_at)
        SELECT
            'review_issue'::text,
            CONCAT(app, ' - ', theme)::text,
            CONCAT(n_reviews, ' avis · ', pct_negative, '% négatifs · ★', avg_rating)::text,
            (n_reviews * pct_negative)::numeric,
            jsonb_build_object(
                'app', app, 'theme', theme,
                'n_reviews', n_reviews,
                'avg_rating', avg_rating,
                'pct_negative', pct_negative,
                'n_recent_30d', n_recent_30d
            ),
            'fix_or_communicate',
            NOW()
        FROM intelligence.theme_summary
        WHERE n_reviews >= 30 AND pct_negative >= 25
        ORDER BY n_reviews * pct_negative DESC
    """)
    n = cur.rowcount
    conn.commit()
    print(f'  ✓ {n} review_issue opportunities ajoutées')

    # ═══════════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== INTELLIGENCE LAYER updated ===')
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='intelligence' ORDER BY table_name
    """)
    for (t,) in cur.fetchall():
        cur.execute(f'SELECT COUNT(*) FROM intelligence."{t}"')
        n = cur.fetchone()[0]
        print(f'  intelligence.{t}: {n:,}')

    print('\n📋 Exemples de requêtes :\n')
    print("""
  -- Top issues à fix par app
  SELECT app, theme, n_reviews, pct_negative
  FROM intelligence.theme_summary
  WHERE pct_negative > 30 ORDER BY n_reviews DESC LIMIT 20;

  -- Avis négatifs récents sur un sujet précis
  SELECT review_date, app, country, content
  FROM intelligence.review_themes rt
  JOIN public.reviews r ON r.id = rt.review_id
  WHERE rt.theme = 'bug_crash'
    AND rt.rating <= 2
    AND rt.review_date >= CURRENT_DATE - INTERVAL '30 days'
  ORDER BY review_date DESC;

  -- Distribution sentiment par app
  SELECT app, sentiment, COUNT(*)
  FROM intelligence.review_sentiment
  GROUP BY app, sentiment ORDER BY app, sentiment;
""")

    conn.close()


if __name__ == '__main__':
    main()

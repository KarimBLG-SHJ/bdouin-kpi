#!/usr/bin/env python3
"""
dashboard_verbatim.py — Module Verbatim NLP (audience.verbatim_nlp).

Routes :
  GET /verbatim                       → page HTML
  GET /api/verbatim/summary           → volumes, sentiment, top thèmes par source
  GET /api/verbatim/critiques         → top critiques actionnables
  GET /api/verbatim/sample            → sample filtrable (theme, sentiment, source)
  GET /api/verbatim/sentiment_trend   → distribution sentiment dans le temps
"""

import os
import psycopg2
from flask import Blueprint, jsonify, request, send_from_directory
from psycopg2.extras import RealDictCursor

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway',
)

verbatim_bp = Blueprint('verbatim_dashboard', __name__)

ACTIONABLE_THEMES = ['probleme', 'technique_app', 'service_client', 'prix', 'livraison']


def get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


def query_all(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


def query_one(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()


@verbatim_bp.route('/verbatim')
def verbatim_page():
    return send_from_directory('static', 'verbatim_dashboard.html')


@verbatim_bp.route('/api/verbatim/summary')
def verbatim_summary():
    totals = query_one("SELECT count(*) AS total FROM audience.verbatim_nlp")

    by_source = query_all("""
        SELECT entity_type, source, count(*) AS n
        FROM audience.verbatim_nlp
        GROUP BY 1, 2 ORDER BY 3 DESC
    """)

    sentiment = query_all("""
        SELECT sentiment, count(*) AS n,
               round(avg(sentiment_score)::numeric, 2) AS avg_score
        FROM audience.verbatim_nlp
        GROUP BY 1 ORDER BY 2 DESC
    """)

    top_themes = query_all("""
        SELECT theme, count(*) AS n
        FROM (
            SELECT unnest(themes) AS theme FROM audience.verbatim_nlp
        ) t GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """)

    top_intentions = query_all("""
        SELECT intention, count(*) AS n
        FROM audience.verbatim_nlp
        GROUP BY 1 ORDER BY 2 DESC
    """)

    langs = query_all("""
        SELECT lang, count(*) AS n
        FROM audience.verbatim_nlp
        GROUP BY 1 ORDER BY 2 DESC
    """)

    return jsonify({
        'total': totals['total'],
        'by_source': [dict(r) for r in by_source],
        'sentiment': [dict(r) for r in sentiment],
        'top_themes': [dict(r) for r in top_themes],
        'intentions': [dict(r) for r in top_intentions],
        'langs': [dict(r) for r in langs],
    })


@verbatim_bp.route('/api/verbatim/critiques')
def verbatim_critiques():
    """Top critiques actionnables : sentiment negative + thèmes business."""
    limit = int(request.args.get('limit', 50))

    rows = query_all("""
        SELECT entity_type, source, text, lang, sentiment_score, themes, intention,
               processed_at
        FROM audience.verbatim_nlp
        WHERE sentiment = 'negative'
          AND themes && %s::text[]
        ORDER BY sentiment_score ASC, processed_at DESC
        LIMIT %s
    """, (ACTIONABLE_THEMES, limit))

    by_theme = query_all("""
        SELECT theme, count(*) AS n
        FROM (
            SELECT unnest(themes) AS theme
            FROM audience.verbatim_nlp
            WHERE sentiment = 'negative' AND themes && %s::text[]
        ) t
        WHERE theme = ANY(%s::text[])
        GROUP BY 1 ORDER BY 2 DESC
    """, (ACTIONABLE_THEMES, ACTIONABLE_THEMES))

    return jsonify({
        'by_theme': [dict(r) for r in by_theme],
        'items': [dict(r) for r in rows],
        'actionable_themes': ACTIONABLE_THEMES,
    })


@verbatim_bp.route('/api/verbatim/sample')
def verbatim_sample():
    theme = request.args.get('theme')
    sentiment = request.args.get('sentiment')
    source = request.args.get('source')
    entity_type = request.args.get('entity_type')
    limit = int(request.args.get('limit', 50))

    where = []
    params = []

    if theme:
        where.append("themes && %s::text[]")
        params.append([theme])
    if sentiment:
        where.append("sentiment = %s")
        params.append(sentiment)
    if source:
        where.append("source = %s")
        params.append(source)
    if entity_type:
        where.append("entity_type = %s")
        params.append(entity_type)

    where_sql = ' AND '.join(where) if where else 'TRUE'

    rows = query_all(f"""
        SELECT entity_type, source, text, lang, sentiment, sentiment_score,
               themes, intention, processed_at
        FROM audience.verbatim_nlp
        WHERE {where_sql}
        ORDER BY processed_at DESC
        LIMIT %s
    """, tuple(params + [limit]))

    return jsonify([dict(r) for r in rows])


@verbatim_bp.route('/api/verbatim/by_theme_source')
def verbatim_by_theme_source():
    """Matrix thème × source pour heatmap."""
    rows = query_all("""
        SELECT entity_type, source, theme, count(*) AS n
        FROM (
            SELECT entity_type, source, unnest(themes) AS theme
            FROM audience.verbatim_nlp
        ) t
        GROUP BY 1, 2, 3 ORDER BY 4 DESC
    """)
    return jsonify([dict(r) for r in rows])


def register(app):
    app.register_blueprint(verbatim_bp)

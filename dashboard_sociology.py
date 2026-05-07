#!/usr/bin/env python3
"""
dashboard_sociology.py — Module Sociologie / Audience BDouin.

Routes :
  GET /sociology                   → page HTML
  GET /api/sociology/kpis          → KPIs globaux audience
  GET /api/sociology/gender        → répartition genre + LTV + open rate
  GET /api/sociology/culture       → répartition culture + LTV + engagement
  GET /api/sociology/segments      → segments RFM × culture
  GET /api/sociology/geography     → pays via TLD email
  GET /api/sociology/signals       → flags spéciaux (oum, abou, likely_convert, apple…)
"""

import psycopg2
from flask import Blueprint, jsonify, send_from_directory
from psycopg2.extras import RealDictCursor

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

socio_bp = Blueprint('sociology_dashboard', __name__)


def get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


def query_one(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()


def query_all(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


@socio_bp.route('/sociology')
def sociology_page():
    return send_from_directory('static', 'sociology_dashboard.html')


@socio_bp.route('/api/sociology/kpis')
def sociology_kpis():
    row = query_one("""
        SELECT
            COUNT(*)                                        AS total_users,
            COUNT(*) FILTER (WHERE in_prestashop)           AS buyers,
            COUNT(*) FILTER (WHERE in_mailerlite)           AS subscribers,
            COUNT(*) FILTER (WHERE gender = 'F')            AS female,
            COUNT(*) FILTER (WHERE gender = 'M')            AS male,
            COUNT(*) FILTER (WHERE gender = 'U')            AS unknown_gender,
            COUNT(*) FILTER (WHERE culture = 'maghreb')     AS culture_maghreb,
            COUNT(*) FILTER (WHERE culture = 'europe')      AS culture_europe,
            COUNT(*) FILTER (WHERE culture = 'mixed')       AS culture_mixed,
            COUNT(*) FILTER (WHERE culture = 'unknown')     AS culture_unknown,
            COUNT(*) FILTER (WHERE likely_convert = TRUE)   AS likely_convert,
            COUNT(*) FILTER (WHERE is_oum)                  AS oum,
            COUNT(*) FILTER (WHERE is_abou)                 AS abou,
            COUNT(*) FILTER (WHERE is_apple)                AS apple_users,
            COUNT(*) FILTER (WHERE is_pro_email = TRUE)     AS pro_email,
            COUNT(*) FILTER (WHERE is_education = TRUE)     AS edu_email
        FROM intelligence.features_user
    """)
    return jsonify(dict(row))


@socio_bp.route('/api/sociology/gender')
def sociology_gender():
    rows = query_all("""
        SELECT
            COALESCE(gender, 'U')                       AS gender,
            COUNT(*)                                    AS users,
            COUNT(*) FILTER (WHERE in_prestashop)       AS buyers,
            AVG(monetary) FILTER (WHERE monetary > 0)::float  AS avg_ltv,
            AVG(email_open_rate) FILTER (WHERE email_open_rate > 0)::float AS avg_open_rate,
            AVG(frequency) FILTER (WHERE frequency > 0)::float AS avg_orders
        FROM intelligence.features_user
        GROUP BY 1
        ORDER BY users DESC
    """)
    return jsonify([dict(r) for r in rows])


@socio_bp.route('/api/sociology/culture')
def sociology_culture():
    rows = query_all("""
        SELECT
            COALESCE(culture, 'unknown')                AS culture,
            COUNT(*)                                    AS users,
            COUNT(*) FILTER (WHERE in_prestashop)       AS buyers,
            COUNT(*) FILTER (WHERE likely_convert)      AS likely_convert,
            AVG(monetary) FILTER (WHERE monetary > 0)::float  AS avg_ltv,
            AVG(email_open_rate) FILTER (WHERE email_open_rate > 0)::float AS avg_open_rate,
            AVG(email_click_rate) FILTER (WHERE email_click_rate > 0)::float AS avg_ctr,
            AVG(frequency) FILTER (WHERE frequency > 0)::float AS avg_orders
        FROM intelligence.features_user
        GROUP BY 1
        ORDER BY users DESC
    """)
    return jsonify([dict(r) for r in rows])


@socio_bp.route('/api/sociology/segments')
def sociology_segments():
    rows = query_all("""
        SELECT
            segment,
            COUNT(*)                                    AS users,
            COUNT(*) FILTER (WHERE culture = 'maghreb') AS maghreb,
            COUNT(*) FILTER (WHERE culture = 'europe')  AS europe,
            COUNT(*) FILTER (WHERE culture = 'mixed')   AS mixed,
            COUNT(*) FILTER (WHERE gender = 'F')        AS female,
            COUNT(*) FILTER (WHERE gender = 'M')        AS male,
            AVG(monetary) FILTER (WHERE monetary > 0)::float AS avg_ltv
        FROM intelligence.features_user
        WHERE segment IS NOT NULL
        GROUP BY segment
        ORDER BY avg_ltv DESC NULLS LAST
    """)
    return jsonify([dict(r) for r in rows])


@socio_bp.route('/api/sociology/geography')
def sociology_geography():
    rows = query_all("""
        SELECT
            email_tld_country                           AS country,
            COUNT(*)                                    AS users,
            COUNT(*) FILTER (WHERE in_prestashop)       AS buyers,
            AVG(monetary) FILTER (WHERE monetary > 0)::float AS avg_ltv
        FROM intelligence.features_user
        WHERE email_tld_country IS NOT NULL
        GROUP BY email_tld_country
        ORDER BY users DESC
        LIMIT 15
    """)
    return jsonify([dict(r) for r in rows])


@socio_bp.route('/api/sociology/signals')
def sociology_signals():
    rows = query_all("""
        SELECT
            COALESCE(culture, 'unknown')                AS culture,
            COUNT(*) FILTER (WHERE is_oum)              AS oum,
            COUNT(*) FILTER (WHERE is_abou)             AS abou,
            COUNT(*) FILTER (WHERE likely_convert)      AS likely_convert,
            COUNT(*) FILTER (WHERE lastname_culture = 'maghreb') AS lastname_maghreb,
            COUNT(*) FILTER (WHERE is_apple)            AS apple,
            COUNT(*) FILTER (WHERE is_pro_email)        AS pro_email,
            COUNT(*) FILTER (WHERE is_education)        AS edu
        FROM intelligence.features_user
        GROUP BY 1
        ORDER BY culture
    """)
    return jsonify([dict(r) for r in rows])


def register(app):
    app.register_blueprint(socio_bp)

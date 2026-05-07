#!/usr/bin/env python3
"""
dashboard_apps.py — Module Apps Mobile BDouin.

Routes :
  GET /apps                     → page HTML
  GET /api/apps/kpis            → KPIs globaux iOS + Android
  GET /api/apps/ios-monthly     → téléchargements iOS mensuels par app
  GET /api/apps/ios-countries   → top pays iOS
  GET /api/apps/android-monthly → utilisateurs actifs Android mensuels par app
  GET /api/apps/android-quality → crash rate / ANR par app
"""

import psycopg2
from flask import Blueprint, jsonify, send_from_directory
from psycopg2.extras import RealDictCursor

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# Mapping app IDs → noms lisibles
IOS_NAMES = {
    '1612014910': 'Awlad School',
    '6737732771': 'Awlad Quiz GO',
}
ANDROID_NAMES = {
    'com.bdouin.awladschool':       'Awlad School',
    'com.bdouin.awladsalat':        'Awlad Salat',
    'com.bdouin.apps.muslimstrips': 'Muslim Strips',
    'com.bdouin.awladquiz':         'Awlad Quiz',
}

apps_bp = Blueprint('apps_dashboard', __name__)


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


@apps_bp.route('/apps')
def apps_page():
    return send_from_directory('static', 'apps_dashboard.html')


@apps_bp.route('/api/apps/kpis')
def apps_kpis():
    ios = query_one("""
        SELECT
            SUM(units)                                          AS total_downloads,
            SUM(updates)                                        AS total_updates,
            SUM(units) FILTER (WHERE app_id = '1612014910')     AS awlad_school_dl,
            SUM(units) FILTER (WHERE app_id = '6737732771')     AS awlad_quiz_dl,
            MIN(date)                                           AS first_date,
            MAX(date)                                           AS last_date
        FROM asc_downloads
    """)

    android = query_one("""
        SELECT
            SUM(value) FILTER (WHERE package_name='com.bdouin.awladschool'
                               AND metric='distinctUsers' AND date >= '2026-01-01')  AS school_active,
            SUM(value) FILTER (WHERE package_name='com.bdouin.awladsalat'
                               AND metric='distinctUsers' AND date >= '2026-01-01')  AS salat_active,
            SUM(value) FILTER (WHERE package_name='com.bdouin.apps.muslimstrips'
                               AND metric='distinctUsers' AND date >= '2026-01-01')  AS strips_active,
            SUM(value) FILTER (WHERE package_name='com.bdouin.awladquiz'
                               AND metric='distinctUsers' AND date >= '2026-01-01')  AS quiz_active
        FROM playstore_metrics
    """)

    return jsonify({'ios': dict(ios), 'android': dict(android)})


@apps_bp.route('/api/apps/ios-monthly')
def apps_ios_monthly():
    rows = query_all("""
        SELECT
            date_trunc('month', date)::date     AS month,
            app_id,
            SUM(units)                          AS downloads,
            SUM(updates)                        AS updates
        FROM asc_downloads
        GROUP BY 1, 2
        ORDER BY 1 ASC, 2
    """)
    result = [dict(r) | {'app_name': IOS_NAMES.get(r['app_id'], r['app_id'])} for r in rows]
    return jsonify(result)


@apps_bp.route('/api/apps/ios-countries')
def apps_ios_countries():
    rows = query_all("""
        SELECT
            country,
            SUM(units)      AS downloads,
            SUM(updates)    AS updates
        FROM asc_downloads
        GROUP BY country
        ORDER BY downloads DESC
        LIMIT 15
    """)
    return jsonify([dict(r) for r in rows])


@apps_bp.route('/api/apps/android-monthly')
def apps_android_monthly():
    rows = query_all("""
        SELECT
            date_trunc('month', date)::date     AS month,
            package_name,
            SUM(value)                          AS active_users
        FROM playstore_metrics
        WHERE metric = 'distinctUsers'
        GROUP BY 1, 2
        ORDER BY 1 ASC, 2
    """)
    result = [dict(r) | {'app_name': ANDROID_NAMES.get(r['package_name'], r['package_name'])} for r in rows]
    return jsonify(result)


@apps_bp.route('/api/apps/android-quality')
def apps_android_quality():
    rows = query_all("""
        SELECT
            package_name,
            AVG(value) FILTER (WHERE metric = 'crashRate')              AS crash_rate,
            AVG(value) FILTER (WHERE metric = 'anrRate')                AS anr_rate,
            AVG(value) FILTER (WHERE metric = 'userPerceivedCrashRate') AS perceived_crash_rate,
            AVG(value) FILTER (WHERE metric = 'distinctUsers')          AS avg_active_users,
            MAX(date)                                                    AS last_date
        FROM playstore_metrics
        WHERE date >= NOW() - INTERVAL '90 days'
        GROUP BY package_name
        ORDER BY avg_active_users DESC NULLS LAST
    """)
    result = [dict(r) | {'app_name': ANDROID_NAMES.get(r['package_name'], r['package_name'])} for r in rows]
    return jsonify(result)


def register(app):
    app.register_blueprint(apps_bp)

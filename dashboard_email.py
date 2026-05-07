#!/usr/bin/env python3
"""
dashboard_email.py — Module Email (MailerLite) pour le dashboard BDouin.

Routes :
  GET /email                  → page HTML
  GET /api/email/kpis         → KPIs globaux
  GET /api/email/campaigns    → liste complète des campagnes
  GET /api/email/top-campaigns→ top 20 par taux d'ouverture
  GET /api/email/groups       → 39 groupes avec stats
  GET /api/email/monthly      → timeline mensuelle
"""

import psycopg2
from flask import Blueprint, jsonify, send_from_directory
from psycopg2.extras import RealDictCursor

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

email_bp = Blueprint('email_dashboard', __name__)


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


@email_bp.route('/email')
def email_page():
    return send_from_directory('static', 'email_dashboard.html')


@email_bp.route('/api/email/kpis')
def email_kpis():
    data = {}

    data['subscribers'] = query_one("""
        SELECT
            COUNT(*)                                           AS total,
            COUNT(*) FILTER (WHERE status = 'active')         AS active,
            COUNT(*) FILTER (WHERE status = 'unsubscribed')   AS unsubscribed,
            COUNT(*) FILTER (WHERE status = 'bounced')        AS bounced
        FROM ml_subscribers
    """)

    data['campaigns'] = query_one("""
        SELECT
            COUNT(*)                                           AS total_sent,
            AVG(opened_rate)::float                            AS avg_open_rate,
            AVG(clicked_rate)::float                           AS avg_ctr,
            SUM(total_recipients)                              AS total_recipients,
            SUM(unsubscribed)                                  AS total_unsubscribed,
            SUM(bounced)                                       AS total_bounced,
            MAX(date_send)                                     AS last_send
        FROM ml_campaigns
        WHERE status = 'sent'
    """)

    return jsonify({k: dict(v) if v else {} for k, v in data.items()})


@email_bp.route('/api/email/campaigns')
def email_campaigns():
    rows = query_all("""
        SELECT
            id,
            name,
            subject,
            date_send,
            total_recipients,
            opened_count,
            ROUND(opened_rate::numeric, 1)::float    AS opened_rate,
            clicked_count,
            ROUND(clicked_rate::numeric, 1)::float   AS clicked_rate,
            unsubscribed,
            bounced
        FROM ml_campaigns
        WHERE status = 'sent'
        ORDER BY date_send DESC
    """)
    return jsonify([dict(r) for r in rows])


@email_bp.route('/api/email/top-campaigns')
def email_top_campaigns():
    rows = query_all("""
        SELECT
            name,
            subject,
            date_send,
            total_recipients,
            ROUND(opened_rate::numeric, 1)::float  AS opened_rate,
            ROUND(clicked_rate::numeric, 1)::float AS clicked_rate,
            unsubscribed
        FROM ml_campaigns
        WHERE status = 'sent' AND total_recipients > 1000
        ORDER BY opened_rate DESC
        LIMIT 20
    """)
    return jsonify([dict(r) for r in rows])


@email_bp.route('/api/email/groups')
def email_groups():
    rows = query_all("""
        SELECT
            name,
            total,
            active,
            unsubscribed,
            bounced,
            sent,
            opened,
            clicked,
            CASE WHEN sent > 0 THEN ROUND((opened::numeric / sent) * 100, 1) END AS open_rate,
            CASE WHEN sent > 0 THEN ROUND((clicked::numeric / sent) * 100, 1) END AS click_rate
        FROM ml_groups
        ORDER BY total DESC
    """)
    return jsonify([dict(r) for r in rows])


@email_bp.route('/api/email/monthly')
def email_monthly():
    rows = query_all("""
        SELECT
            date_trunc('month', date_send)::date       AS month,
            COUNT(*)                                    AS campaigns,
            SUM(total_recipients)                       AS total_recipients,
            AVG(opened_rate)::float                     AS avg_open_rate,
            AVG(clicked_rate)::float                    AS avg_ctr,
            SUM(unsubscribed)                           AS total_unsubscribed
        FROM ml_campaigns
        WHERE status = 'sent'
        GROUP BY 1
        ORDER BY 1 ASC
    """)
    return jsonify([dict(r) for r in rows])


@email_bp.route('/api/email/campaign-sociology')
def email_campaign_sociology():
    rows = query_all("""
        SELECT
            c.id,
            c.name,
            c.subject,
            c.date_send,
            c.total_recipients,
            ROUND(c.opened_rate::numeric, 1)::float                         AS opened_rate,
            COUNT(DISTINCT o.subscriber_id)                                  AS openers_profiled,
            COUNT(*) FILTER (WHERE fu.culture = 'maghreb')                   AS maghreb,
            COUNT(*) FILTER (WHERE fu.culture = 'europe')                    AS europe,
            COUNT(*) FILTER (WHERE fu.culture = 'mixed')                     AS mixed,
            COUNT(*) FILTER (WHERE fu.culture = 'unknown')                   AS culture_unknown,
            COUNT(*) FILTER (WHERE fu.gender = 'F')                          AS female,
            COUNT(*) FILTER (WHERE fu.gender = 'M')                          AS male,
            COUNT(*) FILTER (WHERE fu.likely_convert = TRUE)                 AS likely_convert,
            COUNT(*) FILTER (WHERE fu.is_oum OR fu.is_abou)                  AS oum_abou
        FROM ml_campaigns c
        JOIN ml_campaign_opens o ON o.campaign_id = c.id
        JOIN gold.user_link ul ON ul.source_id = o.subscriber_id::text
                               AND ul.source_table = 'ml_subscribers'
        JOIN intelligence.features_user fu ON fu.user_id_master = ul.user_id_master
        WHERE c.status = 'sent'
        GROUP BY c.id, c.name, c.subject, c.date_send, c.total_recipients, c.opened_rate
        ORDER BY c.date_send DESC
    """)
    return jsonify([dict(r) for r in rows])


def register(app):
    app.register_blueprint(email_bp)

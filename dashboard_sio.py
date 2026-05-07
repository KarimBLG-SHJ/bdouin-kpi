#!/usr/bin/env python3
"""
dashboard_sio.py — Module OP Spéciale SIO (Lancement packs BDouin, fév-mars 2026)

Routes :
  GET /sio                       → page HTML
  GET /api/sio/kpis              → KPIs globaux opération
  GET /api/sio/funnel            → funnel de conversion ADP par pack
  GET /api/sio/qualification     → répartition client type (new / existing / ml_only)
  GET /api/sio/timeline          → transactions par jour
  GET /api/sio/geography         → répartition par pays
  GET /api/sio/multi_pack        → acheteurs multi-packs
"""

import psycopg2
from flask import Blueprint, jsonify, send_from_directory
from psycopg2.extras import RealDictCursor

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

sio_bp = Blueprint('sio_dashboard', __name__)


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


@sio_bp.route('/sio')
def sio_page():
    return send_from_directory('static', 'sio_dashboard.html')


@sio_bp.route('/api/sio/kpis')
def sio_kpis():
    row = query_one("""
        SELECT
            (SELECT SUM(revenue_total_eur) FROM gold.sio_conversion_funnel)   AS ca_total,
            (SELECT COUNT(DISTINCT invoice_number) FROM public.sio_transactions) AS nb_transactions,
            (SELECT COUNT(*) FROM gold.sio_customer_segments WHERE has_purchased) AS nb_acheteurs,
            (SELECT COUNT(*) FROM gold.sio_customer_segments)                  AS nb_contacts,
            (SELECT COUNT(*) FROM gold.sio_customer_segments WHERE is_multi_pack) AS nb_multi_pack,
            (SELECT COUNT(*) FROM gold.sio_customer_segments WHERE customer_type = 'new_customer') AS nb_nouveaux,
            (SELECT COUNT(*) FROM gold.sio_customer_segments WHERE customer_type = 'existing_customer') AS nb_existants,
            (SELECT COUNT(*) FROM gold.sio_customer_segments WHERE customer_type = 'ml_only') AS nb_ml_only,
            (SELECT ROUND(AVG(amount_eur), 2) FROM public.sio_transactions) AS panier_moyen,
            (SELECT COUNT(*) FROM gold.sio_customer_segments WHERE is_ml_subscriber) AS nb_ml_subs
    """)
    return jsonify(dict(row))


@sio_bp.route('/api/sio/funnel')
def sio_funnel():
    rows = query_all("""
        SELECT
            pack,
            adp_abandoned,
            adp_converted,
            total_adp,
            adp_conversion_rate_pct,
            nb_direct,
            nb_adp_conv,
            total_purchased,
            revenue_direct_eur,
            revenue_adp_eur,
            revenue_total_eur,
            avg_order_eur
        FROM gold.sio_conversion_funnel
        ORDER BY revenue_total_eur DESC
    """)
    return jsonify([dict(r) for r in rows])


@sio_bp.route('/api/sio/qualification')
def sio_qualification():
    rows = query_all("""
        SELECT
            customer_type,
            COUNT(*)                                               AS total,
            COUNT(*) FILTER (WHERE has_purchased)                  AS purchased,
            COUNT(*) FILTER (WHERE is_ml_subscriber)               AS ml_sub,
            COUNT(*) FILTER (WHERE ml_status = 'active')           AS ml_active,
            ROUND(AVG(total_spent_eur) FILTER (WHERE has_purchased), 2) AS avg_spend,
            ROUND(SUM(total_spent_eur), 2)                         AS total_spend,
            COUNT(*) FILTER (WHERE is_presta_newsletter)           AS presta_nl
        FROM gold.sio_customer_segments
        GROUP BY customer_type
        ORDER BY total DESC
    """)
    return jsonify([dict(r) for r in rows])


@sio_bp.route('/api/sio/timeline')
def sio_timeline():
    rows = query_all("""
        SELECT
            transaction_date::DATE                          AS day,
            COUNT(DISTINCT invoice_number)                   AS nb_orders,
            ROUND(SUM(amount_eur), 2)                        AS revenue,
            COUNT(DISTINCT email)                            AS nb_clients,
            COUNT(*) FILTER (WHERE pack = 'awlad_school')   AS awlad,
            COUNT(*) FILTER (WHERE pack = 'famille_foulane') AS foulane,
            COUNT(*) FILTER (WHERE pack = 'mini_guides')    AS guides
        FROM public.sio_transactions
        GROUP BY 1
        ORDER BY 1
    """)
    return jsonify([dict(r) for r in rows])


@sio_bp.route('/api/sio/geography')
def sio_geography():
    rows = query_all("""
        SELECT
            COALESCE(country, 'Inconnu')                    AS country,
            COUNT(*)                                        AS nb_contacts,
            COUNT(*) FILTER (WHERE has_purchased)           AS nb_acheteurs,
            ROUND(SUM(total_spent_eur), 2)                  AS ca_total,
            ROUND(AVG(total_spent_eur) FILTER (WHERE has_purchased), 2) AS panier_moyen
        FROM gold.sio_customer_segments
        WHERE country IS NOT NULL AND country != ''
        GROUP BY 1
        ORDER BY nb_contacts DESC
        LIMIT 20
    """)
    return jsonify([dict(r) for r in rows])


@sio_bp.route('/api/sio/multi_pack')
def sio_multi_pack():
    rows = query_all("""
        SELECT
            nb_packs_purchased                              AS nb_packs,
            COUNT(*)                                        AS nb_clients,
            ROUND(SUM(total_spent_eur), 2)                  AS ca_total,
            ROUND(AVG(total_spent_eur), 2)                  AS avg_spend
        FROM gold.sio_customer_segments
        WHERE has_purchased
        GROUP BY 1
        ORDER BY 1
    """)
    return jsonify([dict(r) for r in rows])


def register(app):
    app.register_blueprint(sio_bp)

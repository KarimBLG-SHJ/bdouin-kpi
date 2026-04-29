#!/usr/bin/env python3
"""
dashboard_exec.py — Routes Flask pour le dashboard exécutif BDouin.

Sources : tables GOLD (rapides, déjà nettoyées et reliées).

Routes :
  GET /exec               → page HTML
  GET /api/exec/kpis      → KPIs globaux JSON
  GET /api/exec/timeline  → courbe revenue 12 mois
  GET /api/exec/top-books → top 10 livres
  GET /api/exec/top-customers → top 10 hardcore fans
  GET /api/exec/funnel    → funnel ML → orders
  GET /api/exec/seo-top   → top 20 requêtes SEO
"""

import psycopg2
from flask import Blueprint, jsonify, send_from_directory
from psycopg2.extras import RealDictCursor

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

exec_bp = Blueprint('exec_dashboard', __name__)


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


@exec_bp.route('/exec')
def exec_page():
    return send_from_directory('static', 'exec_dashboard.html')


@exec_bp.route('/api/exec/kpis')
def kpis():
    """KPIs globaux : CA, commandes, clients, abonnés ML, etc."""
    data = {}

    # CA shop & commandes
    data['shop'] = query_one("""
        SELECT
            COUNT(*)                                                      AS total_orders,
            COUNT(*) FILTER (WHERE ordered_at >= NOW() - INTERVAL '30 days') AS orders_30d,
            COALESCE(SUM(total_paid_eur), 0)::float                       AS total_revenue,
            COALESCE(SUM(total_paid_eur) FILTER (WHERE ordered_at >= NOW() - INTERVAL '30 days'), 0)::float AS revenue_30d,
            COUNT(DISTINCT user_id_master)                                AS unique_buyers
        FROM gold.orders WHERE is_valid AND NOT is_unpaid
    """)

    # Sofiadis B2B
    data['b2b'] = query_one("""
        SELECT
            COALESCE(SUM(ca_net_eur), 0)::float                           AS total_ca,
            COALESCE(SUM(ca_net_eur) FILTER (WHERE period >= (NOW() - INTERVAL '90 days')::date), 0)::float AS ca_90d,
            COUNT(*)                                                       AS months_tracked
        FROM public.sofiadis_b2b_monthly
    """)

    # MailerLite
    data['mailerlite'] = query_one("""
        SELECT
            COUNT(*)                                AS total_subs,
            COUNT(*) FILTER (WHERE is_active)       AS active_subs,
            COUNT(*) FILTER (WHERE is_unsubscribed) AS unsubscribed,
            COUNT(*) FILTER (WHERE is_bounced)      AS bounced
        FROM clean.ml_subscribers
    """)

    # Apps reviews
    data['apps'] = query_one("""
        SELECT
            COUNT(*)                                           AS total_reviews,
            ROUND(AVG(rating)::numeric, 2)::float              AS avg_rating,
            COUNT(*) FILTER (WHERE rating = 5)                 AS rating_5,
            COUNT(*) FILTER (WHERE rating <= 2)                AS rating_low
        FROM clean.reviews
    """)

    # IMAK total cost + Print costs
    data['imak'] = query_one("""
        SELECT
            COUNT(*)                                  AS total_invoices,
            COALESCE(SUM(total_cost_eur), 0)::float   AS total_cost,
            COALESCE(SUM(qty), 0)                     AS total_qty
        FROM public.imak_print_orders
    """)

    # Web traffic last 30d
    data['traffic'] = query_one("""
        SELECT
            COALESCE(SUM(sessions), 0)::int          AS sessions_30d,
            COALESCE(SUM(users), 0)::int             AS users_30d
        FROM public.ga4_sessions
        WHERE date >= (CURRENT_DATE - INTERVAL '30 days')::date
    """)

    # SEO clicks 30d
    data['seo'] = query_one("""
        SELECT
            COALESCE(SUM(clicks), 0)::int             AS clicks_30d,
            COALESCE(SUM(impressions), 0)::int        AS impressions_30d
        FROM public.gsc_queries
        WHERE date >= (CURRENT_DATE - INTERVAL '30 days')::date
    """)

    # IG engagement
    data['instagram'] = query_one("""
        SELECT
            COUNT(*)                                  AS total_posts,
            COALESCE(SUM(like_count), 0)::int        AS total_likes,
            COALESCE(SUM(comments_count), 0)::int    AS total_comments
        FROM public.meta_ig_posts
    """)

    # Veille web mentions
    data['mentions'] = query_one("""
        SELECT COUNT(*) AS total FROM public.web_mentions
    """)

    # Master IDs
    data['masters'] = query_one("""
        SELECT
            (SELECT COUNT(*) FROM gold.users_master)    AS users,
            (SELECT COUNT(*) FROM gold.products_master) AS products,
            (SELECT COUNT(*) FROM gold.content_master)  AS contents
    """)

    # Pipeline status
    data['pipeline'] = query_one("""
        SELECT
            run_id, status, ROUND(duration_sec::numeric, 0)::int AS duration_sec,
            tables_changed, tables_skipped, tables_failed,
            started_at, finished_at
        FROM logs.pipeline_runs
        WHERE status IN ('success','partial')
        ORDER BY run_id DESC LIMIT 1
    """) or {}

    return jsonify(data)


@exec_bp.route('/api/exec/timeline')
def timeline():
    """CA mensuel sur 24 mois — shop + B2B."""
    shop = query_all("""
        SELECT
            TO_CHAR(DATE_TRUNC('month', ordered_at), 'YYYY-MM') AS month,
            COALESCE(SUM(total_paid_eur), 0)::float            AS revenue,
            COUNT(*)                                            AS orders
        FROM gold.orders
        WHERE ordered_at >= NOW() - INTERVAL '24 months'
          AND is_valid AND NOT is_unpaid
        GROUP BY 1 ORDER BY 1
    """)
    b2b = query_all("""
        SELECT
            TO_CHAR(period, 'YYYY-MM') AS month,
            ca_net_eur::float          AS revenue
        FROM public.sofiadis_b2b_monthly
        WHERE period >= (NOW() - INTERVAL '24 months')::date
        ORDER BY period
    """)
    return jsonify({'shop': shop, 'b2b': b2b})


@exec_bp.route('/api/exec/top-books')
def top_books():
    """Top 20 livres par CA shop."""
    rows = query_all("""
        SELECT
            COALESCE(pm.canonical_name, oi.product_name_raw)  AS book,
            SUM(oi.qty_ordered)::int                          AS qty,
            COALESCE(SUM(oi.total_ttc), 0)::float            AS revenue,
            COUNT(DISTINCT oi.order_id)                       AS orders,
            COUNT(DISTINCT oi.user_id_master)                 AS unique_buyers
        FROM gold.order_items oi
        LEFT JOIN gold.products_master pm USING (product_id_master)
        WHERE oi.product_name_raw IS NOT NULL
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT 20
    """)
    return jsonify(rows)


@exec_bp.route('/api/exec/top-customers')
def top_customers():
    """Top 20 hardcore fans (multi-orders)."""
    rows = query_all("""
        SELECT
            ig.primary_email                                  AS email,
            COUNT(o.order_id)                                 AS orders,
            COALESCE(SUM(o.total_paid_eur), 0)::float        AS total_spent,
            MIN(o.ordered_at)                                 AS first_order,
            MAX(o.ordered_at)                                 AS last_order,
            ig.in_mailerlite,
            ig.presta_city,
            ig.ml_country
        FROM gold.user_identity_graph ig
        JOIN gold.orders o USING (user_id_master)
        WHERE o.is_valid AND NOT o.is_unpaid
        GROUP BY ig.primary_email, ig.in_mailerlite, ig.presta_city, ig.ml_country
        ORDER BY total_spent DESC
        LIMIT 20
    """)
    return jsonify(rows)


@exec_bp.route('/api/exec/funnel')
def funnel():
    """Funnel ML → ordered → paid."""
    row = query_one("SELECT * FROM gold.funnel LIMIT 1")
    return jsonify(row or {})


@exec_bp.route('/api/exec/seo-top')
def seo_top():
    """Top 30 requêtes SEO sur 90j."""
    rows = query_all("""
        SELECT
            query                              AS query,
            SUM(clicks)::int                  AS clicks,
            SUM(impressions)::int             AS impressions,
            ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions),0) * 100), 2)::float AS ctr_pct,
            ROUND(AVG(position)::numeric, 1)::float AS avg_position
        FROM public.gsc_queries
        WHERE date >= (CURRENT_DATE - INTERVAL '90 days')::date
        GROUP BY query
        HAVING SUM(clicks) > 0
        ORDER BY clicks DESC
        LIMIT 30
    """)
    return jsonify(rows)


@exec_bp.route('/api/exec/recent-mentions')
def recent_mentions():
    """20 dernières mentions web."""
    rows = query_all("""
        SELECT
            source, keyword, title, url, author, mention_date
        FROM public.web_mentions
        WHERE mention_date IS NOT NULL
        ORDER BY mention_date DESC
        LIMIT 20
    """)
    return jsonify(rows)


@exec_bp.route('/api/exec/countries')
def countries():
    """Top 15 pays par sessions GA4 90j + nb subscribers ML."""
    traffic = query_all("""
        SELECT country, SUM(sessions)::int AS sessions, SUM(users)::int AS users
        FROM public.ga4_sessions
        WHERE date >= (CURRENT_DATE - INTERVAL '90 days')::date
        GROUP BY country
        ORDER BY sessions DESC
        LIMIT 15
    """)
    return jsonify(traffic)


def register(app):
    app.register_blueprint(exec_bp)

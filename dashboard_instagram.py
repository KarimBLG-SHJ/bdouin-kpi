#!/usr/bin/env python3
"""
dashboard_instagram.py — Module Instagram (Meta) pour le dashboard BDouin.

Routes :
  GET /instagram                  → page HTML
  GET /api/instagram/kpis         → KPIs globaux
  GET /api/instagram/top-posts    → top 20 posts par engagement
  GET /api/instagram/monthly      → timeline mensuelle (posts, likes, reach)
  GET /api/instagram/media-types  → répartition par type de média
"""

import psycopg2
from flask import Blueprint, jsonify, send_from_directory
from psycopg2.extras import RealDictCursor

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

ig_bp = Blueprint('instagram_dashboard', __name__)


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


@ig_bp.route('/instagram')
def ig_page():
    return send_from_directory('static', 'instagram_dashboard.html')


@ig_bp.route('/api/instagram/kpis')
def ig_kpis():
    data = {}

    data['posts'] = query_one("""
        SELECT
            COUNT(*)                          AS total_posts,
            SUM(like_count)                   AS total_likes,
            SUM(comments_count)               AS total_comments,
            AVG(like_count)::float            AS avg_likes,
            AVG(comments_count)::float        AS avg_comments,
            MAX(timestamp)::date              AS last_post
        FROM meta_ig_posts
    """)

    data['mentions'] = query_one("""
        SELECT COUNT(*) AS total FROM meta_ig_mentions
    """)

    data['reach'] = query_one("""
        SELECT
            SUM(value)  AS total_reach
        FROM meta_ig_post_insights
        WHERE metric = 'reach'
    """)

    return jsonify({k: dict(v) if v else {} for k, v in data.items()})


@ig_bp.route('/api/instagram/top-posts')
def ig_top_posts():
    rows = query_all("""
        SELECT
            p.id,
            p.timestamp::date                                   AS date,
            p.media_type,
            LEFT(p.caption, 100)                                AS caption,
            p.like_count,
            p.comments_count,
            p.like_count + p.comments_count                     AS engagement,
            COALESCE(r.reach, 0)                                AS reach,
            p.permalink
        FROM meta_ig_posts p
        LEFT JOIN (
            SELECT post_id, value AS reach
            FROM meta_ig_post_insights
            WHERE metric = 'reach'
        ) r ON r.post_id = p.id
        ORDER BY p.like_count + p.comments_count DESC
        LIMIT 20
    """)
    return jsonify([dict(r) for r in rows])


@ig_bp.route('/api/instagram/monthly')
def ig_monthly():
    rows = query_all("""
        SELECT
            date_trunc('month', p.timestamp)::date              AS month,
            COUNT(p.id)                                         AS posts,
            SUM(p.like_count)                                   AS total_likes,
            SUM(p.comments_count)                               AS total_comments,
            AVG(p.like_count)::float                            AS avg_likes,
            COALESCE(SUM(r.value), 0)                           AS total_reach
        FROM meta_ig_posts p
        LEFT JOIN meta_ig_post_insights r
            ON r.post_id = p.id AND r.metric = 'reach'
        GROUP BY 1
        ORDER BY 1 ASC
    """)
    return jsonify([dict(r) for r in rows])


@ig_bp.route('/api/instagram/media-types')
def ig_media_types():
    rows = query_all("""
        SELECT
            media_type,
            COUNT(*)                          AS posts,
            AVG(like_count)::float            AS avg_likes,
            AVG(comments_count)::float        AS avg_comments,
            SUM(like_count + comments_count)  AS total_engagement
        FROM meta_ig_posts
        GROUP BY media_type
        ORDER BY total_engagement DESC
    """)
    return jsonify([dict(r) for r in rows])


def register(app):
    app.register_blueprint(ig_bp)

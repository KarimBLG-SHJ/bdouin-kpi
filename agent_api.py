#!/usr/bin/env python3
"""
agent_api.py — Flask blueprint pour l'agent IA BDouin.

Routes :
  GET  /agent              — page chat HTML
  POST /api/agent/chat     — envoie un message, reçoit la réponse
  POST /api/agent/reset    — reset la conversation
"""

import os
import json
import uuid
import psycopg2
from datetime import datetime
from flask import Blueprint, request, jsonify, send_from_directory, session

import anthropic

from agent_tools import TOOLS_SCHEMA, call_tool

agent_bp = Blueprint('agent', __name__)

# ─── Config ────────────────────────────────────────────────────────────
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
MODEL = os.environ.get('AGENT_MODEL', 'claude-sonnet-4-5-20250929')
MAX_TURNS = 15  # safety limit on tool-call loop
MAX_HISTORY = 10  # last N messages kept in conversation
DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

# ─── System prompt (cached) ───────────────────────────────────────────
SYSTEM_PROMPT = """Tu es l'agent IA BDouin — assistant business pour une maison d'édition de livres jeunesse musulmane.

Ton rôle : répondre aux questions du board et des départements (édition, marketing, sales B2B, production, apps, finance) en interrogeant directement la base de données BDouin.

Tu réponds en français, de manière concise mais informative. Tu cites toujours les chiffres sourcés (jamais inventés). Tu proposes spontanément les bons formats de sortie selon la question.

═══════════════════════════════════════════════════════════════════════
SCHÉMAS DISPONIBLES
═══════════════════════════════════════════════════════════════════════

📊 gold.* — entités business reliées (utilise en priorité)

  gold.users_master(user_id_master, primary_email, in_prestashop, in_mailerlite, first_seen, last_seen)
    → 325k users uniques identifiés par email cross-source

  gold.orders(order_id, user_id_master, ordered_at, total_paid_eur, payment_method, is_valid, is_unpaid)
    → 8 475 commandes shop bdouin.com

  gold.order_items(order_detail_id, order_id, user_id_master, product_id_master, product_name, ean13, qty_ordered, total_ttc, ordered_at)
    → 15 672 lignes de commande (ce qui s'est vendu)

  gold.products_master(product_id_master, canonical_name, ean_or_isbn, sku, avg_price_eur)
    → 80 produits uniques (livres + packs)

  gold.email_activity(user_id_master, email, country_code, city, group_count, group_names, is_active, is_unsubscribed, is_bounced, subscribed_at, unsubscribed_at)
    → 324k abonnés MailerLite avec leurs groupes

  gold.feedback(feedback_type, source_app, country_code, rating, text, author, feedback_date)
    → 73k avis apps + commentaires Instagram

  gold.seo_queries(date, query, query_clean, clicks, impressions, ctr, position)
    → 1.6M requêtes Search Console bdouin.com

  gold.web_traffic(date, country_code, traffic_source, traffic_medium, device, sessions, users)
    → 207k sessions GA4

  gold.user_journey(user_id_master, event_time, event_type, event_value, source, country, revenue)
    → 400k événements timeline (subscribed, ordered, paid, etc.)

  gold.user_identity_graph(user_id_master, primary_email, in_prestashop, in_mailerlite, ml_country, presta_city, phone, multi_source_score)
    → vue 360° d'un user

  gold.attribution_last_touch / attribution_first_touch(order_id, user_id_master, ordered_at, revenue, attributed_source, attributed_event_type)
    → attribution par commande

  gold.content_master(content_id_master, content_type, content_title, content_url, content_source)
    → 10k contenus unifiés (web_page, email_campaign, social_post, document, gmail_thread)

🧠 intelligence.* — features ML + insights

  intelligence.features_user(user_id_master, primary_email, country_code, frequency, monetary, recency_days,
    rfm_recency, rfm_frequency, rfm_monetary, segment, churn_risk_score, churn_factors, ml_active, ml_unsubscribed,
    last_activity_at, days_since_activity, expected_interval_days, activity_gap_ratio)
    → segment IN ('champion','loyal_customer','regular','one_time_buyer','recent_buyer','occasional','churned_one_off','churned','fan_no_purchase','lost_subscriber','inactive_subscriber')

  intelligence.features_product(product_id_master, product_name, ean_or_isbn, sku,
    shop_orders, shop_qty_sold, shop_revenue, shop_qty_90d, return_rate_pct,
    b2b_qty_sold, b2b_revenue, imak_qty_printed, imak_total_cost, imak_avg_unit_cost,
    velocity_per_day, estimated_unit_margin_eur)

  intelligence.features_query(query_clean, total_clicks, total_impressions, ctr_pct, avg_position, opportunity_score, rank_bucket)
    → SEO opportunities

  intelligence.user_clusters(user_id_master, cluster_id, cluster_label)
    → label IN ('engaged_fan','recent_buyer','casual_fan','loyal_buyer','churned_buyer','at_risk_churn','outlier')

  intelligence.cluster_summary(cluster_id, cluster_label, size, avg_monetary, avg_frequency, avg_recency_days, top_country)

  intelligence.product_history(product_id_master, product, month, shop_qty, shop_revenue, b2b_qty, b2b_revenue, total_qty, total_revenue)
    → série mensuelle unifiée par produit

  intelligence.product_seasonality(product_id_master, product, month_num, seasonality_coef, years_seen)

  intelligence.forecast_monthly(product_id_master, product, month, forecast_qty, seasonality_coef, confidence)

  intelligence.reorder_alerts(product_name, estimated_stock, forecast_demand_6m, projected_stock_6m, reorder_status)

  intelligence.patterns_detected(product_a, product_b, co_occurrences, support_pct, confidence_a_to_b, lift)
    → market basket : livres achetés ensemble

  intelligence.anomalies(metric, period, observed_value, expected_value, z_score, anomaly_type)

  intelligence.opportunities(opportunity_type, title, description, score, details, action_suggested)
    → synthèse actionable

  intelligence.demand_radar(source, term, volume_recent, volume_previous, volume_delta, growth_pct)
    → trends WoW (Search Console + recherches internes)

  intelligence.review_themes(review_id, app, theme, rating, review_date)
    → themes IN ('bug_crash','app_loading','pricing','content_arabic','content_islam','audio_quality','ads_complaint','ux_praise','ux_complaint','kid_friendly','progression','feature_request','crash_after_update','login_issue','bug_general','recommendation','gratitude','translation')

  intelligence.review_sentiment(review_id, app, rating, sentiment, sentiment_score)

═══════════════════════════════════════════════════════════════════════
RÈGLES DE COMPORTEMENT
═══════════════════════════════════════════════════════════════════════

1. **Préfère gold.* et intelligence.*** plutôt que public.* (sauf cas spécifiques).
2. **Liste/export demandé** → `generate_excel`. NE pas dump 300 lignes en texte.
3. **Évolution / comparaison / courbe** → `generate_chart`. Choisis le bon chart_type.
4. **Question chiffre simple** → `query_db` direct, présente le résultat.
5. **Toujours sourcer** : "selon gold.orders" / "d'après intelligence.features_user".
6. **Réponse markdown propre** : titres, listes, tableaux. Mets les chiffres importants en **gras**.
7. **Liens download visibles** : "📥 [Télécharger l'Excel]" / "📊 [Voir le graphique]".
8. **Si erreur SQL** : corrige et réessaie (max 2 fois).
9. **Si tu hésites entre tables** : appelle `describe_table` pour voir les colonnes exactes.
10. **Sois concis** : pas de blabla, va à l'essentiel. Mais explique le contexte business si pertinent.
11. **Format euros** : `1 234€` (espaces fins, pas de décimales sauf si <100).
12. **Dates** : YYYY-MM-DD ou YYYY-MM pour les mois.

Si l'utilisateur pose une question hors-data BDouin (météo, news, etc.) → décline poliment et propose une question business pertinente.
"""


# ─── Anthropic client ──────────────────────────────────────────────────
def get_client():
    if not ANTHROPIC_KEY:
        return None
    return anthropic.Anthropic(api_key=ANTHROPIC_KEY)


# ─── Conversation logging ─────────────────────────────────────────────
def log_conversation(session_id: str, role: str, content: str, tools_used: list = None):
    try:
        with psycopg2.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS logs.agent_conversations (
                        id           BIGSERIAL PRIMARY KEY,
                        session_id   TEXT,
                        role         TEXT,
                        content      TEXT,
                        tools_used   JSONB,
                        created_at   TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    INSERT INTO logs.agent_conversations (session_id, role, content, tools_used)
                    VALUES (%s, %s, %s, %s)
                """, (session_id, role, content[:10000], json.dumps(tools_used or [])))
            conn.commit()
    except Exception as e:
        print(f'[log_conv] {e}')


# ─── Conversation memory (in-memory dict) ─────────────────────────────
CONVERSATIONS = {}


def get_session_id():
    sid = session.get('agent_sid')
    if not sid:
        sid = str(uuid.uuid4())
        session['agent_sid'] = sid
    return sid


# ─── Routes ────────────────────────────────────────────────────────────

@agent_bp.route('/agent')
def agent_page():
    """Serve chat HTML."""
    here = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(os.path.join(here, 'static'), 'agent.html')


@agent_bp.route('/api/agent/chat', methods=['POST'])
def chat():
    """Process a user message, run Claude with tools, return final response."""
    client = get_client()
    if not client:
        return jsonify({'error': 'ANTHROPIC_API_KEY not configured'}), 500

    data = request.get_json() or {}
    user_msg = (data.get('message') or '').strip()
    if not user_msg:
        return jsonify({'error': 'Empty message'}), 400

    sid = get_session_id()
    history = CONVERSATIONS.setdefault(sid, [])

    # Append user message
    history.append({'role': 'user', 'content': user_msg})
    log_conversation(sid, 'user', user_msg)

    # Trim history to last MAX_HISTORY
    if len(history) > MAX_HISTORY * 2:
        history = history[-MAX_HISTORY * 2:]
        CONVERSATIONS[sid] = history

    # Loop with tools
    tool_calls_log = []
    final_text = ''

    try:
        for turn in range(MAX_TURNS):
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=[{
                    'type': 'text',
                    'text': SYSTEM_PROMPT,
                    'cache_control': {'type': 'ephemeral'},
                }],
                tools=TOOLS_SCHEMA,
                messages=history,
            )

            # Check stop reason
            if response.stop_reason == 'end_turn':
                # Final answer
                for block in response.content:
                    if block.type == 'text':
                        final_text += block.text
                history.append({'role': 'assistant', 'content': response.content})
                break

            elif response.stop_reason == 'tool_use':
                # Append assistant message with tool_use blocks
                history.append({'role': 'assistant', 'content': response.content})

                # Execute each tool_use block
                tool_results = []
                for block in response.content:
                    if block.type == 'tool_use':
                        tool_input = block.input
                        result = call_tool(block.name, **tool_input)
                        tool_calls_log.append({'name': block.name, 'input': tool_input, 'truncated_result': str(result)[:300]})
                        tool_results.append({
                            'type': 'tool_result',
                            'tool_use_id': block.id,
                            'content': json.dumps(result, ensure_ascii=False, default=str)[:30000],
                        })
                    elif block.type == 'text':
                        final_text += block.text + '\n'

                # Send tool results back
                history.append({'role': 'user', 'content': tool_results})
            else:
                # max_tokens or other
                for block in response.content:
                    if block.type == 'text':
                        final_text += block.text
                history.append({'role': 'assistant', 'content': response.content})
                break
        else:
            final_text += '\n\n_(Limite de tours atteinte — réponse partielle)_'

        log_conversation(sid, 'assistant', final_text, tool_calls_log)

        return jsonify({
            'response': final_text.strip() or '(pas de réponse)',
            'tools_used': tool_calls_log,
            'session_id': sid,
        })

    except Exception as e:
        err = f'Agent error: {str(e)[:500]}'
        log_conversation(sid, 'error', err)
        return jsonify({'error': err}), 500


@agent_bp.route('/api/agent/reset', methods=['POST'])
def reset():
    sid = get_session_id()
    CONVERSATIONS.pop(sid, None)
    session.pop('agent_sid', None)
    return jsonify({'ok': True})


@agent_bp.route('/api/agent/health', methods=['GET'])
def health():
    return jsonify({
        'ok': True,
        'has_api_key': bool(ANTHROPIC_KEY),
        'model': MODEL,
        'max_turns': MAX_TURNS,
        'tools_available': [t['name'] for t in TOOLS_SCHEMA],
    })


def register(app):
    app.register_blueprint(agent_bp)

import os
import json
import requests
from flask import Flask, request, Response, send_from_directory, jsonify

app = Flask(__name__, static_folder="static")

PRESTA_BASE = "https://www.bdouin.com/api"
PRESTA_KEY = os.environ.get("PRESTA_KEY", "AU83IAKGBTE3SRAIW85IFLZ8642AXQPH")
MAILERLITE_BASE = "https://api.mailerlite.com/api/v2"
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/presta/<path:path>")
def proxy_presta(path):
    """Proxy requests to PrestaShop API."""
    url = f"{PRESTA_BASE}/{path}"
    params = request.args.to_dict(flat=False)
    # Flatten single-value params
    params = {k: v[0] if len(v) == 1 else v for k, v in params.items()}
    try:
        resp = requests.get(url, params=params, timeout=30)
        excluded_headers = ["content-encoding", "content-length", "transfer-encoding", "connection"]
        headers = {k: v for k, v in resp.raw.headers.items() if k.lower() not in excluded_headers}
        return Response(resp.content, status=resp.status_code, headers=headers)
    except Exception as e:
        return Response(f'{{"error": "{str(e)}"}}', status=502, content_type="application/json")


@app.route("/api/mailerlite/<path:path>")
def proxy_mailerlite(path):
    """Proxy requests to MailerLite API."""
    url = f"{MAILERLITE_BASE}/{path}"
    # Forward the Authorization header
    headers = {}
    if "X-MailerLite-ApiKey" in request.headers:
        headers["X-MailerLite-ApiKey"] = request.headers["X-MailerLite-ApiKey"]
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        excluded_headers = ["content-encoding", "content-length", "transfer-encoding", "connection"]
        out_headers = {k: v for k, v in resp.raw.headers.items() if k.lower() not in excluded_headers}
        return Response(resp.content, status=resp.status_code, headers=out_headers)
    except Exception as e:
        return Response(f'{{"error": "{str(e)}"}}', status=502, content_type="application/json")


@app.route("/api/ga4")
def proxy_ga4():
    """Fetch GA4 metrics using Google Analytics Data API."""
    if not GA4_PROPERTY_ID:
        return jsonify({"error": "GA4_PROPERTY_ID not configured"}), 503

    ga4_creds = os.environ.get("GA4_CREDENTIALS_JSON", "")
    if not ga4_creds:
        return jsonify({"error": "GA4_CREDENTIALS_JSON not configured"}), 503

    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            RunReportRequest, DateRange, Dimension, Metric
        )
        from google.oauth2 import service_account

        creds_dict = json.loads(ga4_creds)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        client = BetaAnalyticsDataClient(credentials=credentials)

        # Main KPIs: sessions, users, bounce rate, conversions (28 days)
        kpi_request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            date_ranges=[DateRange(start_date="28daysAgo", end_date="today")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="newUsers"),
                Metric(name="bounceRate"),
                Metric(name="conversions"),
                Metric(name="purchaseRevenue"),
            ],
        )
        kpi_resp = client.run_report(kpi_request)
        kpi_row = kpi_resp.rows[0] if kpi_resp.rows else None

        from google.analytics.data_v1beta.types import FilterExpression, Filter

        # Traffic sources WITH conversions & revenue per channel
        sources_request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            date_ranges=[DateRange(start_date="28daysAgo", end_date="today")],
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="conversions"),
                Metric(name="purchaseRevenue"),
            ],
            limit=10,
        )
        sources_resp = client.run_report(sources_request)

        # Top PRODUCT pages only (filter URLs containing .html = product pages on PrestaShop)
        pages_request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            date_ranges=[DateRange(start_date="28daysAgo", end_date="today")],
            dimensions=[Dimension(name="pagePath")],
            metrics=[Metric(name="screenPageViews"), Metric(name="activeUsers")],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="pagePath",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.CONTAINS,
                        value=".html",
                    ),
                )
            ),
            limit=15,
        )
        pages_resp = client.run_report(pages_request)

        # Funnel: add to cart, begin checkout, purchase
        funnel_request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            date_ranges=[DateRange(start_date="28daysAgo", end_date="today")],
            metrics=[
                Metric(name="addToCarts"),
                Metric(name="checkouts"),
                Metric(name="ecommercePurchases"),
            ],
        )
        try:
            funnel_resp = client.run_report(funnel_request)
            funnel_row = funnel_resp.rows[0] if funnel_resp.rows else None
        except Exception:
            funnel_row = None

        total_sessions = int(kpi_row.metric_values[0].value) if kpi_row else 0

        result = {
            "kpis": {
                "sessions": total_sessions,
                "activeUsers": int(kpi_row.metric_values[1].value) if kpi_row else 0,
                "newUsers": int(kpi_row.metric_values[2].value) if kpi_row else 0,
                "bounceRate": float(kpi_row.metric_values[3].value) if kpi_row else 0,
                "conversions": int(kpi_row.metric_values[4].value) if kpi_row else 0,
                "revenue": float(kpi_row.metric_values[5].value) if kpi_row else 0,
            },
            "funnel": {
                "addToCarts": int(funnel_row.metric_values[0].value) if funnel_row else 0,
                "checkouts": int(funnel_row.metric_values[1].value) if funnel_row else 0,
                "purchases": int(funnel_row.metric_values[2].value) if funnel_row else 0,
            },
            "sources": [
                {
                    "channel": row.dimension_values[0].value,
                    "sessions": int(row.metric_values[0].value),
                    "users": int(row.metric_values[1].value),
                    "conversions": int(row.metric_values[2].value),
                    "revenue": float(row.metric_values[3].value),
                }
                for row in sources_resp.rows
            ],
            "topPages": [
                {
                    "path": row.dimension_values[0].value,
                    "views": int(row.metric_values[0].value),
                    "users": int(row.metric_values[1].value),
                }
                for row in pages_resp.rows
            ],
        }

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat", methods=["POST"])
def chat():
    """AI Chatbot that queries PrestaShop/GA4/MailerLite APIs."""
    if not OPENAI_API_KEY:
        return jsonify({"error": "OPENAI_API_KEY not configured"}), 503

    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

    user_msg = request.json.get("message", "")
    history = request.json.get("history", [])
    if not user_msg:
        return jsonify({"error": "No message"}), 400

    # Tool definitions for the LLM
    tools = [
        {
            "type": "function",
            "function": {
                "name": "query_prestashop",
                "description": "Query the PrestaShop API to get orders, products, customers, messages, order history, etc. Use resource like 'orders', 'order_details', 'customer_messages', 'order_histories', 'products', 'customers'.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "resource": {
                            "type": "string",
                            "description": "API resource: orders, order_details, customer_messages, order_histories, products, customers"
                        },
                        "display": {
                            "type": "string",
                            "description": "Fields to retrieve, e.g. '[id,total_paid_tax_incl,date_add,current_state]'"
                        },
                        "filter": {
                            "type": "string",
                            "description": "Optional filter params, e.g. 'filter[current_state]=3' or 'filter[date_add]=[2025-01-01,2025-01-31]'"
                        },
                        "sort": {
                            "type": "string",
                            "description": "Sort order, e.g. '[id_DESC]'"
                        },
                        "limit": {
                            "type": "string",
                            "description": "Limit results, e.g. '100'"
                        }
                    },
                    "required": ["resource", "display"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "query_ga4",
                "description": "Get Google Analytics data: sessions, users, bounce rate, conversions, revenue, traffic sources, top pages, conversion funnel.",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        }
    ]

    system_prompt = """Tu es l'assistant IA du dashboard BDouin, un éditeur de livres islamiques (e-commerce PrestaShop sur bdouin.com).

Tu réponds en français, de manière concise et utile. Tu aides Karim à piloter son business.

Données disponibles via tes outils :
- PrestaShop : commandes (orders), détails produits (order_details), messages clients (customer_messages), historique statuts (order_histories), produits (products), clients (customers)
- Google Analytics (GA4) : sessions, utilisateurs, taux de rebond, conversions, revenus, sources de trafic, pages populaires

Les states PrestaShop importants :
- 2 = en attente paiement
- 3 = en préparation
- 4 = expédié
- 5 = livré
- 6 = annulé
- 7 = remboursé
- 8 = erreur paiement

Quand tu donnes des montants, utilise le format français avec €.
Sois direct, donne les chiffres, pas de blabla."""

    messages = [{"role": "system", "content": system_prompt}]
    # Add conversation history (last 10 messages)
    for h in history[-10:]:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_msg})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=tools,
            tool_choice="auto",
            temperature=0.3,
            max_tokens=1000,
        )

        msg = response.choices[0].message

        # Handle tool calls (up to 3 rounds)
        rounds = 0
        while msg.tool_calls and rounds < 3:
            rounds += 1
            messages.append(msg)

            for tc in msg.tool_calls:
                args = json.loads(tc.function.arguments)
                result = ""

                if tc.function.name == "query_prestashop":
                    try:
                        url = f"{PRESTA_BASE}/{args['resource']}"
                        params = {
                            "display": args.get("display", "full"),
                            "output_format": "JSON",
                            "ws_key": PRESTA_KEY,
                            "sort": args.get("sort", "[id_DESC]"),
                            "limit": args.get("limit", "200"),
                        }
                        if args.get("filter"):
                            # Parse filter string like "filter[current_state]=3"
                            for part in args["filter"].split("&"):
                                if "=" in part:
                                    k, v = part.split("=", 1)
                                    params[k] = v
                        resp = requests.get(url, params=params, timeout=30)
                        data = resp.json()
                        # Truncate if too large
                        result = json.dumps(data, ensure_ascii=False)
                        if len(result) > 8000:
                            result = result[:8000] + "... (tronqué)"
                    except Exception as e:
                        result = f"Erreur API PrestaShop: {str(e)}"

                elif tc.function.name == "query_ga4":
                    try:
                        # Reuse the existing GA4 logic
                        import urllib.request
                        ga4_url = request.host_url.rstrip("/") + "/api/ga4"
                        ga4_resp = requests.get(ga4_url, timeout=30)
                        result = ga4_resp.text
                        if len(result) > 8000:
                            result = result[:8000] + "... (tronqué)"
                    except Exception as e:
                        result = f"Erreur GA4: {str(e)}"

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=0.3,
                max_tokens=1000,
            )
            msg = response.choices[0].message

        return jsonify({"reply": msg.content or "Je n'ai pas pu générer de réponse."})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

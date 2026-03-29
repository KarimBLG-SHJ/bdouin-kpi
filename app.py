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
        return jsonify({"error": "OPENAI_API_KEY non configurée. Ajoutez-la dans les variables Railway."}), 503

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
                "description": "Query the PrestaShop API. Resources: orders, order_details, customer_messages, order_histories, products, customers. For best sellers, use order_details with product_name and product_quantity. For revenue, use orders with total_paid_tax_incl. Always use date filters when asking about a specific period.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "resource": {
                            "type": "string",
                            "description": "API resource: orders, order_details, customer_messages, order_histories, products, customers"
                        },
                        "display": {
                            "type": "string",
                            "description": "Fields to retrieve, e.g. '[id,total_paid_tax_incl,date_add,current_state]' or 'full'"
                        },
                        "filter": {
                            "type": "string",
                            "description": "Filter params, e.g. 'filter[current_state]=3' or 'filter[date_add]=[2026-01-01,2026-01-31]'. Multiple filters separated by &"
                        },
                        "sort": {
                            "type": "string",
                            "description": "Sort order, e.g. '[id_DESC]' or '[product_quantity_DESC]'"
                        },
                        "limit": {
                            "type": "string",
                            "description": "Max results, e.g. '500'"
                        }
                    },
                    "required": ["resource"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "query_ga4",
                "description": "Get Google Analytics data (last 28 days): sessions, users, bounce rate, conversions, revenue, traffic sources, top pages, conversion funnel.",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }
    ]

    system_prompt = """Tu es l'assistant IA du dashboard BDouin, un éditeur de livres islamiques (e-commerce PrestaShop sur bdouin.com).
Tu réponds en français, de manière concise et utile. Tu aides Karim à piloter son business.

IMPORTANT - Comment utiliser l'API PrestaShop :
- orders : champs disponibles = id, current_state, total_paid_tax_incl, date_add, payment, total_products. Supporte filter[date_add]=[YYYY-MM-DD,YYYY-MM-DD] et filter[current_state]=X
- order_details : champs = id, id_order, product_name, product_quantity. PAS de champ date_add ! Pour filtrer par date, récupère d'abord les orders de la période, puis utilise filter[id_order]=[id1|id2|id3] sur order_details.
- customer_messages : champs = id, id_employee, id_customer_thread, date_add, message
- order_histories : champs = id_order, id_order_state, date_add
- products : champs = id, name, price, active, quantity
- customers : champs = id, firstname, lastname, email, date_add

States commandes : 2=attente paiement, 3=préparation, 4=expédié, 5=livré, 6=annulé, 7=remboursé, 8=erreur paiement

STRATEGIE pour "produits les plus vendus sur une période" :
1. Appelle orders avec display=[id,date_add] et filter[date_add]=[YYYY-MM-01,YYYY-MM-31] et limit=5000
2. Appelle order_details avec display=[id_order,product_name,product_quantity] et limit=5000
3. Croise les données : garde les order_details dont id_order est dans la liste des orders de la période
4. Agrège par product_name et trie par quantité totale

Quand tu donnes des montants, utilise le format français avec €. Sois direct, donne les chiffres, pas de blabla."""

    messages_list = [{"role": "system", "content": system_prompt}]
    for h in history[-10:]:
        messages_list.append({"role": h["role"], "content": h["content"]})
    messages_list.append({"role": "user", "content": user_msg})

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages_list,
            tools=tools,
            tool_choice="auto",
            temperature=0.3,
            max_tokens=1500,
        )

        msg = response.choices[0].message
        app.logger.info(f"[CHAT] Initial response: content={msg.content}, tool_calls={bool(msg.tool_calls)}")

        # Handle tool calls (up to 3 rounds)
        rounds = 0
        while msg.tool_calls and rounds < 3:
            rounds += 1
            # Serialize the assistant message properly
            assistant_msg = {"role": "assistant", "content": msg.content or "", "tool_calls": []}
            for tc in msg.tool_calls:
                assistant_msg["tool_calls"].append({
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                })
            messages_list.append(assistant_msg)

            for tc in msg.tool_calls:
                args = json.loads(tc.function.arguments)
                result = ""
                app.logger.info(f"[CHAT] Tool call: {tc.function.name}, args={args}")

                if tc.function.name == "query_prestashop":
                    try:
                        url = f"{PRESTA_BASE}/{args.get('resource', 'orders')}"
                        params = {
                            "output_format": "JSON",
                            "ws_key": PRESTA_KEY,
                        }
                        if args.get("display"):
                            params["display"] = args["display"]
                        if args.get("sort"):
                            params["sort"] = args["sort"]
                        if args.get("limit"):
                            params["limit"] = args["limit"]
                        else:
                            params["limit"] = "500"
                        if args.get("filter"):
                            for part in args["filter"].split("&"):
                                if "=" in part:
                                    k, v = part.split("=", 1)
                                    params[k] = v
                        resp = requests.get(url, params=params, timeout=30)
                        app.logger.info(f"[CHAT] PrestaShop status={resp.status_code}, url={resp.url}")
                        try:
                            data = resp.json()
                            result = json.dumps(data, ensure_ascii=False)
                        except Exception:
                            result = resp.text[:3000]
                        if len(result) > 8000:
                            result = result[:8000] + "... (tronqué)"
                    except Exception as e:
                        result = f"Erreur API PrestaShop: {str(e)}"
                        app.logger.error(f"[CHAT] PrestaShop error: {e}")

                elif tc.function.name == "query_ga4":
                    try:
                        ga4_url = request.host_url.rstrip("/") + "/api/ga4"
                        ga4_resp = requests.get(ga4_url, timeout=30)
                        result = ga4_resp.text
                        if len(result) > 8000:
                            result = result[:8000] + "... (tronqué)"
                    except Exception as e:
                        result = f"Erreur GA4: {str(e)}"

                messages_list.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result or "Aucune donnée retournée",
                })

            # Allow tools for first 2 rounds, then force text answer
            if rounds < 3:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=messages_list,
                    tools=tools,
                    tool_choice="auto",
                    temperature=0.3,
                    max_tokens=1500,
                )
            else:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=messages_list,
                    temperature=0.3,
                    max_tokens=1500,
                )
            msg = response.choices[0].message
            app.logger.info(f"[CHAT] Round {rounds} response: content={msg.content and msg.content[:200]}, tool_calls={bool(msg.tool_calls)}")

            if msg.content and not msg.tool_calls:
                break

        final_reply = msg.content or "Désolé, je n'ai pas réussi à traiter ta demande. Réessaie avec une question plus précise."
        return jsonify({"reply": final_reply})

    except Exception as e:
        app.logger.error(f"[CHAT] Exception: {e}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({"reply": f"Erreur : {str(e)}"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

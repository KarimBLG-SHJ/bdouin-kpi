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
                "name": "best_sellers",
                "description": "Get the top selling products for a specific period. Returns top 20 products ranked by quantity sold. Use this for any question about best sellers, most sold products, popular items.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "description": "Start date YYYY-MM-DD, e.g. '2026-03-01'"
                        },
                        "end_date": {
                            "type": "string",
                            "description": "End date YYYY-MM-DD, e.g. '2026-03-31'"
                        }
                    },
                    "required": ["start_date", "end_date"]
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

STRATEGIE pour "produits les plus vendus" : utilise l'outil best_sellers avec les dates de début et fin. C'est le plus fiable.

Pour "combien de commandes" ou "CA" : utilise query_prestashop avec resource=orders, display=[id,current_state,total_paid_tax_incl,date_add]. ATTENTION : date_add n'est PAS filtrable côté API. Le serveur filtre en Python. Utilise le paramètre date_from et date_to dans le filter si besoin.

Pour toute question générale (messages clients, état des commandes, etc.) : utilise query_prestashop avec la resource appropriée.

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
                        resource = args.get('resource', 'orders')
                        url = f"{PRESTA_BASE}/{resource}"
                        params = {
                            "output_format": "JSON",
                            "ws_key": PRESTA_KEY,
                        }
                        if args.get("display"):
                            params["display"] = args["display"]
                        if args.get("sort"):
                            params["sort"] = args["sort"]
                        params["limit"] = args.get("limit", "5000")
                        # Extract date filters (not supported by PS API, we filter in Python)
                        date_from = None
                        date_to = None
                        if args.get("filter"):
                            for part in args["filter"].split("&"):
                                if "=" in part:
                                    k, v = part.split("=", 1)
                                    if "date_add" in k:
                                        # Parse [YYYY-MM-DD,YYYY-MM-DD]
                                        dates = v.strip("[]").split(",")
                                        if len(dates) == 2:
                                            date_from, date_to = dates[0].strip(), dates[1].strip()
                                    else:
                                        params[k] = v
                        resp = requests.get(url, params=params, timeout=30)
                        app.logger.info(f"[CHAT] PrestaShop status={resp.status_code}, url={resp.url}")
                        try:
                            data = resp.json()
                        except Exception:
                            result = resp.text[:3000]
                            data = None

                        # Smart aggregation to avoid sending raw data to GPT
                        if data and resource == "order_details":
                            items = data.get("order_details", [])
                            from collections import Counter
                            sales = Counter()
                            for item in items:
                                name = item.get("product_name", "?")
                                qty = int(item.get("product_quantity", 1))
                                sales[name] += qty
                            top20 = sales.most_common(20)
                            result = json.dumps({"top_produits": [{"produit": n, "quantite": q} for n, q in top20], "total_lignes": len(items)}, ensure_ascii=False)
                        elif data and resource == "orders":
                            items = data.get("orders", [])
                            # Filter by date in Python if requested
                            if date_from and date_to:
                                items = [o for o in items if date_from <= str(o.get("date_add", ""))[:10] <= date_to]
                            from collections import Counter
                            if len(items) > 50:
                                total_rev = sum(float(o.get("total_paid_tax_incl", 0)) for o in items)
                                valid = [o for o in items if float(o.get("total_paid_tax_incl", 0)) > 0 and int(o.get("current_state", 0)) not in [6, 7, 8]]
                                valid_rev = sum(float(o.get("total_paid_tax_incl", 0)) for o in valid)
                                states = Counter(int(o.get("current_state", 0)) for o in items)
                                avg_ticket = valid_rev / len(valid) if valid else 0
                                result = json.dumps({
                                    "periode": f"{date_from or 'tout'} → {date_to or 'tout'}",
                                    "resume": f"{len(items)} commandes total, {len(valid)} valides, CA: {valid_rev:.2f}€, panier moyen: {avg_ticket:.2f}€",
                                    "etats": {str(k): v for k, v in states.most_common()},
                                    "dernières_10": items[:10]
                                }, ensure_ascii=False)
                            else:
                                result = json.dumps({"orders": items, "count": len(items)}, ensure_ascii=False)
                        elif data:
                            result = json.dumps(data, ensure_ascii=False)

                        if result and len(result) > 12000:
                            result = result[:12000] + "... (tronqué)"
                    except Exception as e:
                        result = f"Erreur API PrestaShop: {str(e)}"
                        app.logger.error(f"[CHAT] PrestaShop error: {e}")

                elif tc.function.name == "best_sellers":
                    try:
                        start = args.get("start_date", "2026-01-01")
                        end = args.get("end_date", "2026-12-31")
                        # Step 1: Get all recent orders (date_add is NOT filterable in PrestaShop API)
                        orders_url = f"{PRESTA_BASE}/orders"
                        orders_resp = requests.get(orders_url, params={
                            "output_format": "JSON", "ws_key": PRESTA_KEY,
                            "display": "[id,date_add,current_state]",
                            "sort": "[id_DESC]",
                            "limit": "10000"
                        }, timeout=30)
                        orders_data = orders_resp.json()
                        order_list = orders_data.get("orders", [])
                        # Filter by date range in Python + exclude cancelled/refunded
                        valid_ids = set()
                        for o in order_list:
                            d = str(o.get("date_add", ""))[:10]  # YYYY-MM-DD
                            state = int(o.get("current_state", 0))
                            if d >= start and d <= end and state not in [6, 7, 8]:
                                valid_ids.add(str(o["id"]))
                        app.logger.info(f"[CHAT] best_sellers: {len(valid_ids)} valid orders for {start} to {end}")

                        # Step 2: Get recent order_details (sorted DESC to get newest first)
                        details_url = f"{PRESTA_BASE}/order_details"
                        details_resp = requests.get(details_url, params={
                            "output_format": "JSON", "ws_key": PRESTA_KEY,
                            "display": "[id_order,product_name,product_quantity]",
                            "sort": "[id_DESC]",
                            "limit": "15000"
                        }, timeout=30)
                        details_data = details_resp.json()
                        all_details = details_data.get("order_details", [])

                        # Step 3: Cross-reference and aggregate
                        from collections import Counter
                        sales = Counter()
                        for item in all_details:
                            if str(item.get("id_order")) in valid_ids:
                                name = item.get("product_name", "?")
                                qty = int(item.get("product_quantity", 1))
                                sales[name] += qty

                        top20 = sales.most_common(20)
                        total_qty = sum(q for _, q in top20)
                        result = json.dumps({
                            "periode": f"{start} → {end}",
                            "nb_commandes_valides": len(valid_ids),
                            "top_20_produits": [{"rang": i+1, "produit": n, "quantite": q} for i, (n, q) in enumerate(top20)],
                            "total_top20": total_qty
                        }, ensure_ascii=False)
                    except Exception as e:
                        result = f"Erreur best_sellers: {str(e)}"
                        app.logger.error(f"[CHAT] best_sellers error: {e}")

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

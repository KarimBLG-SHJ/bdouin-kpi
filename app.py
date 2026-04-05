import os
import json
import hashlib
import hmac
import time
import secrets
import requests
from functools import wraps
from flask import Flask, request, Response, send_from_directory, jsonify, make_response, redirect

app = Flask(__name__, static_folder="static")

PRESTA_BASE = "https://www.bdouin.com/api"
MAILERLITE_BASE = "https://api.mailerlite.com/api/v2"
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "")
NOTION_API_KEY = os.environ.get("NOTION_API_KEY", "")
NOTION_IMAK_DB = "19ebc51392ec4527ba456d9db6ce7400"

# Auth
DASH_PASSWORD_HASH = "1c27c98794afb7eac2f413673a8900ee7684fcafec7c7df53235f836db7e8a29"
COOKIE_SECRET = os.environ.get("COOKIE_SECRET", secrets.token_hex(32))
COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days


def _sign_token(timestamp):
    msg = f"bdouin-dash:{timestamp}".encode()
    return hmac.new(COOKIE_SECRET.encode(), msg, hashlib.sha256).hexdigest()


def _check_auth():
    token = request.cookies.get("bdouin_auth")
    if not token:
        return False
    try:
        ts, sig = token.split(":", 1)
        if time.time() - float(ts) > COOKIE_MAX_AGE:
            return False
        return hmac.compare_digest(sig, _sign_token(ts))
    except (ValueError, TypeError):
        return False


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _check_auth():
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


LOGIN_HTML = """<!DOCTYPE html>
<html lang="fr"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>BDouin — Connexion</title>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Manrope',sans-serif;background:#0c1117;color:#e4e8ec;display:flex;align-items:center;justify-content:center;min-height:100vh}
.lock-box{background:#141c24;border:1px solid #1e2d3a;border-radius:16px;padding:48px 40px;text-align:center;max-width:380px;width:90%}
.lock-box img{width:120px;margin-bottom:16px;border-radius:12px}
.lock-box h2{font-size:22px;margin-bottom:6px}
.lock-box h2 span{color:#24b9d7}
.lock-box p{font-size:13px;color:#7a8d9e;margin-bottom:24px}
.lock-box input{width:100%;padding:12px 16px;border-radius:10px;border:1px solid #1e2d3a;background:#0c1117;color:#e4e8ec;font-size:15px;text-align:center;outline:none;transition:border 0.2s;font-family:inherit}
.lock-box input:focus{border-color:#24b9d7}
.lock-box button{width:100%;margin-top:12px;padding:12px;border:none;border-radius:10px;background:#24b9d7;color:#fff;font-size:14px;font-weight:600;cursor:pointer;transition:opacity 0.2s;font-family:inherit}
.lock-box button:hover{opacity:0.85}
.error{color:#ef4444;font-size:12px;margin-top:8px;display:none}
</style></head><body>
<div class="lock-box">
<img src="https://www.bdouin.com/img/logo-1683623253.jpg" alt="BDouin">
<h2><span>KPI</span> Dashboard</h2>
<p>Accès restreint — entrez le mot de passe</p>
<form method="POST" action="/login">
<input type="password" name="password" placeholder="Mot de passe" autofocus required>
<button type="submit">Accéder</button>
<div class="error" id="err">Mot de passe incorrect</div>
</form></div>
ERRSCRIPT
</body></html>"""


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if _check_auth():
            return redirect("/")
        return LOGIN_HTML.replace("ERRSCRIPT", "")

    password = request.form.get("password", "")
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    if hmac.compare_digest(pw_hash, DASH_PASSWORD_HASH):
        ts = str(int(time.time()))
        sig = _sign_token(ts)
        resp = make_response(redirect("/"))
        resp.set_cookie("bdouin_auth", f"{ts}:{sig}", max_age=COOKIE_MAX_AGE,
                        httponly=True, samesite="Lax", secure=True)
        return resp
    else:
        err_page = LOGIN_HTML.replace("ERRSCRIPT",
            '<script>document.getElementById("err").style.display="block"</script>')
        return err_page, 401


@app.route("/logout")
def logout():
    resp = make_response(redirect("/login"))
    resp.delete_cookie("bdouin_auth")
    return resp


@app.route("/")
@require_auth
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/presta/<path:path>")
@require_auth
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
@require_auth
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
@require_auth
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


@app.route("/api/notion/imak")
@require_auth
def notion_imak():
    """Fetch IMAK Print Tracker data from Notion."""
    if not NOTION_API_KEY:
        return jsonify({"error": "NOTION_API_KEY not configured"}), 503

    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }

    try:
        # Query all pages from the IMAK database
        all_results = []
        has_more = True
        start_cursor = None

        while has_more:
            body = {"page_size": 100}
            if start_cursor:
                body["start_cursor"] = start_cursor

            resp = requests.post(
                f"https://api.notion.com/v1/databases/{NOTION_IMAK_DB}/query",
                headers=headers,
                json=body,
                timeout=30,
            )
            if not resp.ok:
                return jsonify({"error": f"Notion API error: {resp.status_code}"}), 502

            data = resp.json()
            all_results.extend(data.get("results", []))
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")

        # Parse each page into clean JSON
        items = []
        for page in all_results:
            props = page.get("properties", {})

            def get_title(p):
                t = p.get("title", [])
                return t[0]["plain_text"] if t else ""

            def get_select(p):
                s = p.get("select")
                return s["name"] if s else ""

            def get_number(p):
                return p.get("number")

            def get_date(p):
                d = p.get("date")
                return d["start"] if d else None

            def get_text(p):
                rt = p.get("rich_text", [])
                return rt[0]["plain_text"] if rt else ""

            title = get_title(props.get("Title", {}))
            imak_status = get_select(props.get("IMAK Status", {}))
            item_type = get_select(props.get("Type", {}))
            language = get_select(props.get("Language", {}))
            stock_status = get_select(props.get("Stock Status", {}))

            items.append({
                "title": title,
                "type": "NEW" if "NEW" in item_type else ("REPRINT" if "REPRINT" in item_type else item_type),
                "imakStatus": imak_status,
                "qtyOrdered": get_number(props.get("Qty Ordered", {})),
                "unitPrice": get_number(props.get("Unit Price EUR", {})),
                "totalPrice": get_number(props.get("Total Price EUR", {})),
                "eta": get_date(props.get("ETA", {})),
                "productionStart": get_date(props.get("🏭 Production Start", {})),
                "reception": get_date(props.get("📦 Reception", {})),
                "exw": get_date(props.get("📦 EXW", {})),
                "filesSent": get_date(props.get("📁 Files Sent", {})),
                "language": language or "FR",
                "stockStatus": stock_status,
                "stockFeb26": get_number(props.get("Stock Feb.26", {})),
                "notes": get_text(props.get("Notes", {})),
            })

        return jsonify({"items": items, "count": len(items)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

import os
import json
import hashlib
import hmac
import time
import secrets
import calendar
from datetime import datetime, timedelta, timezone
import requests
try:
    import jwt as _jwt_module
    jwt = _jwt_module
except ImportError:
    jwt = None
from functools import wraps
from flask import Flask, request, Response, send_from_directory, jsonify, make_response, redirect

app = Flask(__name__, static_folder="static")


@app.after_request
def _no_index(resp):
    # Bloque tous les moteurs d'indexation (Google, Bing, GPTBot, etc.)
    resp.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive, nosnippet, noimageindex"
    return resp


@app.route("/robots.txt")
def robots():
    return Response(
        "User-agent: *\nDisallow: /\n",
        mimetype="text/plain",
    )


PRESTA_BASE = "https://www.bdouin.com/api"
PRESTA_KEY = "AU83IAKGBTE3SRAIW85IFLZ8642AXQPH"
MAILERLITE_BASE = "https://api.mailerlite.com/api/v2"
ML_KEY = os.environ.get("ML_KEY", "19bfaa983463fdcd6c354ec1954df7cc")
ML_HEADERS = lambda: {"X-MailerLite-ApiKey": ML_KEY, "Content-Type": "application/json"}
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "")

# Auth
DASH_PASSWORD_HASH = "1c27c98794afb7eac2f413673a8900ee7684fcafec7c7df53235f836db7e8a29"
COOKIE_SECRET = os.environ.get("COOKIE_SECRET", secrets.token_hex(32))
COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days
SUMMARY_API_KEY = os.environ.get("SUMMARY_API_KEY", "")


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


def require_auth_or_key(f):
    """Accept either a valid cookie or X-API-Key header matching SUMMARY_API_KEY."""
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get("X-API-Key", "")
        if SUMMARY_API_KEY and hmac.compare_digest(api_key, SUMMARY_API_KEY):
            return f(*args, **kwargs)
        if _check_auth():
            return f(*args, **kwargs)
        return jsonify({"error": "unauthorized"}), 401
    return decorated


LOGIN_HTML = """<!DOCTYPE html>
<html lang="fr"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="robots" content="noindex, nofollow, noarchive, nosnippet">
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


@app.route("/roadmap")
@require_auth
def roadmap():
    return send_from_directory("static", "roadmap.html")


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


# =====================================================================
# SUMMARY ENDPOINT — aggregated KPIs for Slack agent / cron consumers
# =====================================================================

FR_MONTHS = [
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
]


def _fetch_presta_orders(from_date_iso):
    """Fetch all orders from PrestaShop since from_date_iso (YYYY-MM-DD).

    Uses PrestaShop filter[date_add]=>[YYYY-MM-DD 00:00:00] syntax.
    Paginates server-side (up to 10000 orders).
    """
    all_orders = []
    offset = 0
    page_size = 500
    display = "[id,id_customer,current_state,total_paid_tax_incl,date_add,payment]"
    date_filter = f">[{from_date_iso} 00:00:00]"

    while offset <= 10000:
        params = {
            "display": display,
            "filter[date_add]": date_filter,
            "date": "1",
            "sort": "[id_DESC]",
            "limit": f"{offset},{page_size}",
            "output_format": "JSON",
            "ws_key": PRESTA_KEY,
        }
        resp = requests.get(f"{PRESTA_BASE}/orders", params=params, timeout=30)
        if resp.status_code != 200:
            break
        batch = resp.json().get("orders", []) or []
        if not batch:
            break
        all_orders.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return all_orders


def _fetch_presta_order_details(order_ids):
    """Fetch order_details for a list of order IDs. Returns list of line items."""
    if not order_ids:
        return []

    # PrestaShop filter supports | for OR match on id_order
    id_filter = "[" + "|".join(str(i) for i in order_ids) + "]"
    params = {
        "display": "[id_order,product_id,product_name,product_quantity,unit_price_tax_incl]",
        "filter[id_order]": id_filter,
        "limit": "5000",
        "output_format": "JSON",
        "ws_key": PRESTA_KEY,
    }
    try:
        resp = requests.get(f"{PRESTA_BASE}/order_details", params=params, timeout=30)
        if resp.status_code != 200:
            return []
        return resp.json().get("order_details", []) or []
    except Exception:
        return []


def _filter_valid(orders):
    """Keep only orders in valid states (2,3,4,5) with positive amount."""
    valid = []
    for o in orders:
        try:
            state = int(o.get("current_state", 0))
            amount = float(o.get("total_paid_tax_incl") or 0)
            if state in (2, 3, 4, 5) and amount > 0:
                valid.append(o)
        except (ValueError, TypeError):
            continue
    return valid


def _parse_order_date(o):
    """Parse PrestaShop date_add (YYYY-MM-DD HH:MM:SS) to datetime."""
    try:
        return datetime.strptime(o["date_add"][:19], "%Y-%m-%d %H:%M:%S")
    except (KeyError, ValueError):
        return None


def _aggregate_period(orders, start, end):
    """Aggregate orders between start (inclusive) and end (exclusive)."""
    subset = []
    for o in orders:
        d = _parse_order_date(o)
        if d and start <= d < end:
            subset.append(o)

    revenue = sum(float(o.get("total_paid_tax_incl") or 0) for o in subset)
    count = len(subset)
    avg = round(revenue / count, 2) if count else 0
    return {
        "revenue": round(revenue, 2),
        "orders": count,
        "avgTicket": avg,
    }, subset


def _pct_diff(current, previous):
    if not previous:
        return None
    return round((current - previous) / previous * 100, 1)


def _best_sellers(details, valid_order_ids):
    """Return top 10 products by qty, summed across valid orders."""
    valid_set = set(int(i) for i in valid_order_ids)
    by_product = {}
    for d in details:
        try:
            if int(d.get("id_order", 0)) not in valid_set:
                continue
            pid = d.get("product_id")
            name = d.get("product_name", "—")
            qty = int(d.get("product_quantity") or 0)
            price = float(d.get("unit_price_tax_incl") or 0)
        except (ValueError, TypeError):
            continue
        key = (pid, name)
        if key not in by_product:
            by_product[key] = {"productId": pid, "name": name, "qty": 0, "revenue": 0.0}
        by_product[key]["qty"] += qty
        by_product[key]["revenue"] += qty * price

    items = list(by_product.values())
    for it in items:
        it["revenue"] = round(it["revenue"], 2)
    items.sort(key=lambda x: x["qty"], reverse=True)
    return items[:10]


def _payment_methods(orders):
    counts = {}
    for o in orders:
        p = (o.get("payment") or "inconnu").strip().lower()
        counts[p] = counts.get(p, 0) + 1
    return counts


@app.route("/api/summary")
@require_auth_or_key
def summary():
    """Aggregated KPI summary for the current month, yesterday, today, YoY.

    Auth: either dashboard cookie OR X-API-Key header matching SUMMARY_API_KEY.
    Designed for Slack agent / cron consumers.
    """
    try:
        now = datetime.now()
        today_start = datetime(now.year, now.month, now.day)
        tomorrow_start = today_start + timedelta(days=1)
        yesterday_start = today_start - timedelta(days=1)

        current_month_start = datetime(now.year, now.month, 1)
        last_month = 12 if now.month == 1 else now.month - 1
        last_month_year = now.year - 1 if now.month == 1 else now.year
        last_month_start = datetime(last_month_year, last_month, 1)
        last_month_end = current_month_start

        last_year_month_start = datetime(now.year - 1, now.month, 1)
        next_month = 1 if now.month == 12 else now.month + 1
        next_month_year_ly = now.year if now.month == 12 else now.year - 1
        last_year_month_end = datetime(next_month_year_ly, next_month, 1)

        # Fetch window: last year same month start → now
        from_date = last_year_month_start.strftime("%Y-%m-%d")
        all_orders = _fetch_presta_orders(from_date)
        valid_orders = _filter_valid(all_orders)

        # Period aggregates
        yest_agg, yest_subset = _aggregate_period(valid_orders, yesterday_start, today_start)
        today_agg, _ = _aggregate_period(valid_orders, today_start, tomorrow_start)
        cm_agg, cm_subset = _aggregate_period(valid_orders, current_month_start, tomorrow_start)
        lm_agg, _ = _aggregate_period(valid_orders, last_month_start, last_month_end)
        ly_agg, _ = _aggregate_period(valid_orders, last_year_month_start, last_year_month_end)

        # Current month details
        days_in_month = calendar.monthrange(now.year, now.month)[1]
        days_elapsed = now.day
        if days_elapsed > 0:
            forecast_revenue = round(cm_agg["revenue"] / days_elapsed * days_in_month, 2)
            forecast_orders = int(cm_agg["orders"] / days_elapsed * days_in_month)
        else:
            forecast_revenue = 0
            forecast_orders = 0

        # Best sellers for current month
        cm_order_ids = [o.get("id") for o in cm_subset]
        details = _fetch_presta_order_details(cm_order_ids) if cm_order_ids else []
        bests = _best_sellers(details, cm_order_ids)

        # Payment methods (current month)
        payments = _payment_methods(cm_subset)

        result = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "currency": "EUR",
            "yesterday": yest_agg,
            "today": today_agg,
            "currentMonth": {
                "label": f"{FR_MONTHS[now.month - 1]} {now.year}",
                **cm_agg,
                "daysElapsed": days_elapsed,
                "daysInMonth": days_in_month,
                "forecast": {
                    "revenue": forecast_revenue,
                    "orders": forecast_orders,
                },
            },
            "lastMonth": {
                "label": f"{FR_MONTHS[last_month - 1]} {last_month_year}",
                **lm_agg,
            },
            "lastYearMonth": {
                "label": f"{FR_MONTHS[now.month - 1]} {now.year - 1}",
                **ly_agg,
            },
            "comparison": {
                "vsLastMonth": {
                    "revenuePct": _pct_diff(cm_agg["revenue"], lm_agg["revenue"]),
                    "ordersPct": _pct_diff(cm_agg["orders"], lm_agg["orders"]),
                },
                "vsYoY": {
                    "revenuePct": _pct_diff(cm_agg["revenue"], ly_agg["revenue"]),
                    "ordersPct": _pct_diff(cm_agg["orders"], ly_agg["orders"]),
                },
            },
            "bestSellers": bests,
            "paymentMethods": payments,
        }

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =====================================================================
# GA4 MULTI-PROPERTY ENDPOINT — 7-day KPIs for all configured properties
# =====================================================================
#
# Config env var: GA4_PROPERTIES = "Name1:ID1,Name2:ID2"
# (ex: "BDouin Shop:382670810,HooPow:261456373")
# Credentials: GA4_CREDENTIALS_JSON (full service account JSON)

def _ga4_query_property(client, prop_id, days=7):
    """Fetch basic 7-day KPIs for a single property."""
    from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
    try:
        # Core KPIs
        kpi_req = RunReportRequest(
            property=f"properties/{prop_id}",
            date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="newUsers"),
                Metric(name="eventCount"),
                Metric(name="screenPageViews"),
            ],
        )
        r = client.run_report(kpi_req)
        row = r.rows[0] if r.rows else None

        # Top 5 countries
        country_req = RunReportRequest(
            property=f"properties/{prop_id}",
            date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
            dimensions=[Dimension(name="country")],
            metrics=[Metric(name="activeUsers")],
            limit=5,
        )
        cr = client.run_report(country_req)

        return {
            "activeUsers": int(row.metric_values[0].value) if row else 0,
            "sessions": int(row.metric_values[1].value) if row else 0,
            "newUsers": int(row.metric_values[2].value) if row else 0,
            "eventCount": int(row.metric_values[3].value) if row else 0,
            "screenPageViews": int(row.metric_values[4].value) if row else 0,
            "topCountries": [
                {"country": c.dimension_values[0].value, "users": int(c.metric_values[0].value)}
                for c in cr.rows
            ],
        }
    except Exception as e:
        return {"error": str(e)[:200]}


@app.route("/api/ga4-multi")
@require_auth_or_key
def ga4_multi():
    """Fetch 7-day KPIs for all configured GA4 properties in parallel.

    Auth: cookie OR X-API-Key. Designed for Slack agent + dashboard.
    Env: GA4_PROPERTIES (CSV Name:ID), GA4_CREDENTIALS_JSON.
    """
    ga4_creds = os.environ.get("GA4_CREDENTIALS_JSON", "")
    ga4_props_cfg = os.environ.get("GA4_PROPERTIES", "")
    if not ga4_creds or not ga4_props_cfg:
        return jsonify({"error": "GA4_CREDENTIALS_JSON or GA4_PROPERTIES not configured"}), 503

    # Parse "Name:ID,Name:ID"
    properties = []
    for part in ga4_props_cfg.split(","):
        part = part.strip()
        if ":" in part:
            name, pid = part.split(":", 1)
            properties.append({"name": name.strip(), "propertyId": pid.strip()})
    if not properties:
        return jsonify({"error": "GA4_PROPERTIES is empty"}), 503

    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.oauth2 import service_account
        from concurrent.futures import ThreadPoolExecutor

        creds = service_account.Credentials.from_service_account_info(
            json.loads(ga4_creds),
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        client = BetaAnalyticsDataClient(credentials=creds)

        # Days window: default 7, override via ?days=30
        try:
            days = max(1, min(365, int(request.args.get("days", "7"))))
        except ValueError:
            days = 7

        # Query each property in parallel
        def work(prop):
            return {**prop, "data": _ga4_query_property(client, prop["propertyId"], days=days)}

        with ThreadPoolExecutor(max_workers=min(6, len(properties))) as ex:
            results = list(ex.map(work, properties))

        return jsonify({
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "periodDays": days,
            "properties": results,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =====================================================================
# APP STORE + GOOGLE PLAY REVIEWS — voix du public sur les apps
# =====================================================================
#
# Collecte iOS (App Store Connect API officielle, fallback RSS) + Android (google-play-scraper).
# Cache mémoire 24h + refresh via APScheduler au démarrage + toutes les 24h.

APPS = [
    {"name": "Awlad School",         "iosId": "1612014910", "androidPkg": "com.bdouin.awladschool"},
    {"name": "Awlad Quiz GO",        "iosId": "6737732771", "androidPkg": "com.bdouin.awladquiz"},
    {"name": "Awlad Classroom",      "iosId": "6754524897", "androidPkg": "com.bdouin.awladclassroom"},
    {"name": "Awlad Salat",          "iosId": "1669010427", "androidPkg": "com.bdouin.awladsalat"},
    {"name": "Awlad Coran",          "iosId": "6477914472", "androidPkg": "com.bdouin.awladquran"},
    {"name": "BDouin Magazine",      "iosId": "6472446290", "androidPkg": "com.bdouin.mag"},
    {"name": "BDouin Maker",         "iosId": "946822771",  "androidPkg": "mobapp.at.lebdouin"},
    {"name": "BDouin Stories",       "iosId": "6479290376", "androidPkg": "com.bdouin.apps.muslimstrips"},
]

# App Store Connect API — clé équipe Admin (AA77LF9WN8)
ASC_KEY_ID      = os.environ.get("ASC_KEY_ID", "")
ASC_ISSUER_ID   = os.environ.get("ASC_ISSUER_ID", "")
ASC_PRIVATE_KEY = os.environ.get("ASC_PRIVATE_KEY", "").replace("\\n", "\n")

# ISO alpha-3 → alpha-2 pour les territoires ASC
_ASC_TERRITORY_MAP = {
    "FRA":"fr","CAN":"ca","BEL":"be","CHE":"ch","MAR":"ma","DZA":"dz","TUN":"tn",
    "SEN":"sn","CIV":"ci","SAU":"sa","ARE":"ae","QAT":"qa","KWT":"kw","EGY":"eg",
    "JOR":"jo","LBN":"lb","TUR":"tr","GBR":"gb","USA":"us","IDN":"id","MYS":"my",
    "PAK":"pk","DEU":"de","ESP":"es","ITA":"it","NLD":"nl","BRA":"br","MEX":"mx",
}

# Stores iOS : principaux marchés francophones + musulmans (RSS limité ~500 reviews par pays)
IOS_COUNTRIES = ["fr", "ca", "be", "ch", "ma", "dz", "tn", "sn", "ci", "sa", "ae", "qa", "kw",
                 "eg", "jo", "lb", "tr", "gb", "us", "id", "my", "pk"]
# Android pagination max count par app/country
ANDROID_COUNTRIES = ["fr", "ma", "dz", "tn", "sa", "ae", "eg", "us", "gb", "be", "ca"]
ANDROID_COUNT = 2000  # up to 2000 per country

REVIEWS_CACHE = {"data": None, "fetchedAt": None}
REVIEWS_PERSIST_PATH = os.environ.get("REVIEWS_CACHE_PATH", "/tmp/reviews_cache.json")


# =====================================================================
# POSTGRES — persistent storage for reviews + web mentions + raw data
# =====================================================================

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()


def _db_conn():
    """Get a psycopg2 connection. Returns None if DATABASE_URL not set."""
    if not DATABASE_URL:
        return None
    try:
        import psycopg2
        return psycopg2.connect(DATABASE_URL, connect_timeout=10)
    except Exception as e:
        print(f"[db] connect failed: {e}")
        return None


def _db_migrate():
    """Create tables if they don't exist. Idempotent."""
    conn = _db_conn()
    if not conn:
        return False
    try:
        with conn, conn.cursor() as cur:
            # reviews — one row per review, dedupe on (store, store_review_id)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reviews (
                    id              TEXT PRIMARY KEY,
                    app             TEXT NOT NULL,
                    store           TEXT NOT NULL,
                    country         TEXT,
                    rating          INT NOT NULL,
                    title           TEXT,
                    content         TEXT,
                    version         TEXT,
                    author          TEXT,
                    review_date     DATE,
                    thumbs_up       INT DEFAULT 0,
                    reply_content   TEXT,
                    first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
                    last_seen_at    TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_reviews_app      ON reviews(app);
                CREATE INDEX IF NOT EXISTS idx_reviews_store    ON reviews(store);
                CREATE INDEX IF NOT EXISTS idx_reviews_date     ON reviews(review_date DESC);
                CREATE INDEX IF NOT EXISTS idx_reviews_rating   ON reviews(rating);
                CREATE INDEX IF NOT EXISTS idx_reviews_country  ON reviews(country);
            """)
            # web_mentions — pour la veille web (Google Alerts, Reddit, Twitter, etc.)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS web_mentions (
                    id              TEXT PRIMARY KEY,
                    source          TEXT NOT NULL,
                    url             TEXT,
                    title           TEXT,
                    snippet         TEXT,
                    author          TEXT,
                    mention_date    DATE,
                    keyword         TEXT,
                    sentiment       REAL,
                    raw             JSONB,
                    first_seen_at   TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_mentions_source ON web_mentions(source);
                CREATE INDEX IF NOT EXISTS idx_mentions_date   ON web_mentions(mention_date DESC);
                CREATE INDEX IF NOT EXISTS idx_mentions_kw     ON web_mentions(keyword);
            """)
            # raw_sources — fourre-tout pour scraps divers (forums, reviews site, etc.)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_sources (
                    id              BIGSERIAL PRIMARY KEY,
                    source          TEXT NOT NULL,
                    url             TEXT,
                    payload         JSONB NOT NULL,
                    collected_at    TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_raw_source ON raw_sources(source);
                CREATE INDEX IF NOT EXISTS idx_raw_date   ON raw_sources(collected_at DESC);
            """)
            # PrestaShop — paniers abandonnés
            cur.execute("""
                CREATE TABLE IF NOT EXISTS presta_abandoned_carts (
                    cart_id         INT PRIMARY KEY,
                    id_customer     INT DEFAULT 0,
                    id_guest        INT DEFAULT 0,
                    date_add        TIMESTAMPTZ,
                    date_upd        TIMESTAMPTZ,
                    nb_products     INT DEFAULT 0,
                    total_estimated REAL DEFAULT 0,
                    products        JSONB,
                    collected_at    TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_abandoned_date    ON presta_abandoned_carts(date_add DESC);
                CREATE INDEX IF NOT EXISTS idx_abandoned_total   ON presta_abandoned_carts(total_estimated DESC);
                CREATE INDEX IF NOT EXISTS idx_abandoned_products ON presta_abandoned_carts USING gin(products);
            """)
            # GA4 Ads — campagnes
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ga4_ads_campaigns (
                    date            DATE NOT NULL,
                    campaign_name   TEXT NOT NULL,
                    campaign_type   TEXT,
                    sessions        INT DEFAULT 0,
                    conversions     INT DEFAULT 0,
                    engagement_rate REAL DEFAULT 0,
                    collected_at    TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (date, campaign_name)
                );
                CREATE INDEX IF NOT EXISTS idx_ga4_camps_date ON ga4_ads_campaigns(date DESC);
            """)
            # GA4 Ads — mots-clés
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ga4_ads_keywords (
                    date            DATE NOT NULL,
                    keyword         TEXT NOT NULL,
                    sessions        INT DEFAULT 0,
                    conversions     INT DEFAULT 0,
                    engagement_rate REAL DEFAULT 0,
                    collected_at    TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (date, keyword)
                );
                CREATE INDEX IF NOT EXISTS idx_ga4_kw_date ON ga4_ads_keywords(date DESC);
            """)
            # GA4 Ads — landing pages
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ga4_ads_landing_pages (
                    date            DATE NOT NULL,
                    landing_page    TEXT NOT NULL,
                    campaign_name   TEXT NOT NULL,
                    sessions        INT DEFAULT 0,
                    conversions     INT DEFAULT 0,
                    collected_at    TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (date, landing_page, campaign_name)
                );
                CREATE INDEX IF NOT EXISTS idx_ga4_lp_date ON ga4_ads_landing_pages(date DESC);
            """)
            # MailerLite — groupes/segments
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ml_groups (
                    id              BIGINT PRIMARY KEY,
                    name            TEXT,
                    total           INT DEFAULT 0,
                    active          INT DEFAULT 0,
                    unsubscribed    INT DEFAULT 0,
                    bounced         INT DEFAULT 0,
                    sent            INT DEFAULT 0,
                    opened          INT DEFAULT 0,
                    clicked         INT DEFAULT 0,
                    date_created    TIMESTAMPTZ,
                    date_updated    TIMESTAMPTZ,
                    collected_at    TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            # MailerLite — campagnes envoyées
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ml_campaigns (
                    id                  BIGINT PRIMARY KEY,
                    name                TEXT,
                    subject             TEXT,
                    type                TEXT,
                    status              TEXT,
                    date_send           TIMESTAMPTZ,
                    total_recipients    INT DEFAULT 0,
                    opened_count        INT DEFAULT 0,
                    opened_rate         REAL DEFAULT 0,
                    clicked_count       INT DEFAULT 0,
                    clicked_rate        REAL DEFAULT 0,
                    unsubscribed        INT DEFAULT 0,
                    bounced             INT DEFAULT 0,
                    html_content        TEXT,
                    plain_text          TEXT,
                    collected_at        TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_ml_campaigns_date ON ml_campaigns(date_send DESC);
            """)
            # MailerLite — abonnés (777k lignes, collecte progressive)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ml_subscribers (
                    id              BIGINT PRIMARY KEY,
                    email           TEXT UNIQUE,
                    name            TEXT,
                    status          TEXT,
                    country         TEXT,
                    city            TEXT,
                    language        TEXT,
                    signup_ip       TEXT,
                    date_subscribe  TIMESTAMPTZ,
                    date_unsubscribe TIMESTAMPTZ,
                    fields          JSONB,
                    groups          JSONB,
                    collected_at    TIMESTAMPTZ DEFAULT NOW(),
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_ml_subs_status  ON ml_subscribers(status);
                CREATE INDEX IF NOT EXISTS idx_ml_subs_country ON ml_subscribers(country);
                CREATE INDEX IF NOT EXISTS idx_ml_subs_date    ON ml_subscribers(date_subscribe DESC);
            """)
            # Sofiadis B2B — ventes distributeur (ligne par ligne, mois × titre)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sofiadis_b2b_sales (
                    id              BIGSERIAL PRIMARY KEY,
                    period          DATE NOT NULL,
                    title           TEXT NOT NULL,
                    ean             TEXT,
                    qty_sold        INT DEFAULT 0,
                    qty_returned    INT DEFAULT 0,
                    net_qty         INT DEFAULT 0,
                    price_ht        NUMERIC(10,4) DEFAULT 0,
                    total_ht        NUMERIC(10,2) DEFAULT 0,
                    source          TEXT DEFAULT 'sofiadis_releve',
                    collected_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (period, title)
                );
                CREATE INDEX IF NOT EXISTS idx_sof_b2b_period ON sofiadis_b2b_sales(period DESC);
                CREATE INDEX IF NOT EXISTS idx_sof_b2b_title  ON sofiadis_b2b_sales(title);
            """)
            # Sofiadis Logistique — frais mensuels bdouin.com
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sofiadis_logistics (
                    id              BIGSERIAL PRIMARY KEY,
                    period          DATE NOT NULL UNIQUE,
                    amount_ht       NUMERIC(10,2) DEFAULT 0,
                    invoice_ref     TEXT,
                    status          TEXT DEFAULT 'unknown',
                    source          TEXT DEFAULT 'sofiadis_facture',
                    collected_at    TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_sof_log_period ON sofiadis_logistics(period DESC);
            """)
            # IMAK — commandes d'impression (ligne par ligne, titre × date impression)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS imak_print_orders (
                    id              BIGSERIAL PRIMARY KEY,
                    print_date      DATE NOT NULL,
                    title           TEXT NOT NULL,
                    qty             INT DEFAULT 0,
                    unit_cost_eur   NUMERIC(10,4) DEFAULT 0,
                    total_cost_eur  NUMERIC(10,2) DEFAULT 0,
                    invoice_ref     TEXT,
                    period          TEXT,
                    status          TEXT DEFAULT 'unknown',
                    collected_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (print_date, title, invoice_ref)
                );
                CREATE INDEX IF NOT EXISTS idx_imak_date  ON imak_print_orders(print_date DESC);
                CREATE INDEX IF NOT EXISTS idx_imak_title ON imak_print_orders(title);
            """)
        return True
    except Exception as e:
        print(f"[db] migrate failed: {e}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _db_insert_reviews(app_name, reviews_list):
    """Upsert reviews into DB. Dedupe on id. Returns (inserted, updated)."""
    conn = _db_conn()
    if not conn:
        return (0, 0)
    inserted = updated = 0
    try:
        with conn, conn.cursor() as cur:
            for r in reviews_list:
                rid = r.get("id") or _review_id(r)
                rdate = r.get("date") or None
                if rdate and len(rdate) < 10:
                    rdate = None
                cur.execute("""
                    INSERT INTO reviews (id, app, store, country, rating, title, content,
                                         version, author, review_date, thumbs_up, reply_content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        last_seen_at = NOW(),
                        rating       = EXCLUDED.rating,
                        content      = EXCLUDED.content,
                        reply_content= EXCLUDED.reply_content
                    RETURNING (xmax = 0) AS was_inserted
                """, (rid, app_name, r.get("store",""), r.get("country",""), r.get("rating",0),
                      r.get("title",""), r.get("content",""), r.get("version",""),
                      r.get("author",""), rdate, r.get("thumbsUp",0), r.get("replyContent","")))
                was_inserted = cur.fetchone()[0]
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1
        return (inserted, updated)
    except Exception as e:
        print(f"[db] insert reviews failed: {e}")
        return (inserted, updated)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _db_stats():
    """Return overall DB stats."""
    conn = _db_conn()
    if not conn:
        return {"error": "no DATABASE_URL"}
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM reviews")
            n_reviews = cur.fetchone()[0]
            cur.execute("SELECT app, store, COUNT(*), ROUND(AVG(rating)::numeric, 2) FROM reviews GROUP BY app, store ORDER BY app, store")
            by_app = [{"app": r[0], "store": r[1], "count": r[2], "avgRating": float(r[3]) if r[3] else 0} for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*) FROM web_mentions")
            n_mentions = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM raw_sources")
            n_raw = cur.fetchone()[0]
        return {
            "reviews": n_reviews,
            "reviewsByApp": by_app,
            "webMentions": n_mentions,
            "rawSources": n_raw,
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _review_id(r):
    """Dédup key : store + country + author + date + content head."""
    h = hashlib.sha256()
    h.update(f"{r.get('store','')}|{r.get('country','')}|{r.get('author','')}|{r.get('date','')}|{(r.get('content','') or '')[:100]}".encode("utf-8"))
    return h.hexdigest()[:16]


def _asc_jwt():
    """Generate a 20-min App Store Connect JWT. Returns None if credentials missing."""
    if not (jwt and ASC_KEY_ID and ASC_ISSUER_ID and ASC_PRIVATE_KEY):
        return None
    try:
        payload = {"iss": ASC_ISSUER_ID, "exp": int(time.time()) + 1200, "aud": "appstoreconnect-v1"}
        return jwt.encode(payload, ASC_PRIVATE_KEY, algorithm="ES256",
                          headers={"kid": ASC_KEY_ID, "typ": "JWT"})
    except Exception as e:
        print(f"[asc] JWT error: {e}")
        return None


def _fetch_ios_reviews_asc(app_id):
    """Fetch ALL iOS reviews via App Store Connect API (no country/page limit)."""
    token = _asc_jwt()
    if not token:
        return None
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.appstoreconnect.apple.com/v1/apps/{app_id}/customerReviews"
    params = {"sort": "-createdDate", "limit": 200}
    out = []
    seen = set()
    while url:
        try:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            params = {}  # only on first request
            if r.status_code != 200:
                print(f"[asc] {app_id} HTTP {r.status_code}: {r.text[:200]}")
                return None
            body = r.json()
            for item in body.get("data", []):
                a = item.get("attributes", {})
                territory = a.get("territory", "")
                country = _ASC_TERRITORY_MAP.get(territory, territory.lower()[:2])
                content = a.get("body") or ""
                author = a.get("reviewerNickname") or ""
                date = (a.get("createdDate") or "")[:10]
                dedup = f"{country}|{author}|{date}|{content[:100]}"
                if dedup in seen:
                    continue
                seen.add(dedup)
                out.append({
                    "store": "ios",
                    "country": country,
                    "rating": int(a.get("rating") or 0),
                    "title": a.get("title") or "",
                    "content": content,
                    "version": "",
                    "author": author,
                    "date": date,
                })
            url = body.get("links", {}).get("next")
        except Exception as e:
            print(f"[asc] {app_id} error: {e}")
            return None
    print(f"[asc] {app_id}: {len(out)} reviews fetched")
    return out


def _fetch_ios_reviews_country(app_id, country, pages=10):
    out = []
    for page in range(1, pages + 1):
        url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"
        try:
            r = requests.get(url, timeout=15)
            if r.status_code != 200:
                break
            d = r.json()
            entries = d.get("feed", {}).get("entry", [])
            if isinstance(entries, dict):
                entries = [entries]
            batch = []
            for e in entries:
                if "im:rating" not in e:
                    continue
                batch.append({
                    "store": "ios",
                    "country": country,
                    "rating": int(e["im:rating"]["label"]),
                    "title": e.get("title", {}).get("label", ""),
                    "content": e.get("content", {}).get("label", ""),
                    "version": e.get("im:version", {}).get("label", ""),
                    "author": e.get("author", {}).get("name", {}).get("label", ""),
                    "date": e.get("updated", {}).get("label", "")[:10],
                })
            if not batch:
                break
            out.extend(batch)
        except Exception:
            break
    return out


def _fetch_ios_reviews(app_id):
    """Pull iOS reviews — ASC API preferred (no limit), fallback to RSS."""
    asc = _fetch_ios_reviews_asc(app_id)
    if asc is not None:
        for r in asc:
            r["id"] = _review_id(r)
        return asc
    # Fallback: RSS public (limité ~500/pays)
    seen = set()
    all_reviews = []
    for country in IOS_COUNTRIES:
        batch = _fetch_ios_reviews_country(app_id, country)
        for r in batch:
            rid = _review_id(r)
            if rid in seen:
                continue
            seen.add(rid)
            r["id"] = rid
            all_reviews.append(r)
    return all_reviews


def _fetch_android_reviews_country(package, country, lang, count):
    try:
        from google_play_scraper import reviews as gps_reviews, Sort
        result, _ = gps_reviews(package, lang=lang, country=country,
                                sort=Sort.NEWEST, count=count)
        return [{
            "store": "android",
            "country": country,
            "rating": r.get("score") or 0,
            "title": "",
            "content": r.get("content") or "",
            "version": r.get("reviewCreatedVersion") or "",
            "author": r.get("userName") or "",
            "date": r.get("at").strftime("%Y-%m-%d") if r.get("at") else "",
            "thumbsUp": r.get("thumbsUpCount") or 0,
            "replyContent": r.get("replyContent") or "",
        } for r in result]
    except Exception:
        return []


def _fetch_android_reviews(package):
    """Pull Android reviews from all configured countries, dedup."""
    seen = set()
    all_reviews = []
    for country in ANDROID_COUNTRIES:
        batch = _fetch_android_reviews_country(package, country, "fr", ANDROID_COUNT)
        for r in batch:
            rid = _review_id(r)
            if rid in seen:
                continue
            seen.add(rid)
            r["id"] = rid
            all_reviews.append(r)
    return all_reviews


def _compute_stats(reviews_list):
    """Aggregate: count by rating, avg, last 30d vs previous 30d trend."""
    if not reviews_list:
        return {"total": 0, "avg": 0, "byRating": {}, "last30d": {}, "prev30d": {}}
    counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    total_score = 0
    today = datetime.now(timezone.utc).date()
    cutoff_30 = today - timedelta(days=30)
    cutoff_60 = today - timedelta(days=60)
    last30 = []
    prev30 = []
    for r in reviews_list:
        rating = r.get("rating", 0)
        if rating in counts:
            counts[rating] += 1
            total_score += rating
        try:
            d = datetime.strptime(r.get("date", "1970-01-01"), "%Y-%m-%d").date()
            if d >= cutoff_30:
                last30.append(rating)
            elif d >= cutoff_60:
                prev30.append(rating)
        except Exception:
            pass
    avg = round(total_score / sum(counts.values()), 2) if sum(counts.values()) > 0 else 0
    def _avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else 0
    return {
        "total": sum(counts.values()),
        "avg": avg,
        "byRating": counts,
        "last30d": {"count": len(last30), "avg": _avg(last30)},
        "prev30d": {"count": len(prev30), "avg": _avg(prev30)},
    }


def _country_breakdown(reviews_list):
    """Breakdown by country : count + avg rating."""
    by_country = {}
    for r in reviews_list:
        c = r.get("country", "?")
        if c not in by_country:
            by_country[c] = {"count": 0, "sum": 0}
        by_country[c]["count"] += 1
        by_country[c]["sum"] += r.get("rating", 0)
    return [
        {"country": c, "count": v["count"], "avg": round(v["sum"] / v["count"], 2) if v["count"] else 0}
        for c, v in sorted(by_country.items(), key=lambda x: -x[1]["count"])
    ]


# =====================================================================
# MAILERLITE — collecte groupes, campagnes, abonnés
# =====================================================================

def _ml_collect_groups():
    """Collecte les 39 groupes MailerLite et les upserte en DB."""
    try:
        r = requests.get(f"{MAILERLITE_BASE}/groups", headers=ML_HEADERS(),
                         params={"limit": 100}, timeout=20)
        if r.status_code != 200:
            print(f"[ml] groups HTTP {r.status_code}")
            return 0
        groups = r.json()
        conn = _db_conn()
        if not conn:
            return 0
        with conn, conn.cursor() as cur:
            for g in groups:
                cur.execute("""
                    INSERT INTO ml_groups (id, name, total, active, unsubscribed, bounced,
                                           sent, opened, clicked, date_created, date_updated, collected_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        name=EXCLUDED.name, total=EXCLUDED.total, active=EXCLUDED.active,
                        unsubscribed=EXCLUDED.unsubscribed, bounced=EXCLUDED.bounced,
                        sent=EXCLUDED.sent, opened=EXCLUDED.opened, clicked=EXCLUDED.clicked,
                        date_updated=EXCLUDED.date_updated, collected_at=NOW()
                """, (
                    g["id"], g.get("name"), g.get("total",0), g.get("active",0),
                    g.get("unsubscribed",0), g.get("bounced",0),
                    g.get("sent",0), g.get("opened",0), g.get("clicked",0),
                    g.get("date_created"), g.get("date_updated")
                ))
        conn.close()
        print(f"[ml] groups: {len(groups)} upserted")
        return len(groups)
    except Exception as e:
        print(f"[ml] groups error: {e}")
        return 0


def _ml_collect_campaigns():
    """Collecte toutes les campagnes envoyées avec stats + contenu HTML."""
    try:
        all_campaigns = []
        offset = 0
        limit = 100
        while True:
            r = requests.get(f"{MAILERLITE_BASE}/campaigns/sent", headers=ML_HEADERS(),
                             params={"limit": limit, "offset": offset}, timeout=20)
            if r.status_code != 200:
                print(f"[ml] campaigns HTTP {r.status_code}")
                break
            batch = r.json()
            if not batch:
                break
            all_campaigns.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        if not all_campaigns:
            return 0
        conn = _db_conn()
        if not conn:
            return 0
        with conn, conn.cursor() as cur:
            for c in all_campaigns:
                # Fetch HTML content for this campaign
                html_content = plain_text = None
                try:
                    cr = requests.get(f"{MAILERLITE_BASE}/campaigns/{c['id']}",
                                      headers=ML_HEADERS(), timeout=10)
                    if cr.status_code == 200:
                        cd = cr.json()
                        mails = cd.get("mails") or []
                        if mails:
                            html_content = mails[0].get("html") or mails[0].get("body")
                            plain_text   = mails[0].get("plain_text") or mails[0].get("text")
                except Exception:
                    pass
                cur.execute("""
                    INSERT INTO ml_campaigns (id, name, subject, type, status, date_send,
                        total_recipients, opened_count, opened_rate, clicked_count, clicked_rate,
                        unsubscribed, bounced, html_content, plain_text, collected_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        name=EXCLUDED.name, subject=EXCLUDED.subject,
                        total_recipients=EXCLUDED.total_recipients,
                        opened_count=EXCLUDED.opened_count, opened_rate=EXCLUDED.opened_rate,
                        clicked_count=EXCLUDED.clicked_count, clicked_rate=EXCLUDED.clicked_rate,
                        unsubscribed=EXCLUDED.unsubscribed, bounced=EXCLUDED.bounced,
                        html_content=COALESCE(EXCLUDED.html_content, ml_campaigns.html_content),
                        plain_text=COALESCE(EXCLUDED.plain_text, ml_campaigns.plain_text),
                        collected_at=NOW()
                """, (
                    c["id"], c.get("name"), c.get("subject"),
                    c.get("type"), c.get("status"), c.get("date_send"),
                    c.get("total_recipients", 0),
                    c.get("opened", {}).get("count", 0),
                    c.get("opened", {}).get("rate", 0),
                    c.get("clicked", {}).get("count", 0),
                    c.get("clicked", {}).get("rate", 0),
                    c.get("unsubscribed", {}).get("count", 0) if isinstance(c.get("unsubscribed"), dict) else c.get("unsubscribed", 0),
                    c.get("bounced", {}).get("count", 0) if isinstance(c.get("bounced"), dict) else c.get("bounced", 0),
                    html_content, plain_text
                ))
        conn.close()
        print(f"[ml] campaigns: {len(all_campaigns)} upserted")
        return len(all_campaigns)
    except Exception as e:
        print(f"[ml] campaigns error: {e}")
        return 0


def _ml_collect_subscribers(full=False):
    """Collecte les abonnés MailerLite en DB.

    full=True  : recharge tout (777k, ~15 min)
    full=False : collecte incrémentale — abonnés mis à jour depuis la dernière collecte
    """
    import threading
    def _run():
        try:
            # Déterminer la date de dernière collecte pour le mode incrémental
            since = None
            if not full:
                conn = _db_conn()
                if conn:
                    with conn, conn.cursor() as cur:
                        cur.execute("SELECT MAX(updated_at) FROM ml_subscribers")
                        row = cur.fetchone()
                        if row and row[0]:
                            since = row[0].strftime("%Y-%m-%d")
                    conn.close()

            offset = 0
            limit = 1000
            total = 0
            while True:
                params = {"limit": limit, "offset": offset}
                if since and not full:
                    params["filters[date_updated][from]"] = since
                r = requests.get(f"{MAILERLITE_BASE}/subscribers", headers=ML_HEADERS(),
                                 params=params, timeout=30)
                if r.status_code != 200:
                    print(f"[ml] subscribers HTTP {r.status_code} at offset {offset}")
                    break
                batch = r.json()
                if not batch:
                    break
                conn = _db_conn()
                if not conn:
                    break
                with conn, conn.cursor() as cur:
                    for s in batch:
                        fields = {f["key"]: f.get("value") for f in (s.get("fields") or [])}
                        groups = [{"id": g["id"], "name": g.get("name")} for g in (s.get("groups") or [])]
                        cur.execute("""
                            INSERT INTO ml_subscribers
                                (id, email, name, status, country, city, language, signup_ip,
                                 date_subscribe, date_unsubscribe, fields, groups, collected_at, updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                            ON CONFLICT (id) DO UPDATE SET
                                email=EXCLUDED.email, name=EXCLUDED.name, status=EXCLUDED.status,
                                country=EXCLUDED.country, city=EXCLUDED.city, language=EXCLUDED.language,
                                date_unsubscribe=EXCLUDED.date_unsubscribe,
                                fields=EXCLUDED.fields, groups=EXCLUDED.groups, updated_at=NOW()
                        """, (
                            s.get("id"), s.get("email"), s.get("name"), s.get("type"),
                            fields.get("country"), fields.get("city"), fields.get("last_name"),
                            s.get("signup_ip"),
                            s.get("date_subscribe"), s.get("date_unsubscribe"),
                            json.dumps(fields), json.dumps(groups)
                        ))
                conn.close()
                total += len(batch)
                if offset % 10000 == 0:
                    print(f"[ml] subscribers: {total} collected...")
                if len(batch) < limit:
                    break
                offset += limit
            print(f"[ml] subscribers done: {total} total")
        except Exception as e:
            print(f"[ml] subscribers thread error: {e}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return "started"


def _ml_collect_all(full_subscribers=False):
    """Lance la collecte complète MailerLite (groups + campaigns + subscribers)."""
    print("[ml] starting full collect...")
    _ml_collect_groups()
    _ml_collect_campaigns()
    _ml_collect_subscribers(full=full_subscribers)


# =====================================================================
# GA4 ADS — campagnes, mots-clés, landing pages
# =====================================================================

def _ga4_client():
    """Retourne un client GA4 Data API. None si credentials absents."""
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.oauth2 import service_account
        raw = os.environ.get("GA4_CREDENTIALS_JSON", "")
        if not raw:
            return None
        info = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        return BetaAnalyticsDataClient(credentials=creds)
    except Exception as e:
        print(f"[ga4] client error: {e}")
        return None


def _ga4_collect_ads(days=180):
    """Collecte campagnes, mots-clés et landing pages depuis GA4 Ads (BDouin Shop).

    days : fenêtre historique (défaut 180j, incrémental ensuite)
    """
    try:
        from google.analytics.data_v1beta.types import (
            RunReportRequest, Dimension, Metric, DateRange, OrderBy
        )
        client = _ga4_client()
        if not client:
            print("[ga4_ads] no credentials")
            return 0

        # Déterminer la date de début (incrémental si données existantes)
        conn = _db_conn()
        start_date = f"{days}daysAgo"
        if conn:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT MAX(date) FROM ga4_ads_campaigns")
                row = cur.fetchone()
                if row and row[0]:
                    # reprend depuis avant-hier pour couvrir les données tardives
                    from datetime import date, timedelta
                    since = row[0] - timedelta(days=2)
                    start_date = since.strftime("%Y-%m-%d")
            conn.close()

        property_id = "properties/382670810"
        date_range = DateRange(start_date=start_date, end_date="today")

        # --- 1. Campagnes ---
        resp = client.run_report(RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="date"),
                        Dimension(name="sessionGoogleAdsCampaignName"),
                        Dimension(name="sessionGoogleAdsCampaignType")],
            metrics=[Metric(name="sessions"), Metric(name="conversions"),
                     Metric(name="engagementRate")],
            date_ranges=[date_range], limit=10000,
        ))
        conn = _db_conn()
        if not conn:
            return 0
        n_camps = 0
        with conn, conn.cursor() as cur:
            for row in resp.rows:
                d, name, ctype = [v.value for v in row.dimension_values]
                sessions, conversions, er = [v.value for v in row.metric_values]
                if not name or name == "(not set)":
                    continue
                cur.execute("""
                    INSERT INTO ga4_ads_campaigns
                        (date, campaign_name, campaign_type, sessions, conversions, engagement_rate, collected_at)
                    VALUES (%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (date, campaign_name) DO UPDATE SET
                        campaign_type=EXCLUDED.campaign_type,
                        sessions=EXCLUDED.sessions, conversions=EXCLUDED.conversions,
                        engagement_rate=EXCLUDED.engagement_rate, collected_at=NOW()
                """, (d, name, ctype, int(float(sessions)), int(float(conversions)), float(er)))
                n_camps += 1
        conn.close()

        # --- 2. Mots-clés ---
        resp2 = client.run_report(RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="date"),
                        Dimension(name="sessionGoogleAdsKeyword")],
            metrics=[Metric(name="sessions"), Metric(name="conversions"),
                     Metric(name="engagementRate")],
            date_ranges=[date_range], limit=10000,
        ))
        conn = _db_conn()
        n_kw = 0
        with conn, conn.cursor() as cur:
            for row in resp2.rows:
                d, kw = [v.value for v in row.dimension_values]
                sessions, conversions, er = [v.value for v in row.metric_values]
                if not kw or kw in ("(not set)", "(not provided)"):
                    continue
                cur.execute("""
                    INSERT INTO ga4_ads_keywords
                        (date, keyword, sessions, conversions, engagement_rate, collected_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (date, keyword) DO UPDATE SET
                        sessions=EXCLUDED.sessions, conversions=EXCLUDED.conversions,
                        engagement_rate=EXCLUDED.engagement_rate, collected_at=NOW()
                """, (d, kw, int(float(sessions)), int(float(conversions)), float(er)))
                n_kw += 1
        conn.close()

        # --- 3. Landing pages ---
        resp3 = client.run_report(RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="date"),
                        Dimension(name="landingPage"),
                        Dimension(name="sessionGoogleAdsCampaignName")],
            metrics=[Metric(name="sessions"), Metric(name="conversions")],
            date_ranges=[date_range], limit=10000,
        ))
        conn = _db_conn()
        n_lp = 0
        with conn, conn.cursor() as cur:
            for row in resp3.rows:
                d, page, camp = [v.value for v in row.dimension_values]
                sessions, conversions = [v.value for v in row.metric_values]
                if not page or page == "(not set)":
                    continue
                cur.execute("""
                    INSERT INTO ga4_ads_landing_pages
                        (date, landing_page, campaign_name, sessions, conversions, collected_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (date, landing_page, campaign_name) DO UPDATE SET
                        sessions=EXCLUDED.sessions, conversions=EXCLUDED.conversions,
                        collected_at=NOW()
                """, (d, page, camp or "(direct)", int(float(sessions)), int(float(conversions))))
                n_lp += 1
        conn.close()

        print(f"[ga4_ads] done — campaigns:{n_camps} keywords:{n_kw} landing_pages:{n_lp}")
        return n_camps + n_kw + n_lp
    except Exception as e:
        print(f"[ga4_ads] error: {e}")
        return 0


@app.route("/api/ga4ads/collect", methods=["POST"])
@require_auth_or_key
def api_ga4ads_collect():
    """Déclenche une collecte GA4 Ads manuelle. ?days=N pour la fenêtre historique."""
    days = int(request.args.get("days", 180))
    import threading
    threading.Thread(target=_ga4_collect_ads, args=(days,), daemon=True).start()
    return jsonify({"status": "started", "days": days})


@app.route("/api/ga4ads/stats")
@require_auth_or_key
def api_ga4ads_stats():
    """Stats GA4 Ads depuis la DB : top campagnes, mots-clés, landing pages."""
    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            # Top campagnes (agrégé toutes dates)
            cur.execute("""
                SELECT campaign_name, campaign_type,
                       SUM(sessions) as s, SUM(conversions) as c,
                       CASE WHEN SUM(sessions)>0 THEN ROUND(SUM(conversions)*100.0/SUM(sessions),1) END as cvr
                FROM ga4_ads_campaigns
                GROUP BY campaign_name, campaign_type
                ORDER BY c DESC LIMIT 15
            """)
            campaigns = [{"name":r[0],"type":r[1],"sessions":r[2],"conversions":r[3],"cvr":r[4]}
                         for r in cur.fetchall()]

            # Top mots-clés par conversions
            cur.execute("""
                SELECT keyword, SUM(sessions) as s, SUM(conversions) as c,
                       ROUND(AVG(engagement_rate)*100,1) as er
                FROM ga4_ads_keywords
                GROUP BY keyword
                ORDER BY c DESC LIMIT 20
            """)
            keywords = [{"keyword":r[0],"sessions":r[1],"conversions":r[2],"engagementRate":r[3]}
                        for r in cur.fetchall()]

            # Top landing pages par conversions
            cur.execute("""
                SELECT landing_page, SUM(sessions) as s, SUM(conversions) as c,
                       CASE WHEN SUM(sessions)>0 THEN ROUND(SUM(conversions)*100.0/SUM(sessions),1) END as cvr
                FROM ga4_ads_landing_pages
                GROUP BY landing_page
                ORDER BY c DESC LIMIT 15
            """)
            landing_pages = [{"page":r[0],"sessions":r[1],"conversions":r[2],"cvr":r[3]}
                             for r in cur.fetchall()]

            # Dernière collecte
            cur.execute("SELECT MAX(collected_at), MAX(date) FROM ga4_ads_campaigns")
            row = cur.fetchone()
            last_collected = row[0].isoformat() if row[0] else None
            last_date = row[1].isoformat() if row[1] else None

        return jsonify({
            "campaigns": campaigns,
            "keywords": keywords,
            "landingPages": landing_pages,
            "lastCollected": last_collected,
            "lastDate": last_date,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


# =====================================================================
# PRESTASHOP — paniers abandonnés
# =====================================================================

def _presta_collect_abandoned_carts():
    """Collecte tous les paniers PrestaShop non convertis en commande.

    Logique :
    1. Récupère tous les id_cart des commandes → set des paniers convertis
    2. Pagine tous les paniers (500/page)
    3. Panier avec produits ET absent du set → abandonné
    4. Upserte dans presta_abandoned_carts
    """
    import threading

    def _run():
        try:
            BASE = "https://www.bdouin.com/api"
            KEY  = "AU83IAKGBTE3SRAIW85IFLZ8642AXQPH"
            sess = requests.Session()

            # --- 1. Paniers convertis (tous les orders) ---
            # limit=100 : PrestaShop est lent avec limit>100
            converted = set()
            offset = 0
            while True:
                r = sess.get(f"{BASE}/orders",
                             params={"output_format":"JSON","ws_key":KEY,
                                     "display":"[id,id_cart]","limit":100,"offset":offset},
                             timeout=20)
                if r.status_code != 200:
                    break
                batch = r.json().get("orders", [])
                if not batch:
                    break
                for o in batch:
                    converted.add(str(o["id_cart"]))
                if len(batch) < 100:
                    break
                offset += 100
            print(f"[abandoned] {len(converted)} paniers convertis")

            # --- 2. Produits : cache prix ---
            price_cache = {}
            p_offset = 0
            while True:
                r = sess.get(f"{BASE}/products",
                             params={"output_format":"JSON","ws_key":KEY,
                                     "display":"[id,name,price]","limit":100,"offset":p_offset},
                             timeout=20)
                if r.status_code != 200:
                    break
                prods = r.json().get("products", [])
                if not prods:
                    break
                for p in prods:
                    name = p.get("name","")
                    if isinstance(name, list):
                        name = next((x.get("value","") for x in name if x.get("id_lang")=="1"), "")
                    price_cache[str(p["id"])] = {"name": name, "price": float(p.get("price", 0) or 0)}
                if len(prods) < 100:
                    break
                p_offset += 100
            print(f"[abandoned] {len(price_cache)} produits en cache")

            # --- 3. Paginer les paniers ---
            conn = _db_conn()
            if not conn:
                return
            total_abandoned = 0
            offset = 0
            while True:
                r = sess.get(f"{BASE}/carts",
                             params={"output_format":"JSON","ws_key":KEY,
                                     "display":"full","limit":100,"offset":offset},
                             timeout=20)
                if r.status_code != 200:
                    print(f"[abandoned] carts HTTP {r.status_code} at offset {offset}")
                    break
                batch = r.json().get("carts", [])
                if not batch:
                    break

                with conn, conn.cursor() as cur:
                    for c in batch:
                        cart_id = str(c["id"])
                        if cart_id in converted:
                            continue  # panier converti → skip
                        rows = c.get("associations", {}).get("cart_rows", [])
                        if not rows:
                            continue  # panier vide → skip

                        # Calcul valeur estimée
                        products = []
                        total = 0.0
                        for row in rows:
                            pid = str(row.get("id_product",""))
                            qty = int(row.get("quantity", 1) or 1)
                            info = price_cache.get(pid, {"name": f"product_{pid}", "price": 0})
                            products.append({"id": pid, "name": info["name"],
                                             "qty": qty, "price": info["price"]})
                            total += info["price"] * qty

                        cur.execute("""
                            INSERT INTO presta_abandoned_carts
                                (cart_id, id_customer, id_guest, date_add, date_upd,
                                 nb_products, total_estimated, products, collected_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                            ON CONFLICT (cart_id) DO UPDATE SET
                                nb_products=EXCLUDED.nb_products,
                                total_estimated=EXCLUDED.total_estimated,
                                products=EXCLUDED.products,
                                collected_at=NOW()
                        """, (
                            int(cart_id),
                            int(c.get("id_customer") or 0),
                            int(c.get("id_guest") or 0),
                            c.get("date_add"), c.get("date_upd"),
                            len(products), round(total, 2),
                            json.dumps(products)
                        ))
                        total_abandoned += 1

                if offset % 2000 == 0 and offset > 0:
                    print(f"[abandoned] {total_abandoned} abandonnés traités (offset {offset})...")
                if len(batch) < 100:
                    break
                offset += 100

            conn.close()
            print(f"[abandoned] done — {total_abandoned} paniers abandonnés en DB")
        except Exception as e:
            print(f"[abandoned] error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return "started"


@app.route("/api/abandoned-carts/collect", methods=["POST"])
@require_auth_or_key
def api_abandoned_collect():
    """Déclenche la collecte des paniers abandonnés PrestaShop."""
    import threading
    threading.Thread(target=_presta_collect_abandoned_carts, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/abandoned-carts/stats")
@require_auth_or_key
def api_abandoned_stats():
    """Stats paniers abandonnés : top produits, valeur perdue, patterns temporels."""
    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            # Comptage global
            cur.execute("SELECT COUNT(*), SUM(total_estimated), AVG(total_estimated), AVG(nb_products) FROM presta_abandoned_carts")
            total, val_total, val_avg, prod_avg = cur.fetchone()

            # Top produits abandonnés (extraire depuis JSONB)
            cur.execute("""
                SELECT p->>'name' as name, p->>'id' as pid,
                       COUNT(*) as nb_carts,
                       SUM((p->>'qty')::int) as total_qty,
                       SUM((p->>'price')::float * (p->>'qty')::int) as val_perdue
                FROM presta_abandoned_carts,
                     jsonb_array_elements(products) as p
                GROUP BY name, pid
                ORDER BY nb_carts DESC LIMIT 20
            """)
            top_products = [{"name":r[0],"productId":r[1],"nbCarts":r[2],
                             "totalQty":r[3],"valueLost":round(r[4] or 0, 2)}
                            for r in cur.fetchall()]

            # Distribution par valeur estimée
            cur.execute("""
                SELECT
                    CASE
                        WHEN total_estimated = 0 THEN '0€'
                        WHEN total_estimated < 20 THEN '<20€'
                        WHEN total_estimated < 50 THEN '20-50€'
                        WHEN total_estimated < 100 THEN '50-100€'
                        ELSE '100€+'
                    END as bucket,
                    COUNT(*) as n
                FROM presta_abandoned_carts
                GROUP BY bucket ORDER BY MIN(total_estimated)
            """)
            by_value = [{"bucket":r[0],"count":r[1]} for r in cur.fetchall()]

            # Pattern temporel (heure du jour)
            cur.execute("""
                SELECT EXTRACT(hour FROM date_add) as h, COUNT(*) as n
                FROM presta_abandoned_carts
                WHERE date_add IS NOT NULL
                GROUP BY h ORDER BY h
            """)
            by_hour = [{"hour":int(r[0]),"count":r[1]} for r in cur.fetchall()]

            # Dernière collecte
            cur.execute("SELECT MAX(collected_at) FROM presta_abandoned_carts")
            last = cur.fetchone()[0]

        return jsonify({
            "total": total or 0,
            "totalValueLost": round(float(val_total or 0), 2),
            "avgCartValue": round(float(val_avg or 0), 2),
            "avgProducts": round(float(prod_avg or 0), 1),
            "topProducts": top_products,
            "byValue": by_value,
            "byHour": by_hour,
            "lastCollected": last.isoformat() if last else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


def _refresh_reviews():
    """Full refresh for all apps, all countries. Called at startup + every 24h.
    Persists to : (1) Postgres `reviews` table (durable), (2) /tmp JSON cache (fast)."""
    all_apps = []
    db_summary = []
    for app in APPS:
        ios = _fetch_ios_reviews(app["iosId"])
        android = _fetch_android_reviews(app["androidPkg"])
        merged = ios + android
        merged.sort(key=lambda r: r.get("date", ""), reverse=True)
        # Persist to Postgres (durable, historique complet)
        db_ins, db_upd = _db_insert_reviews(app["name"], merged)
        db_summary.append({"app": app["name"], "inserted": db_ins, "updated": db_upd})
        all_apps.append({
            "name": app["name"],
            "iosId": app["iosId"],
            "androidPkg": app["androidPkg"],
            "stats": {
                "ios": _compute_stats(ios),
                "android": _compute_stats(android),
                "combined": _compute_stats(merged),
            },
            "countryBreakdown": {
                "ios": _country_breakdown(ios),
                "android": _country_breakdown(android),
            },
            "totalFetched": len(merged),
            "recentReviews": merged[:50],  # Top 50 pour un preview — historique complet via /api/reviews/history
        })
    REVIEWS_CACHE["data"] = all_apps
    REVIEWS_CACHE["fetchedAt"] = datetime.now(timezone.utc).isoformat()
    REVIEWS_CACHE["dbSummary"] = db_summary
    # Persist JSON cache (fallback si DB indispo)
    try:
        with open(REVIEWS_PERSIST_PATH, "w", encoding="utf-8") as f:
            json.dump({"fetchedAt": REVIEWS_CACHE["fetchedAt"], "apps": all_apps},
                      f, ensure_ascii=False)
    except Exception as e:
        print(f"[reviews] persist failed: {e}")
    print(f"[reviews] refresh done — DB: {db_summary}")
    return all_apps


def _load_persisted_reviews():
    """Load cached reviews from disk on startup if fresh."""
    try:
        if os.path.exists(REVIEWS_PERSIST_PATH):
            with open(REVIEWS_PERSIST_PATH, "r", encoding="utf-8") as f:
                d = json.load(f)
            REVIEWS_CACHE["data"] = d.get("apps")
            REVIEWS_CACHE["fetchedAt"] = d.get("fetchedAt")
            return True
    except Exception as e:
        print(f"[reviews] load persist failed: {e}")
    return False


@app.route("/api/reviews")
@require_auth_or_key
def api_reviews():
    """Reviews App Store + Google Play pour les apps BDouin.

    Cache 24h. Refresh automatique en background via APScheduler.
    Auth: cookie OU X-API-Key.
    """
    # Lazy refresh si cache vide ou > 24h
    needs_refresh = False
    if not REVIEWS_CACHE.get("data"):
        needs_refresh = True
    else:
        try:
            fetched = datetime.fromisoformat(REVIEWS_CACHE["fetchedAt"].replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - fetched).total_seconds() > 24 * 3600:
                needs_refresh = True
        except Exception:
            needs_refresh = True
    if needs_refresh:
        try:
            _refresh_reviews()
        except Exception as e:
            return jsonify({"error": f"refresh failed: {str(e)}"}), 500

    return jsonify({
        "fetchedAt": REVIEWS_CACHE["fetchedAt"],
        "apps": REVIEWS_CACHE["data"] or [],
    })


# =====================================================================
# DB endpoints — stats + history query
# =====================================================================

@app.route("/api/db/stats")
@require_auth_or_key
def api_db_stats():
    """Overall database counts — vérification rapide que la collecte fonctionne."""
    return jsonify(_db_stats())


@app.route("/api/reviews/history")
@require_auth_or_key
def api_reviews_history():
    """Historique complet des reviews via Postgres.

    Query params:
      - app         : nom de l'app (ex: "Awlad Quiz GO") — optionnel
      - store       : ios | android — optionnel
      - rating      : 1-5 — optionnel
      - country     : code pays — optionnel
      - search      : full-text dans content/title — optionnel
      - from_date   : YYYY-MM-DD — optionnel
      - to_date     : YYYY-MM-DD — optionnel
      - limit       : défaut 200, max 5000
      - offset      : pagination
    """
    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DATABASE_URL"}), 503
    try:
        clauses = []
        params = []
        for key in ("app", "store", "country"):
            val = request.args.get(key)
            if val:
                clauses.append(f"{key} = %s")
                params.append(val)
        rating = request.args.get("rating")
        if rating and rating.isdigit():
            clauses.append("rating = %s")
            params.append(int(rating))
        from_date = request.args.get("from_date")
        if from_date:
            clauses.append("review_date >= %s")
            params.append(from_date)
        to_date = request.args.get("to_date")
        if to_date:
            clauses.append("review_date <= %s")
            params.append(to_date)
        search = request.args.get("search")
        if search:
            clauses.append("(content ILIKE %s OR title ILIKE %s)")
            like = f"%{search}%"
            params.extend([like, like])
        try:
            limit = max(1, min(5000, int(request.args.get("limit", "200"))))
        except ValueError:
            limit = 200
        try:
            offset = max(0, int(request.args.get("offset", "0")))
        except ValueError:
            offset = 0

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        q = f"""
            SELECT id, app, store, country, rating, title, content, version, author,
                   review_date, thumbs_up, reply_content, first_seen_at
            FROM reviews
            {where}
            ORDER BY review_date DESC NULLS LAST, first_seen_at DESC
            LIMIT %s OFFSET %s
        """
        with conn, conn.cursor() as cur:
            cur.execute(q, params + [limit, offset])
            rows = cur.fetchall()
            cols = ["id","app","store","country","rating","title","content","version",
                    "author","reviewDate","thumbsUp","replyContent","firstSeenAt"]
            result = []
            for r in rows:
                rec = dict(zip(cols, r))
                # Serialize dates
                if rec.get("reviewDate"):
                    rec["reviewDate"] = rec["reviewDate"].isoformat()
                if rec.get("firstSeenAt"):
                    rec["firstSeenAt"] = rec["firstSeenAt"].isoformat()
                result.append(rec)
            # Total count
            cur.execute(f"SELECT COUNT(*) FROM reviews {where}", params)
            total = cur.fetchone()[0]
        return jsonify({"total": total, "limit": limit, "offset": offset, "rows": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.route("/api/mailerlite/collect", methods=["POST"])
@require_auth_or_key
def api_ml_collect():
    """Déclenche une collecte MailerLite manuelle.
    ?full=1 pour relancer la collecte complète des 777k abonnés (lent).
    """
    full = request.args.get("full") == "1"
    _ml_collect_all(full_subscribers=full)
    return jsonify({"status": "started", "full_subscribers": full})


@app.route("/api/mailerlite/stats")
@require_auth_or_key
def api_ml_stats():
    """Stats MailerLite depuis la DB : groupes, campagnes, abonnés."""
    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            # Comptes
            cur.execute("SELECT COUNT(*), SUM(active) FROM ml_groups")
            gcount, gactive = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM ml_campaigns")
            ccount = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE status='active') FROM ml_subscribers")
            scount, sactive = cur.fetchone()

            # Top groupes par actifs
            cur.execute("""
                SELECT name, active, sent, opened, clicked,
                       CASE WHEN sent>0 THEN ROUND(opened*100.0/sent,1) END as open_rate
                FROM ml_groups ORDER BY active DESC LIMIT 10
            """)
            top_groups = [{"name":r[0],"active":r[1],"sent":r[2],
                           "opened":r[3],"clicked":r[4],"openRate":r[5]} for r in cur.fetchall()]

            # Top campagnes par open rate (min 1000 envois)
            cur.execute("""
                SELECT name, subject, date_send, total_recipients,
                       opened_count, opened_rate, clicked_count, clicked_rate
                FROM ml_campaigns
                WHERE total_recipients >= 1000
                ORDER BY opened_rate DESC LIMIT 10
            """)
            top_camps = [{"name":r[0],"subject":r[1],
                          "dateSend":r[2].isoformat() if r[2] else None,
                          "recipients":r[3],"openedCount":r[4],"openedRate":r[5],
                          "clickedCount":r[6],"clickedRate":r[7]} for r in cur.fetchall()]

            # Distribution pays abonnés
            cur.execute("""
                SELECT country, COUNT(*) as n FROM ml_subscribers
                WHERE status='active' AND country IS NOT NULL AND country != ''
                GROUP BY country ORDER BY n DESC LIMIT 20
            """)
            countries = [{"country":r[0],"count":r[1]} for r in cur.fetchall()]

            # Dernière collecte
            cur.execute("SELECT MAX(collected_at) FROM ml_groups")
            last_groups = cur.fetchone()[0]
            cur.execute("SELECT MAX(collected_at) FROM ml_campaigns")
            last_campaigns = cur.fetchone()[0]
            cur.execute("SELECT MAX(collected_at) FROM ml_subscribers")
            last_subs = cur.fetchone()[0]

        return jsonify({
            "groups":    {"count": gcount or 0, "totalActive": gactive or 0, "top": top_groups,
                          "lastCollected": last_groups.isoformat() if last_groups else None},
            "campaigns": {"count": ccount or 0, "top": top_camps,
                          "lastCollected": last_campaigns.isoformat() if last_campaigns else None},
            "subscribers": {"total": scount or 0, "active": sactive or 0,
                            "byCountry": countries,
                            "lastCollected": last_subs.isoformat() if last_subs else None},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


# =====================================================================
# Ingest endpoints — reçoivent les rows brutes depuis Apps Script
# =====================================================================

def _parse_period_from_subject(subject):
    """Extrait une date YYYY-MM depuis le sujet d'un email Sofiadis/IMAK."""
    import re
    months = {
        'janvier':1,'février':2,'fevrier':2,'mars':3,'avril':4,'mai':5,'juin':6,
        'juillet':7,'août':8,'aout':8,'septembre':9,'octobre':10,'novembre':11,'décembre':12,'decembre':12,
        'january':1,'february':2,'march':3,'april':4,'june':6,'july':7,
        'august':8,'september':9,'october':10,'november':11,'december':12,
    }
    subj = subject.lower()
    # Cherche MOIS AAAA ou AAAA - MOIS
    for name, num in months.items():
        if name in subj:
            m = re.search(r'20\d{2}', subject)
            if m:
                return f"{m.group()}-{num:02d}"
    return None


@app.route("/api/sofiadis/b2b/ingest", methods=["POST"])
@require_auth_or_key
def api_sofiadis_b2b_ingest():
    """Reçoit rows brutes depuis Apps Script (relevé Sofiadis B2B Excel)."""
    data = request.get_json(silent=True) or {}
    period_str = data.get("period")  # "2026-03"
    source = data.get("source", "gmail_appsscript")

    # Accepte soit sheets:[{name,data}] (nouveau format multi-feuilles) soit rows:[] (legacy)
    sheets_raw = data.get("sheets")  # [{name: str, data: [[...]]}]
    if sheets_raw:
        sheets_list = [s.get("data", []) for s in sheets_raw]
    else:
        rows_single = data.get("rows", [])
        sheets_list = [rows_single] if rows_single else []

    if not sheets_list or not period_str:
        return jsonify({"error": "period and rows/sheets required"}), 400

    # Convertit "2026-03" → date 2026-03-01
    try:
        from datetime import date
        y, m = period_str.split("-")
        period = date(int(y), int(m), 1)
    except Exception:
        return jsonify({"error": f"invalid period: {period_str}"}), 400

    # Mots-clés pour détection de la ligne d'en-tête
    # Substring dans une cellule (mots longs, sans risque de faux positif)
    HDR_SUB   = ['titre','title','désignation','designation','libellé','libelle',
                 'ouvrage','référence','reference','article']
    # Correspondance exacte de cellule (mots courts — 'ean' matcherait 'echeance' en substring!)
    HDR_EXACT = {'ean', 'isbn', 'ref', 'code'}

    def _is_header(cells):
        return (any(any(k in c for c in cells) for k in HDR_SUB) or
                any(c in HDR_EXACT for c in cells))

    # Essaie chaque feuille jusqu'à trouver une avec une en-tête valide
    header_idx = None
    header = []
    rows = []
    for sheet_rows in sheets_list:
        for i, row in enumerate(sheet_rows):
            cells = [str(c).lower().strip() for c in row]
            if _is_header(cells):
                header_idx = i
                header = cells
                rows = sheet_rows
                break
        if header_idx is not None:
            break

    if header_idx is None:
        first_headers = [s[0] if s else [] for s in sheets_list[:3]]
        print(f"[b2b ingest] no header found across {len(sheets_list)} sheet(s), subject: {source}, first rows: {first_headers}")
        return jsonify({"status": "no_header", "sheets": len(sheets_list),
                        "first_rows": [str(s[0])[:80] if s else '' for s in sheets_list[:3]]}), 200

    # Détecte les colonnes clés
    def col(keys):
        for k in keys:
            for i, h in enumerate(header):
                if k in h: return i
        return None

    idx_title  = col(['titre','title','désignation','designation','libellé','ouvrage','référence','article'])
    idx_ean    = col(['ean','isbn'])
    idx_sold   = col(['vente','vendu','sold','qté v','qty v','quantité v'])
    idx_ret    = col(['retour','avoir','return','qté r','qty r','quantité r'])
    idx_price  = col(['prix','price','p.u.','pu ht','tarif'])
    idx_total  = col(['total','montant','ht'])

    if idx_title is None:
        return jsonify({"status": "no_title_col", "header": header}), 200

    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503

    inserted = 0
    skipped = 0
    try:
        with conn, conn.cursor() as cur:
            for row in rows[header_idx + 1:]:
                if len(row) <= (idx_title or 0): continue
                title = str(row[idx_title]).strip()
                if not title or title.lower() in ('', 'total', 'sous-total', 'none'): continue

                def val(idx, default=0):
                    if idx is None or idx >= len(row): return default
                    v = str(row[idx]).replace(' ','').replace(',','.').strip()
                    try: return float(v)
                    except: return default

                ean       = str(row[idx_ean]).strip() if idx_ean and idx_ean < len(row) else None
                qty_sold  = int(val(idx_sold))
                qty_ret   = int(val(idx_ret))
                net_qty   = qty_sold - qty_ret
                price_ht  = val(idx_price)
                total_ht  = val(idx_total)

                # Si total absent, calcule
                if total_ht == 0 and price_ht and net_qty:
                    total_ht = round(price_ht * net_qty, 2)

                cur.execute("""
                    INSERT INTO sofiadis_b2b_sales
                        (period, title, ean, qty_sold, qty_returned, net_qty, price_ht, total_ht, source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (period, title) DO UPDATE SET
                        ean=EXCLUDED.ean, qty_sold=EXCLUDED.qty_sold,
                        qty_returned=EXCLUDED.qty_returned, net_qty=EXCLUDED.net_qty,
                        price_ht=EXCLUDED.price_ht, total_ht=EXCLUDED.total_ht,
                        source=EXCLUDED.source, collected_at=NOW()
                """, (period, title, ean, qty_sold, qty_ret, net_qty, price_ht, total_ht, source))
                inserted += 1
        return jsonify({"status": "ok", "period": period_str, "inserted": inserted, "skipped": skipped})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


@app.route("/api/sofiadis/logistics/ingest", methods=["POST"])
@require_auth_or_key
def api_sofiadis_logistics_ingest():
    """Reçoit données logistique depuis Apps Script (Excel ou total extrait du PDF)."""
    data = request.get_json(silent=True) or {}
    period_str = data.get("period")
    source = data.get("source", "gmail_appsscript")

    # Cas 1 : montant direct (Apps Script a extrait le total depuis le PDF)
    amount_ht   = data.get("amount_ht")
    invoice_ref = data.get("invoice_ref", "")
    status      = data.get("status", "unknown")

    # Cas 2 : rows brutes (Excel) — accepte sheets:[{name,data}] ou rows:[]
    sheets_raw = data.get("sheets")
    if sheets_raw:
        all_rows = [row for s in sheets_raw for row in s.get("data", [])]
    else:
        all_rows = data.get("rows", [])

    if not period_str:
        return jsonify({"error": "period required"}), 400

    try:
        from datetime import date
        y, m = period_str.split("-")
        period = date(int(y), int(m), 1)
    except Exception:
        return jsonify({"error": f"invalid period: {period_str}"}), 400

    # Si rows fournis, cherche le montant total dans les rows
    if all_rows and amount_ht is None:
        for row in all_rows:
            row_str = ' '.join(str(c).lower() for c in row)
            if 'total' in row_str:
                for cell in row:
                    v = str(cell).replace(' ','').replace(',','.')
                    try:
                        f = float(v)
                        if f > 10:  # filtre les zéros et petits chiffres
                            amount_ht = f
                            break
                    except: continue
                if amount_ht: break

    if amount_ht is None:
        return jsonify({"status": "no_amount_found", "rows_received": len(all_rows)}), 200

    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sofiadis_logistics (period, amount_ht, invoice_ref, status, source)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (period) DO UPDATE SET
                    amount_ht=EXCLUDED.amount_ht, invoice_ref=EXCLUDED.invoice_ref,
                    status=EXCLUDED.status, source=EXCLUDED.source, collected_at=NOW()
            """, (period, amount_ht, invoice_ref, status, source))
        return jsonify({"status": "ok", "period": period_str, "amount_ht": amount_ht})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


@app.route("/api/sofiadis/statement/ingest", methods=["POST"])
@require_auth_or_key
def api_sofiadis_statement_ingest():
    """Reçoit docs comptables Sofiadis/Sofiaco (grand livres, situation comptable, etc.)."""
    data = request.get_json(silent=True) or {}
    period_str = data.get("period", "")
    filename   = data.get("filename", data.get("source", ""))
    sender     = data.get("sender", "")
    source     = data.get("source", "")

    sheets_raw = data.get("sheets")
    if sheets_raw:
        sheet_names = [s.get("name","") for s in sheets_raw]
        total_rows  = sum(len(s.get("data",[])) for s in sheets_raw)
    else:
        sheet_names = []
        total_rows  = len(data.get("rows", []))

    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sofiadis_accounting_docs (
                    id          SERIAL PRIMARY KEY,
                    period      VARCHAR(7),
                    filename    TEXT,
                    sheet_names TEXT,
                    total_rows  INT,
                    sender      TEXT,
                    source      TEXT,
                    collected_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO sofiadis_accounting_docs
                    (period, filename, sheet_names, total_rows, sender, source)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (period_str, filename, ','.join(sheet_names), total_rows, sender, source))
        return jsonify({"status": "ok", "period": period_str, "filename": filename,
                        "sheets": len(sheet_names), "rows": total_rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


@app.route("/api/imak/ingest", methods=["POST"])
@require_auth_or_key
def api_imak_ingest():
    """Reçoit factures IMAK depuis Apps Script (Excel ou rows extraites)."""
    data = request.get_json(silent=True) or {}
    source      = data.get("source", "gmail_appsscript")
    invoice_ref = data.get("invoice_ref", "")
    print_date  = data.get("print_date")  # "2025-03-31"
    period      = data.get("period", "")  # "2025-03"

    # Accepte sheets:[{name,data}] (nouveau) ou rows:[] (legacy)
    sheets_raw = data.get("sheets")
    if sheets_raw:
        sheets_list = [(s.get("name",""), s.get("data",[])) for s in sheets_raw]
    else:
        rows_single = data.get("rows", [])
        sheets_list = [("", rows_single)] if rows_single else []

    if not sheets_list:
        return jsonify({"error": "rows or sheets required"}), 400

    # Trouve en-tête sur n'importe quelle feuille
    # Même logique que B2B : 'ean' en exact uniquement (évite match sur 'echeance')
    IMAK_HDR_SUB   = ['titre','title','désignation','designation','qty','quantit','amount','montant','item','book']
    IMAK_HDR_EXACT = {'ean', 'isbn'}

    header_idx = None
    header = []
    rows = []
    for _sheet_name, sheet_rows in sheets_list:
        for i, row in enumerate(sheet_rows):
            cells = [str(c).lower().strip() for c in row]
            if (any(any(k in c for c in cells) for k in IMAK_HDR_SUB) or
                    any(c in IMAK_HDR_EXACT for c in cells)):
                header_idx = i
                header = cells
                rows = sheet_rows
                break
        if header_idx is not None:
            break

    if header_idx is None:
        return jsonify({"status": "no_header", "sheets": len(sheets_list)}), 200

    def col(keys):
        for k in keys:
            for i, h in enumerate(header):
                if k in h: return i
        return None

    idx_title  = col(['titre','title','désignation','designation','item','book'])
    idx_qty    = col(['qty','quantit','qté','copies','ex.','exemplaire'])
    idx_unit   = col(['unit','p.u.','prix unit','price per'])
    idx_total  = col(['total','amount','montant'])

    if idx_title is None:
        return jsonify({"status": "no_title_col", "header": header}), 200

    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503

    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            for row in rows[header_idx + 1:]:
                if len(row) <= (idx_title or 0): continue
                title = str(row[idx_title]).strip()
                if not title or title.lower() in ('', 'total', 'none'): continue

                def val(idx, default=0):
                    if idx is None or idx >= len(row): return default
                    v = str(row[idx]).replace(' ','').replace(',','.').strip()
                    try: return float(v)
                    except: return default

                qty        = int(val(idx_qty))
                unit_cost  = val(idx_unit)
                total_cost = val(idx_total)
                if total_cost == 0 and unit_cost and qty:
                    total_cost = round(unit_cost * qty, 2)
                if unit_cost == 0 and total_cost and qty:
                    unit_cost = round(total_cost / qty, 4)

                ref = invoice_ref or source[:50]
                pd  = print_date or data.get("email_date", "2000-01-01")

                cur.execute("""
                    INSERT INTO imak_print_orders
                        (print_date, title, qty, unit_cost_eur, total_cost_eur, invoice_ref, period, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'unknown')
                    ON CONFLICT (print_date, title, invoice_ref) DO UPDATE SET
                        qty=EXCLUDED.qty, unit_cost_eur=EXCLUDED.unit_cost_eur,
                        total_cost_eur=EXCLUDED.total_cost_eur, period=EXCLUDED.period,
                        collected_at=NOW()
                """, (pd, title, qty, unit_cost, total_cost, ref, period))
                inserted += 1
        return jsonify({"status": "ok", "inserted": inserted})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


# =====================================================================
# Sofiadis B2B — seed + collect + stats
# =====================================================================

def _sofiadis_b2b_seed():
    """Seed sofiadis_b2b_sales depuis les données Drive (ventes bulk par titre/période)."""
    conn = _db_conn()
    if not conn:
        print("[sofiadis_b2b] no DB"); return
    # Rows: (period, title, qty_sold, price_ht, total_ht)
    rows = [
        # Invoice 2024-055 — Jul-24 batch (bill date 13 Jul 2024)
        ("2024-07-01", "Famille Foulane #1",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #2",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #3",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #4",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #5",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #6",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #7",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #8",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #9",        3000, 4.548,  13644.00),
        ("2024-07-01", "Famille Foulane #10",       5000, 4.548,  22740.00),
        ("2024-07-01", "Muslim Show Collector",     3000, 9.48,   28440.00),
        ("2024-07-01", "Dialogue",                  4000, 5.12,   20480.00),
        ("2024-07-01", "Guide du Hajj",             7000, 5.12,   35840.00),
        ("2024-07-01", "Walad découvre Médine",     5000, 4.548,  22740.00),
        # Invoice 2024-055 — Mar-24 batch (bill date 10 Aug 2024)
        ("2024-08-01", "Recueil Citadelle",         5000, 4.548,  22740.00),
        ("2024-08-01", "Guide du Hajj Omra",        5000, 5.12,   25600.00),
        ("2024-08-01", "Bonnes Actions",            5000, 4.548,  22740.00),
        ("2024-08-01", "Le Mois Béni de Ramadan",   5000, 4.548,  22740.00),
        # Invoice 2025-505 — Jan 2025
        ("2025-01-01", "Famille Foulane #11",       5000, 4.43,   22150.00),
        # Invoice 2025-505/603 — Mar 2025
        ("2025-03-01", "La Salat et les Ablutions (Fille)",   5000, 4.43,  22150.00),
        ("2025-03-01", "Walad découvre La Mecque",            3000, 4.43,  13290.00),
        ("2025-03-01", "La Salat et les Ablutions (Garçon)",  5000, 4.43,  22150.00),
        ("2025-03-01", "Guide du Hajj",                       5000, 4.99,  24950.00),
        ("2025-03-01", "Walad & Binti (Manga)",               3000, 3.14,   9420.00),
        ("2025-03-01", "Walad et Binti T1",                   6000, 4.99,  29940.00),
        ("2025-03-01", "Walad et Binti T2",                   6000, 4.98,  29880.00),
        # Invoice 2025-505 — May 2025
        ("2025-05-01", "Super Etudiant",              4000, 4.43,  17720.00),
        ("2025-05-01", "Walad & Binti (Manga) 5000",  5000, 3.14,  15700.00),
        # Invoice 2025-006 — May 2025 (Muslim Show)
        ("2025-05-01", "Recueil Muslim Show #1",  2000, 3.697,  7393.62),
        ("2025-05-01", "Recueil Muslim Show #2",  2000, 3.697,  7393.62),
        ("2025-05-01", "Recueil Muslim Show #3",  2000, 3.697,  7393.62),
        ("2025-05-01", "Recueil Muslim Show #4",  2000, 3.697,  7393.62),
        # Invoice 2025-505 — Jun 2025
        ("2025-06-01", "Guide du Hajj 10000",    10000, 4.99,  49900.00),
        # Invoice 2025-534 — Oct 2025
        ("2025-10-01", "L'Agence Règle Tout T1",              3080, 4.43,  13644.40),
        ("2025-10-01", "L'Agence Règle Tout T2",              3080, 4.34,  13360.88),
        ("2025-10-01", "L'Agence Règle Tout T3",              3016, 4.43,  13360.88),
        ("2025-10-01", "La Salat et les Ablutions (Garçon)",  10065, 4.43, 44587.95),
        ("2025-10-01", "La Salat et les Ablutions (Fille)",   10024, 4.43, 44406.32),
        ("2025-10-01", "Awlad School",                        4027, 3.70,  14899.90),
        # Invoice 2025-599 — Dec 2025
        ("2025-12-01", "Walad et Binti T3",  6045, 4.98,  30104.10),
    ]
    try:
        with conn, conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO sofiadis_b2b_sales
                    (period, title, qty_sold, net_qty, price_ht, total_ht)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (period, title) DO UPDATE SET
                    qty_sold=EXCLUDED.qty_sold, net_qty=EXCLUDED.net_qty,
                    price_ht=EXCLUDED.price_ht, total_ht=EXCLUDED.total_ht,
                    collected_at=NOW()
            """, [(r[0], r[1], r[2], r[2], r[3], r[4]) for r in rows])
        print(f"[sofiadis_b2b] seeded {len(rows)} rows")
    except Exception as e:
        print(f"[sofiadis_b2b] seed error: {e}")
    finally:
        try: conn.close()
        except Exception: pass


@app.route("/api/sofiadis/b2b/collect", methods=["POST"])
@require_auth_or_key
def api_sofiadis_b2b_collect():
    import threading
    threading.Thread(target=_sofiadis_b2b_seed, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/sofiadis/b2b/stats")
@require_auth_or_key
def api_sofiadis_b2b_stats():
    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(total_ht), SUM(qty_sold) FROM sofiadis_b2b_sales")
            nrows, total_ht, total_qty = cur.fetchone()

            cur.execute("""
                SELECT title, SUM(qty_sold) as qty, SUM(total_ht) as ht
                FROM sofiadis_b2b_sales GROUP BY title ORDER BY ht DESC LIMIT 20
            """)
            by_title = [{"title": r[0], "qtySold": r[1], "totalHt": float(r[2])} for r in cur.fetchall()]

            cur.execute("""
                SELECT TO_CHAR(period, 'YYYY-MM') as month, SUM(total_ht) as ht, SUM(qty_sold) as qty
                FROM sofiadis_b2b_sales GROUP BY month ORDER BY month
            """)
            by_month = [{"month": r[0], "totalHt": float(r[1]), "qtySold": r[2]} for r in cur.fetchall()]

            cur.execute("SELECT MAX(collected_at) FROM sofiadis_b2b_sales")
            last = cur.fetchone()[0]

        return jsonify({
            "rows": nrows or 0,
            "totalHt": float(total_ht or 0),
            "totalQty": total_qty or 0,
            "byTitle": by_title,
            "byMonth": by_month,
            "lastCollected": last.isoformat() if last else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


# =====================================================================
# Sofiadis Logistique — seed + collect + stats
# =====================================================================

def _sofiadis_logistics_seed():
    """Seed sofiadis_logistics depuis les factures Drive (frais mensuels bdouin.com)."""
    conn = _db_conn()
    if not conn:
        print("[sofiadis_log] no DB"); return
    # Rows: (period, amount_ht, invoice_ref, status)
    rows = [
        # Historique des impressions — Feuil1 (Received invoices logistique)
        ("2024-01-01", 2804.94,  "1004563",       "paid"),
        ("2024-02-01", 10185.27, "1004564",       "paid"),
        ("2024-03-01", 9402.84,  "100465",        "paid"),
        ("2024-04-01", 8145.77,  "1004675",       "paid"),
        ("2024-05-01", 2836.16,  "1004676",       "paid"),
        ("2024-06-01", 10868.68, "1004789",       "paid"),
        ("2024-07-01", 4542.23,  "FC241000983",   "paid"),
        ("2024-08-01", 2129.50,  "FC241000722",   "paid"),
        ("2024-09-01", 3699.70,  "FC250301052",   "paid"),
        ("2024-10-01", 5310.94,  "FC250301113",   "paid"),
        ("2024-11-01", 6308.21,  "FC250301038",   "paid"),
        ("2024-12-01", 6341.48,  "FC250301078",   "overdue"),
        ("2025-01-01", 5816.17,  "FC250301062",   "overdue"),
        # Sofiadis Reconciliation 2025 — frais 2025
        ("2025-02-01", 8420.00,  "FC250401353",   "unknown"),
        ("2025-03-01", 17335.00, "FC250401354",   "unknown"),
        ("2025-04-01", 4885.26,  "FC250601935",   "unknown"),
        ("2025-05-01", 6001.69,  "FC250601937",   "unknown"),
        ("2025-06-01", 6995.57,  "FC250701931",   "unknown"),
        ("2025-08-01", 1843.81,  "FC251000796",   "unknown"),
        ("2025-09-01", 7821.82,  "FC251000735",   "unknown"),
        ("2025-10-01", 8365.84,  "FC251101181",   "unknown"),
        ("2025-11-01", 9263.98,  "FC251201539",   "unknown"),
    ]
    try:
        with conn, conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO sofiadis_logistics (period, amount_ht, invoice_ref, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (period) DO UPDATE SET
                    amount_ht=EXCLUDED.amount_ht, invoice_ref=EXCLUDED.invoice_ref,
                    status=EXCLUDED.status, collected_at=NOW()
            """, rows)
        print(f"[sofiadis_log] seeded {len(rows)} rows")
    except Exception as e:
        print(f"[sofiadis_log] seed error: {e}")
    finally:
        try: conn.close()
        except Exception: pass


@app.route("/api/sofiadis/logistics/collect", methods=["POST"])
@require_auth_or_key
def api_sofiadis_logistics_collect():
    import threading
    threading.Thread(target=_sofiadis_logistics_seed, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/sofiadis/logistics/stats")
@require_auth_or_key
def api_sofiadis_logistics_stats():
    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(amount_ht), AVG(amount_ht) FROM sofiadis_logistics")
            nrows, total, avg = cur.fetchone()

            cur.execute("""
                SELECT TO_CHAR(period, 'YYYY-MM') as month, amount_ht, invoice_ref, status
                FROM sofiadis_logistics ORDER BY period
            """)
            by_month = [{"month": r[0], "amountHt": float(r[1]), "invoiceRef": r[2], "status": r[3]}
                        for r in cur.fetchall()]

            cur.execute("SELECT MAX(collected_at) FROM sofiadis_logistics")
            last = cur.fetchone()[0]

        return jsonify({
            "rows": nrows or 0,
            "totalHt": float(total or 0),
            "avgMonthlyHt": float(avg or 0),
            "byMonth": by_month,
            "lastCollected": last.isoformat() if last else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


# =====================================================================
# IMAK Print Orders — seed + collect + stats
# =====================================================================

def _imak_seed():
    """Seed imak_print_orders depuis les factures Drive (impressions titre par titre)."""
    conn = _db_conn()
    if not conn:
        print("[imak] no DB"); return
    # Rows: (print_date, title, qty, total_cost_eur, invoice_ref, period, status)
    rows = [
        # Mar-24 batch — invoiced Aug 2024
        ("2024-08-10", "Recueil Citadelle",           5000, 4750.00,  "1004790",       "2024-03", "overdue"),
        ("2024-08-10", "Guide du Hajj Omra",          5000, 5950.00,  "1004791",       "2024-03", "overdue"),
        ("2024-08-10", "Bonnes Actions",              5000, 4750.00,  "1004792",       "2024-03", "overdue"),
        ("2024-08-10", "Le Mois Béni de Ramadan",     5000, 4750.00,  "1004793",       "2024-03", "overdue"),
        # Jun-24 batch — invoiced Jul 2024
        ("2024-07-13", "Famille Foulane #1",          3000, 3416.25,  "1004732",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #2",          3000, 3511.25,  "1004733",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #3",          3000, 3606.25,  "1004734",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #4",          3000, 3321.25,  "1004735",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #5",          3000, 3141.25,  "1004736",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #6",          3000, 3036.25,  "1004737",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #7",          3000, 2941.25,  "1004738",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #8",          3000, 3606.25,  "1004739",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #9",          3000, 3226.25,  "1004740",       "2024-06", "paid"),
        ("2024-07-13", "Famille Foulane #10",         5000, 5789.58,  "1004741",       "2024-06", "paid"),
        ("2024-07-13", "Muslim Show Collector",       3000, 12281.25, "1004742",       "2024-06", "paid"),
        ("2024-07-13", "Dialogue",                    4000, 8343.75,  "1004743",       "2024-06", "paid"),
        ("2024-07-13", "Guide du Hajj",               7000, 7654.75,  "1004744",       "2024-06", "paid"),
        ("2024-07-13", "Walad découvre Médine",       5000, 8656.25,  "1004745",       "2024-06", "paid"),
        # Oct-24
        ("2024-10-18", "Walad & Binti (Manga)",       3000, 2900.00,  "FC241001943",   "2024-10", "overdue"),
        # Dec-24 batch — invoiced Jan 2025
        ("2025-01-31", "Famille Foulane #11",         5000, 5700.00,  "2025-505",      "2024-12", "unknown"),
        # Mar-25 batch
        ("2025-03-26", "La Salat et les Ablutions (Fille)",   5000,  6400.00, "2025-603", "2025-03", "unknown"),
        ("2025-03-31", "Walad découvre La Mecque",            3000,  8656.25, "2025-505", "2025-03", "unknown"),
        ("2025-03-31", "La Salat et les Ablutions (Garçon)",  5000,  6400.00, "2025-603", "2025-03", "unknown"),
        ("2025-03-31", "Guide du Hajj 2025",                  5000,  6600.00, "2025-505", "2025-03", "unknown"),
        ("2025-03-31", "Walad & Binti (Manga) 3000",          3000,  2900.00, "2025-505", "2025-03", "unknown"),
        ("2025-03-31", "Walad et Binti T1",                   6000,  9200.00, "2025-505", "2025-03", "unknown"),
        ("2025-03-31", "Walad et Binti T2",                   6000,  9200.00, "2025-505", "2025-03", "unknown"),
        # May-25 batch
        ("2025-05-03", "Super Etudiant",              4000, 3800.00,  "2025-505", "2025-03", "unknown"),
        ("2025-05-19", "Recueil Muslim Show #1",      2000, 1270.00,  "2025-006", "2025-03", "unknown"),
        ("2025-05-19", "Recueil Muslim Show #2",      2000, 1270.00,  "2025-006", "2025-03", "unknown"),
        ("2025-05-19", "Recueil Muslim Show #3",      2000, 1270.00,  "2025-006", "2025-03", "unknown"),
        ("2025-05-19", "Recueil Muslim Show #4",      2000, 1270.00,  "2025-006", "2025-03", "unknown"),
        ("2025-05-19", "Walad & Binti (Manga) 5000",  5000, 4833.30,  "2025-505", "2025-03", "unknown"),
        # Jun-25 batch
        ("2025-06-16", "Guide du Hajj 10000",        10000, 13900.00, "2025-505", "2025-06", "unknown"),
        # Sep-25 batch — invoiced Oct 2025
        ("2025-10-07", "L'Agence Règle Tout T1",              3080, 2556.40,  "2025-534", "2025-09", "unknown"),
        ("2025-10-07", "L'Agence Règle Tout T2",              3080, 2382.64,  "2025-534", "2025-09", "unknown"),
        ("2025-10-07", "L'Agence Règle Tout T3",              3016, 2503.28,  "2025-534", "2025-09", "unknown"),
        ("2025-10-07", "La Salat et les Ablutions (Garçon)",  10065, 12681.90, "2025-534", "2025-09", "unknown"),
        ("2025-10-07", "La Salat et les Ablutions (Fille)",   10024, 12630.24, "2025-534", "2025-09", "unknown"),
        ("2025-10-07", "Awlad School",                        4027, 4751.86,  "2025-534", "2025-09", "unknown"),
        # Nov-25 batch — invoiced Dec 2025
        ("2025-12-08", "Walad et Binti T3",           6045, 10175.73, "2025-599", "2025-11", "unknown"),
    ]
    try:
        with conn, conn.cursor() as cur:
            for r in rows:
                qty = r[2]
                total = r[3]
                unit = round(total / qty, 4) if qty else 0
                cur.execute("""
                    INSERT INTO imak_print_orders
                        (print_date, title, qty, unit_cost_eur, total_cost_eur, invoice_ref, period, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (print_date, title, invoice_ref) DO UPDATE SET
                        qty=EXCLUDED.qty, unit_cost_eur=EXCLUDED.unit_cost_eur,
                        total_cost_eur=EXCLUDED.total_cost_eur, period=EXCLUDED.period,
                        status=EXCLUDED.status, collected_at=NOW()
                """, (r[0], r[1], qty, unit, total, r[4], r[5], r[6]))
        print(f"[imak] seeded {len(rows)} rows")
    except Exception as e:
        print(f"[imak] seed error: {e}")
    finally:
        try: conn.close()
        except Exception: pass


@app.route("/api/imak/collect", methods=["POST"])
@require_auth_or_key
def api_imak_collect():
    import threading
    threading.Thread(target=_imak_seed, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/imak/stats")
@require_auth_or_key
def api_imak_stats():
    conn = _db_conn()
    if not conn:
        return jsonify({"error": "no DB"}), 503
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(total_cost_eur), SUM(qty) FROM imak_print_orders")
            nrows, total_eur, total_qty = cur.fetchone()

            cur.execute("""
                SELECT title, SUM(qty) as qty, SUM(total_cost_eur) as cost
                FROM imak_print_orders GROUP BY title ORDER BY cost DESC LIMIT 30
            """)
            by_title = [{"title": r[0], "qty": r[1], "totalEur": float(r[2])} for r in cur.fetchall()]

            cur.execute("""
                SELECT period, SUM(total_cost_eur) as cost, SUM(qty) as qty
                FROM imak_print_orders GROUP BY period ORDER BY period
            """)
            by_period = [{"period": r[0], "totalEur": float(r[1]), "qty": r[2]} for r in cur.fetchall()]

            cur.execute("SELECT MAX(collected_at) FROM imak_print_orders")
            last = cur.fetchone()[0]

        return jsonify({
            "rows": nrows or 0,
            "totalEur": float(total_eur or 0),
            "totalQty": total_qty or 0,
            "byTitle": by_title,
            "byPeriod": by_period,
            "lastCollected": last.isoformat() if last else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try: conn.close()
        except Exception: pass


# =====================================================================
# Background scheduler — refresh reviews every 24h
# =====================================================================

def _init_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler(daemon=True, timezone="UTC")
        # Reviews — refresh toutes les 24h
        scheduler.add_job(_refresh_reviews, "interval", hours=24, id="reviews_refresh",
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30))
        # MailerLite — groups + campaigns toutes les 6h
        scheduler.add_job(lambda: (_ml_collect_groups(), _ml_collect_campaigns()),
                          "interval", hours=6, id="ml_groups_campaigns",
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=60))
        # MailerLite — abonnés incrémental toutes les 24h
        scheduler.add_job(lambda: _ml_collect_subscribers(full=False),
                          "interval", hours=24, id="ml_subscribers_incremental",
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=90))
        # GA4 Ads — refresh quotidien (incrémental automatique)
        scheduler.add_job(lambda: _ga4_collect_ads(days=7),
                          "interval", hours=24, id="ga4_ads_refresh",
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=120))
        # PrestaShop paniers abandonnés — refresh hebdomadaire
        scheduler.add_job(_presta_collect_abandoned_carts,
                          "interval", hours=168, id="abandoned_carts_refresh",
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=150))
        scheduler.start()
    except Exception as e:
        print(f"[scheduler] init failed: {e}")


# At import time: run DB migrations (idempotent) + load persisted cache
if DATABASE_URL:
    _db_migrate()
_load_persisted_reviews()

# Start scheduler only in production (not during import for tests)
if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("ENABLE_SCHEDULER"):
    _init_scheduler()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

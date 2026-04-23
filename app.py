import os
import json
import hashlib
import hmac
import time
import secrets
import calendar
from datetime import datetime, timedelta, timezone
import requests
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
# Collecte iOS (RSS public) + Android (google-play-scraper).
# Cache mémoire 24h + refresh via APScheduler au démarrage + toutes les 24h.

APPS = [
    {"name": "Awlad Quiz GO",        "iosId": "6737732771", "androidPkg": "com.bdouin.awladquiz"},
    {"name": "Awlad School",         "iosId": "1612014910", "androidPkg": "com.bdouin.awladschool"},
]

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
    """Pull iOS reviews from all configured countries, dedup."""
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


# =====================================================================
# Background scheduler — refresh reviews every 24h
# =====================================================================

def _init_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler(daemon=True, timezone="UTC")
        # Run now (startup) + every 24h
        scheduler.add_job(_refresh_reviews, "interval", hours=24, id="reviews_refresh",
                          next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30))
        scheduler.start()
    except Exception as e:
        # Non-fatal, reviews fall back to lazy refresh
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

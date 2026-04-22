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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

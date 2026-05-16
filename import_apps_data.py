#!/usr/bin/env python3
"""
import_apps_data.py — Importe les exports CSV du backend in-house des apps BDouin
(Awlad Quiz, Awlad School, BDouin Maker) dans Postgres Railway.

Source : data/raw/apps/<snapshot>/customers.csv + purchases.csv
Tables : apps_customers (PK account_id), apps_purchases (PK transaction_identifier)

Idempotent : ON CONFLICT DO UPDATE — rejouable sans doublons.

Usage:
    python3 import_apps_data.py --snapshot 2026-05-15
    python3 import_apps_data.py --snapshot 2026-05-15 --dry-run
"""

import argparse
import csv
import os
import sys
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW_DIR = os.path.join(ROOT, 'data', 'raw', 'apps')

DDL_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS apps_customers (
    account_id                      TEXT PRIMARY KEY,
    email                           TEXT,
    name                            TEXT,
    account_creation_datetime       TIMESTAMPTZ,
    apps                            TEXT,
    first_purchase_creation_datetime TIMESTAMPTZ,
    last_purchase_creation_datetime  TIMESTAMPTZ,
    purchase_count                  INTEGER,
    total_paid_usd                  NUMERIC(12,2),
    currencies                      TEXT,
    active_subscription_status      TEXT,
    snapshot_date                   DATE NOT NULL,
    imported_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

DDL_PURCHASES = """
CREATE TABLE IF NOT EXISTS apps_purchases (
    transaction_identifier      TEXT PRIMARY KEY,
    app                         TEXT,
    account_id                  TEXT,
    email                       TEXT,
    name                        TEXT,
    account_creation_datetime   TIMESTAMPTZ,
    product_name                TEXT,
    purchase_type               TEXT,
    creation_datetime           TIMESTAMPTZ,
    amount_paid                 NUMERIC(12,4),
    currency                    TEXT,
    amount_paid_usd             NUMERIC(12,4),
    status                      TEXT,
    snapshot_date               DATE NOT NULL,
    imported_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_apps_purchases_account ON apps_purchases(account_id);
CREATE INDEX IF NOT EXISTS idx_apps_purchases_app     ON apps_purchases(app);
CREATE INDEX IF NOT EXISTS idx_apps_purchases_created ON apps_purchases(creation_datetime);
"""


def _to_ts(s):
    s = (s or '').strip()
    if not s:
        return None
    # Accepte 2024-01-31T23:05:41.843Z et variantes
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except ValueError:
        return None


def _to_int(s):
    s = (s or '').strip()
    return int(s) if s else None


def _to_num(s):
    s = (s or '').strip()
    return float(s) if s else None


def load_customers(path, snapshot_date):
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                r['account_id'],
                r.get('email') or None,
                r.get('name') or None,
                _to_ts(r.get('account_creation_datetime')),
                r.get('apps') or None,
                _to_ts(r.get('first_purchase_creation_datetime')),
                _to_ts(r.get('last_purchase_creation_datetime')),
                _to_int(r.get('purchase_count')),
                _to_num(r.get('total_paid_usd')),
                r.get('currencies') or None,
                r.get('active_subscription_status') or None,
                snapshot_date,
            ))
    return rows


def load_purchases(path, snapshot_date):
    rows = []
    seen = set()
    dupes = 0
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            txid = r['transaction_identifier']
            if txid in seen:
                dupes += 1
                continue
            seen.add(txid)
            rows.append((
                txid,
                r.get('app') or None,
                r.get('account_id') or None,
                r.get('email') or None,
                r.get('name') or None,
                _to_ts(r.get('account_creation_datetime')),
                r.get('product_name') or None,
                r.get('purchase_type') or None,
                _to_ts(r.get('creation_datetime')),
                _to_num(r.get('amount_paid')),
                r.get('currency') or None,
                _to_num(r.get('amount_paid_usd')),
                r.get('status') or None,
                snapshot_date,
            ))
    if dupes:
        print(f'  (dédup intra-CSV : {dupes} doublons transaction_identifier ignorés)')
    return rows


UPSERT_CUSTOMERS = """
INSERT INTO apps_customers (
    account_id, email, name, account_creation_datetime, apps,
    first_purchase_creation_datetime, last_purchase_creation_datetime,
    purchase_count, total_paid_usd, currencies, active_subscription_status,
    snapshot_date
) VALUES %s
ON CONFLICT (account_id) DO UPDATE SET
    email                            = EXCLUDED.email,
    name                             = EXCLUDED.name,
    account_creation_datetime        = EXCLUDED.account_creation_datetime,
    apps                             = EXCLUDED.apps,
    first_purchase_creation_datetime = EXCLUDED.first_purchase_creation_datetime,
    last_purchase_creation_datetime  = EXCLUDED.last_purchase_creation_datetime,
    purchase_count                   = EXCLUDED.purchase_count,
    total_paid_usd                   = EXCLUDED.total_paid_usd,
    currencies                       = EXCLUDED.currencies,
    active_subscription_status       = EXCLUDED.active_subscription_status,
    snapshot_date                    = EXCLUDED.snapshot_date,
    imported_at                      = NOW();
"""

UPSERT_PURCHASES = """
INSERT INTO apps_purchases (
    transaction_identifier, app, account_id, email, name,
    account_creation_datetime, product_name, purchase_type,
    creation_datetime, amount_paid, currency, amount_paid_usd, status,
    snapshot_date
) VALUES %s
ON CONFLICT (transaction_identifier) DO UPDATE SET
    app                       = EXCLUDED.app,
    account_id                = EXCLUDED.account_id,
    email                     = EXCLUDED.email,
    name                      = EXCLUDED.name,
    account_creation_datetime = EXCLUDED.account_creation_datetime,
    product_name              = EXCLUDED.product_name,
    purchase_type             = EXCLUDED.purchase_type,
    creation_datetime         = EXCLUDED.creation_datetime,
    amount_paid               = EXCLUDED.amount_paid,
    currency                  = EXCLUDED.currency,
    amount_paid_usd           = EXCLUDED.amount_paid_usd,
    status                    = EXCLUDED.status,
    snapshot_date             = EXCLUDED.snapshot_date,
    imported_at               = NOW();
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', required=True, help='Dossier daté ex: 2026-05-15')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    snap_dir = os.path.join(RAW_DIR, args.snapshot)
    cust_path = os.path.join(snap_dir, 'customers.csv')
    purch_path = os.path.join(snap_dir, 'purchases.csv')

    for p in (cust_path, purch_path):
        if not os.path.exists(p):
            sys.exit(f'Missing: {p}')

    snapshot_date = datetime.strptime(args.snapshot, '%Y-%m-%d').date()

    print(f'Loading customers from {cust_path}…')
    customers = load_customers(cust_path, snapshot_date)
    print(f'  {len(customers)} rows')

    print(f'Loading purchases from {purch_path}…')
    purchases = load_purchases(purch_path, snapshot_date)
    print(f'  {len(purchases)} rows')

    if args.dry_run:
        print('Dry-run, no DB writes.')
        return

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_CUSTOMERS)
            cur.execute(DDL_PURCHASES)
            execute_values(cur, UPSERT_CUSTOMERS, customers, page_size=500)
            execute_values(cur, UPSERT_PURCHASES, purchases, page_size=1000)
            cur.execute('SELECT COUNT(*) FROM apps_customers')
            n_cust = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM apps_purchases')
            n_purch = cur.fetchone()[0]
        conn.commit()
        print(f'OK — apps_customers: {n_cust} | apps_purchases: {n_purch}')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()

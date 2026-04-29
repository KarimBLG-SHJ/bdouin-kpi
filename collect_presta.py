#!/usr/bin/env python3
"""
collect_presta.py — Aspire TOUT PrestaShop dans Railway Postgres.

Tables créées :
  presta_orders          — toutes les commandes
  presta_order_details   — lignes de commande (quel livre, qté, prix)
  presta_customers       — profil client complet
  presta_addresses       — adresses livraison/facturation (téléphone, pays, ville)
  presta_order_histories — historique statuts commande
  presta_order_payments  — paiements
  presta_order_invoices  — factures
  presta_products        — catalogue (nom, EAN, prix, description)
  presta_stock_movements — mouvements de stock (réappro + ventes)
  presta_carts           — paniers (y compris abandonnés)
  presta_cart_rules      — codes promo utilisés

Usage:
    python3 collect_presta.py
    python3 collect_presta.py --resource orders  # une table seulement
"""

import argparse
import json
import time
import psycopg2
import requests
from psycopg2.extras import execute_values

PRESTA_BASE = "https://www.bdouin.com/api"
PRESTA_KEY  = "AU83IAKGBTE3SRAIW85IFLZ8642AXQPH"
DB_URL      = "postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway"

BATCH = 500   # records per API request
SLEEP = 0.1   # seconds between requests


def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


def safe_json(obj):
    return json.dumps(obj, ensure_ascii=False, default=str).replace('\x00', '')


def presta_get(resource, params=None):
    """Fetch one page from PrestaShop API."""
    p = {"output_format": "JSON", **(params or {})}
    r = requests.get(f"{PRESTA_BASE}/{resource}", auth=(PRESTA_KEY, ""), params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_all_ids(resource):
    """Return all IDs for a resource."""
    data = presta_get(resource, {"display": "[id]", "limit": 1000000})
    key = list(data.keys())[0]
    return [int(x["id"]) for x in data[key]] if data[key] else []


def fetch_bulk(resource, ids, fields=None):
    """Fetch records for a list of IDs in one API call (id_in filter)."""
    id_str = "|".join(str(i) for i in ids)
    params = {
        "display": "full" if not fields else f"[{','.join(fields)}]",
        "filter[id]": f"[{id_str}]",
    }
    data = presta_get(resource, params)
    key = list(data.keys())[0]
    return data[key] if isinstance(data[key], list) else []


# ─── Table setup ──────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS presta_orders (
    id              INTEGER PRIMARY KEY,
    id_customer     INTEGER,
    id_cart         INTEGER,
    id_address_delivery INTEGER,
    id_address_invoice  INTEGER,
    reference       TEXT,
    payment         TEXT,
    module          TEXT,
    current_state   INTEGER,
    date_add        TIMESTAMP,
    date_upd        TIMESTAMP,
    total_paid      NUMERIC(12,4),
    total_paid_real NUMERIC(12,4),
    total_products  NUMERIC(12,4),
    total_shipping  NUMERIC(12,4),
    total_discounts NUMERIC(12,4),
    invoice_date    TIMESTAMP,
    valid           SMALLINT,
    gift            SMALLINT,
    note            TEXT,
    associations    JSONB,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_order_details (
    id                      INTEGER PRIMARY KEY,
    id_order                INTEGER,
    product_id              INTEGER,
    product_name            TEXT,
    product_ean13           TEXT,
    product_reference       TEXT,
    product_quantity        INTEGER,
    product_quantity_return INTEGER,
    product_quantity_refunded INTEGER,
    unit_price_tax_incl     NUMERIC(12,4),
    unit_price_tax_excl     NUMERIC(12,4),
    total_price_tax_incl    NUMERIC(12,4),
    total_price_tax_excl    NUMERIC(12,4),
    reduction_percent       NUMERIC(6,2),
    reduction_amount        NUMERIC(12,4),
    collected_at            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_customers (
    id              INTEGER PRIMARY KEY,
    email           TEXT,
    firstname       TEXT,
    lastname        TEXT,
    id_gender       SMALLINT,
    birthday        DATE,
    newsletter      SMALLINT,
    optin           SMALLINT,
    company         TEXT,
    is_guest        SMALLINT,
    active          SMALLINT,
    deleted         SMALLINT,
    date_add        TIMESTAMP,
    date_upd        TIMESTAMP,
    associations    JSONB,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_addresses (
    id              INTEGER PRIMARY KEY,
    id_customer     INTEGER,
    id_country      INTEGER,
    id_state        INTEGER,
    alias           TEXT,
    company         TEXT,
    lastname        TEXT,
    firstname       TEXT,
    address1        TEXT,
    address2        TEXT,
    postcode        TEXT,
    city            TEXT,
    phone           TEXT,
    phone_mobile    TEXT,
    vat_number      TEXT,
    deleted         SMALLINT,
    date_add        TIMESTAMP,
    date_upd        TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_order_histories (
    id              INTEGER PRIMARY KEY,
    id_order        INTEGER,
    id_order_state  INTEGER,
    id_employee     INTEGER,
    date_add        TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_order_payments (
    id              INTEGER PRIMARY KEY,
    id_order        INTEGER,
    order_reference TEXT,
    amount          NUMERIC(12,4),
    payment_method  TEXT,
    transaction_id  TEXT,
    date_add        TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_order_invoices (
    id              INTEGER PRIMARY KEY,
    id_order        INTEGER,
    number          INTEGER,
    delivery_date   TIMESTAMP,
    date_add        TIMESTAMP,
    total_paid_tax_excl NUMERIC(12,4),
    total_paid_tax_incl NUMERIC(12,4),
    total_products  NUMERIC(12,4),
    total_shipping_tax_excl NUMERIC(12,4),
    total_discount_tax_excl NUMERIC(12,4),
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_products (
    id              INTEGER PRIMARY KEY,
    reference       TEXT,
    ean13           TEXT,
    isbn            TEXT,
    name            TEXT,
    description     TEXT,
    description_short TEXT,
    price           NUMERIC(12,4),
    wholesale_price NUMERIC(12,4),
    active          SMALLINT,
    quantity        INTEGER,
    date_add        TIMESTAMP,
    date_upd        TIMESTAMP,
    associations    JSONB,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_stock_movements (
    id              INTEGER PRIMARY KEY,
    id_product      INTEGER,
    id_order        INTEGER,
    product_name    TEXT,
    ean13           TEXT,
    reference       TEXT,
    physical_quantity INTEGER,
    sign            SMALLINT,
    date_add        TIMESTAMP,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_carts (
    id              INTEGER PRIMARY KEY,
    id_customer     INTEGER,
    id_guest        INTEGER,
    id_currency     INTEGER,
    id_lang         INTEGER,
    id_carrier      INTEGER,
    date_add        TIMESTAMP,
    date_upd        TIMESTAMP,
    associations    JSONB,
    collected_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presta_cart_rules (
    id              INTEGER PRIMARY KEY,
    name            TEXT,
    description     TEXT,
    code            TEXT,
    active          SMALLINT,
    date_from       TIMESTAMP,
    date_to         TIMESTAMP,
    quantity        INTEGER,
    quantity_per_user INTEGER,
    reduction_percent NUMERIC(6,2),
    reduction_amount NUMERIC(12,4),
    free_shipping   SMALLINT,
    gift_product    INTEGER,
    collected_at    TIMESTAMP DEFAULT NOW()
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_presta_orders_customer  ON presta_orders(id_customer);
CREATE INDEX IF NOT EXISTS idx_presta_orders_date      ON presta_orders(date_add);
CREATE INDEX IF NOT EXISTS idx_presta_orders_state     ON presta_orders(current_state);
CREATE INDEX IF NOT EXISTS idx_presta_od_order         ON presta_order_details(id_order);
CREATE INDEX IF NOT EXISTS idx_presta_od_product       ON presta_order_details(product_id);
CREATE INDEX IF NOT EXISTS idx_presta_od_ean           ON presta_order_details(product_ean13);
CREATE INDEX IF NOT EXISTS idx_presta_customers_email  ON presta_customers(email);
CREATE INDEX IF NOT EXISTS idx_presta_addr_customer    ON presta_addresses(id_customer);
CREATE INDEX IF NOT EXISTS idx_presta_hist_order       ON presta_order_histories(id_order);
CREATE INDEX IF NOT EXISTS idx_presta_hist_state       ON presta_order_histories(id_order_state);
CREATE INDEX IF NOT EXISTS idx_presta_pay_order        ON presta_order_payments(id_order);
CREATE INDEX IF NOT EXISTS idx_presta_inv_order        ON presta_order_invoices(id_order);
CREATE INDEX IF NOT EXISTS idx_presta_stock_product    ON presta_stock_movements(id_product);
CREATE INDEX IF NOT EXISTS idx_presta_stock_date       ON presta_stock_movements(date_add);
CREATE INDEX IF NOT EXISTS idx_presta_carts_customer   ON presta_carts(id_customer);
CREATE INDEX IF NOT EXISTS idx_presta_carts_date       ON presta_carts(date_add);
"""

# ─── Collectors ───────────────────────────────────────────────────────────────

def ts(v):
    """Parse timestamp string, return None if empty/invalid."""
    if not v or v in ('0000-00-00 00:00:00', ''):
        return None
    return v


def collect_orders(cur):
    print("\n[orders] Fetching all IDs...")
    ids = fetch_all_ids("orders")
    print(f"  {len(ids)} orders to process")

    cur.execute("SELECT id FROM presta_orders")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new (skipping {len(existing)} already stored)")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("orders", batch)
        rows = []
        for o in records:
            rows.append((
                int(o["id"]),
                int(o.get("id_customer") or 0) or None,
                int(o.get("id_cart") or 0) or None,
                int(o.get("id_address_delivery") or 0) or None,
                int(o.get("id_address_invoice") or 0) or None,
                o.get("reference"),
                o.get("payment"),
                o.get("module"),
                int(o.get("current_state") or 0) or None,
                ts(o.get("date_add")),
                ts(o.get("date_upd")),
                o.get("total_paid_tax_incl") or None,
                o.get("total_paid_real") or None,
                o.get("total_products_wt") or None,
                o.get("total_shipping_tax_incl") or None,
                o.get("total_discounts_tax_incl") or None,
                ts(o.get("invoice_date")),
                int(o.get("valid") or 0),
                int(o.get("gift") or 0),
                o.get("note"),
                safe_json(o.get("associations", {})),
            ))
        execute_values(cur, """
            INSERT INTO presta_orders
              (id,id_customer,id_cart,id_address_delivery,id_address_invoice,
               reference,payment,module,current_state,date_add,date_upd,
               total_paid,total_paid_real,total_products,total_shipping,
               total_discounts,invoice_date,valid,gift,note,associations)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
        print(f"  {min(i+BATCH, len(ids)+len(existing))}/{len(ids)+len(existing)} — {inserted} inserted")
    return inserted


def collect_order_details(cur):
    print("\n[order_details] Fetching all IDs...")
    ids = fetch_all_ids("order_details")
    print(f"  {len(ids)} records")

    cur.execute("SELECT id FROM presta_order_details")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("order_details", batch)
        rows = []
        for o in records:
            rows.append((
                int(o["id"]),
                int(o.get("id_order") or 0) or None,
                int(o.get("product_id") or 0) or None,
                o.get("product_name"),
                o.get("product_ean13") or None,
                o.get("product_reference") or None,
                int(o.get("product_quantity") or 0),
                int(o.get("product_quantity_return") or 0),
                int(o.get("product_quantity_refunded") or 0),
                o.get("unit_price_tax_incl") or None,
                o.get("unit_price_tax_excl") or None,
                o.get("total_price_tax_incl") or None,
                o.get("total_price_tax_excl") or None,
                o.get("reduction_percent") or None,
                o.get("reduction_amount") or None,
            ))
        execute_values(cur, """
            INSERT INTO presta_order_details
              (id,id_order,product_id,product_name,product_ean13,product_reference,
               product_quantity,product_quantity_return,product_quantity_refunded,
               unit_price_tax_incl,unit_price_tax_excl,total_price_tax_incl,
               total_price_tax_excl,reduction_percent,reduction_amount)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
        print(f"  {min(i+BATCH, len(ids)+len(existing))}/{len(ids)+len(existing)} — {inserted} inserted")
    return inserted


def collect_customers(cur):
    print("\n[customers] Fetching all IDs...")
    ids = fetch_all_ids("customers")
    print(f"  {len(ids)} customers")

    cur.execute("SELECT id FROM presta_customers")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("customers", batch)
        rows = []
        for c in records:
            bday = c.get("birthday")
            bday = bday if bday and bday != "0000-00-00" else None
            rows.append((
                int(c["id"]),
                c.get("email"),
                c.get("firstname"),
                c.get("lastname"),
                int(c.get("id_gender") or 0) or None,
                bday,
                int(c.get("newsletter") or 0),
                int(c.get("optin") or 0),
                c.get("company") or None,
                int(c.get("is_guest") or 0),
                int(c.get("active") or 0),
                int(c.get("deleted") or 0),
                ts(c.get("date_add")),
                ts(c.get("date_upd")),
                safe_json(c.get("associations", {})),
            ))
        execute_values(cur, """
            INSERT INTO presta_customers
              (id,email,firstname,lastname,id_gender,birthday,newsletter,optin,
               company,is_guest,active,deleted,date_add,date_upd,associations)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
        print(f"  {min(i+BATCH, len(ids)+len(existing))}/{len(ids)+len(existing)} — {inserted} inserted")
    return inserted


def collect_addresses(cur):
    print("\n[addresses] Fetching all IDs...")
    ids = fetch_all_ids("addresses")
    print(f"  {len(ids)} addresses")

    cur.execute("SELECT id FROM presta_addresses")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("addresses", batch)
        rows = []
        for a in records:
            rows.append((
                int(a["id"]),
                int(a.get("id_customer") or 0) or None,
                int(a.get("id_country") or 0) or None,
                int(a.get("id_state") or 0) or None,
                a.get("alias"),
                a.get("company") or None,
                a.get("lastname"),
                a.get("firstname"),
                a.get("address1"),
                a.get("address2") or None,
                a.get("postcode") or None,
                a.get("city"),
                a.get("phone") or None,
                a.get("phone_mobile") or None,
                a.get("vat_number") or None,
                int(a.get("deleted") or 0),
                ts(a.get("date_add")),
                ts(a.get("date_upd")),
            ))
        execute_values(cur, """
            INSERT INTO presta_addresses
              (id,id_customer,id_country,id_state,alias,company,lastname,firstname,
               address1,address2,postcode,city,phone,phone_mobile,vat_number,
               deleted,date_add,date_upd)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
        print(f"  {min(i+BATCH, len(ids)+len(existing))}/{len(ids)+len(existing)} — {inserted} inserted")
    return inserted


def collect_order_histories(cur):
    print("\n[order_histories] Fetching all IDs...")
    ids = fetch_all_ids("order_histories")
    print(f"  {len(ids)} histories")

    cur.execute("SELECT id FROM presta_order_histories")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("order_histories", batch)
        rows = [(
            int(h["id"]),
            int(h.get("id_order") or 0) or None,
            int(h.get("id_order_state") or 0) or None,
            int(h.get("id_employee") or 0) or None,
            ts(h.get("date_add")),
        ) for h in records]
        execute_values(cur, """
            INSERT INTO presta_order_histories (id,id_order,id_order_state,id_employee,date_add)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
    print(f"  {inserted} inserted")
    return inserted


def collect_order_payments(cur):
    print("\n[order_payments] Fetching all IDs...")
    ids = fetch_all_ids("order_payments")
    print(f"  {len(ids)} payments")

    cur.execute("SELECT id FROM presta_order_payments")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("order_payments", batch)
        rows = []
        for p in records:
            # order_payments may not have id_order directly — stored in order_reference
            rows.append((
                int(p["id"]),
                int(p.get("id_order") or 0) or None,
                p.get("order_reference") or None,
                p.get("amount") or None,
                p.get("payment_method") or None,
                p.get("transaction_id") or None,
                ts(p.get("date_add")),
            ))
        execute_values(cur, """
            INSERT INTO presta_order_payments
              (id,id_order,order_reference,amount,payment_method,transaction_id,date_add)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
    print(f"  {inserted} inserted")
    return inserted


def collect_order_invoices(cur):
    print("\n[order_invoices] Fetching all IDs...")
    ids = fetch_all_ids("order_invoices")
    print(f"  {len(ids)} invoices")

    cur.execute("SELECT id FROM presta_order_invoices")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("order_invoices", batch)
        rows = [(
            int(inv["id"]),
            int(inv.get("id_order") or 0) or None,
            int(inv.get("number") or 0) or None,
            ts(inv.get("delivery_date")),
            ts(inv.get("date_add")),
            inv.get("total_paid_tax_excl") or None,
            inv.get("total_paid_tax_incl") or None,
            inv.get("total_products") or None,
            inv.get("total_shipping_tax_excl") or None,
            inv.get("total_discount_tax_excl") or None,
        ) for inv in records]
        execute_values(cur, """
            INSERT INTO presta_order_invoices
              (id,id_order,number,delivery_date,date_add,total_paid_tax_excl,
               total_paid_tax_incl,total_products,total_shipping_tax_excl,total_discount_tax_excl)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
    print(f"  {inserted} inserted")
    return inserted


def collect_products(cur):
    print("\n[products] Fetching all...")
    ids = fetch_all_ids("products")
    print(f"  {len(ids)} products")

    cur.execute("SELECT id FROM presta_products")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]

    if not ids:
        print("  All already stored")
        return 0

    records = fetch_bulk("products", ids)
    rows = []
    for p in records:
        name = p.get("name")
        if isinstance(name, list):
            name = name[0].get("value", "") if name else ""
        desc = p.get("description")
        if isinstance(desc, list):
            desc = desc[0].get("value", "") if desc else ""
        desc_short = p.get("description_short")
        if isinstance(desc_short, list):
            desc_short = desc_short[0].get("value", "") if desc_short else ""
        rows.append((
            int(p["id"]),
            p.get("reference") or None,
            p.get("ean13") or None,
            p.get("isbn") or None,
            name,
            desc,
            desc_short,
            p.get("price") or None,
            p.get("wholesale_price") or None,
            int(p.get("active") or 0),
            int(p.get("quantity") or 0),
            ts(p.get("date_add")),
            ts(p.get("date_upd")),
            safe_json(p.get("associations", {})),
        ))
    execute_values(cur, """
        INSERT INTO presta_products
          (id,reference,ean13,isbn,name,description,description_short,
           price,wholesale_price,active,quantity,date_add,date_upd,associations)
        VALUES %s ON CONFLICT (id) DO NOTHING
    """, rows)
    print(f"  {len(rows)} inserted")
    return len(rows)


def collect_stock_movements(cur):
    print("\n[stock_movements] Fetching all IDs...")
    ids = fetch_all_ids("stock_movements")
    print(f"  {len(ids)} movements")

    cur.execute("SELECT id FROM presta_stock_movements")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("stock_movements", batch)
        rows = []
        for m in records:
            pname = m.get("product_name")
            if isinstance(pname, list):
                # pick first non-False value
                pname = next((x.get("value") for x in pname if x.get("value")), None)
            rows.append((
                int(m["id"]),
                int(m.get("id_product") or 0) or None,
                int(m.get("id_order") or 0) or None,
                pname,
                m.get("ean13") or None,
                m.get("reference") or None,
                int(m.get("physical_quantity") or 0),
                int(m.get("sign") or 0),
                ts(m.get("date_add")),
            ))
        execute_values(cur, """
            INSERT INTO presta_stock_movements
              (id,id_product,id_order,product_name,ean13,reference,physical_quantity,sign,date_add)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
    print(f"  {inserted} inserted")
    return inserted


def collect_carts(cur):
    print("\n[carts] Fetching all IDs...")
    ids = fetch_all_ids("carts")
    print(f"  {len(ids)} carts")

    cur.execute("SELECT id FROM presta_carts")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("carts", batch)
        rows = [(
            int(c["id"]),
            int(c.get("id_customer") or 0) or None,
            int(c.get("id_guest") or 0) or None,
            int(c.get("id_currency") or 0) or None,
            int(c.get("id_lang") or 0) or None,
            int(c.get("id_carrier") or 0) or None,
            ts(c.get("date_add")),
            ts(c.get("date_upd")),
            safe_json(c.get("associations", {})),
        ) for c in records]
        execute_values(cur, """
            INSERT INTO presta_carts
              (id,id_customer,id_guest,id_currency,id_lang,id_carrier,date_add,date_upd,associations)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
        print(f"  {min(i+BATCH, len(ids)+len(existing))}/{len(ids)+len(existing)} — {inserted} inserted")
    return inserted


def collect_cart_rules(cur):
    print("\n[cart_rules] Fetching all IDs...")
    ids = fetch_all_ids("cart_rules")
    print(f"  {len(ids)} cart_rules")

    cur.execute("SELECT id FROM presta_cart_rules")
    existing = {r[0] for r in cur.fetchall()}
    ids = [i for i in ids if i not in existing]
    print(f"  {len(ids)} new")

    inserted = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        records = fetch_bulk("cart_rules", batch)
        rows = []
        for r in records:
            name = r.get("name")
            if isinstance(name, list):
                name = name[0].get("value", "") if name else ""
            desc = r.get("description")
            if isinstance(desc, list):
                desc = desc[0].get("value", "") if desc else ""
            rows.append((
                int(r["id"]),
                name,
                desc,
                r.get("code") or None,
                int(r.get("active") or 0),
                ts(r.get("date_from")),
                ts(r.get("date_to")),
                int(r.get("quantity") or 0),
                int(r.get("quantity_per_user") or 0),
                r.get("reduction_percent") or None,
                r.get("reduction_amount") or None,
                int(r.get("free_shipping") or 0),
                int(r.get("gift_product") or 0) or None,
            ))
        execute_values(cur, """
            INSERT INTO presta_cart_rules
              (id,name,description,code,active,date_from,date_to,quantity,
               quantity_per_user,reduction_percent,reduction_amount,free_shipping,gift_product)
            VALUES %s ON CONFLICT (id) DO NOTHING
        """, rows)
        inserted += len(rows)
        time.sleep(SLEEP)
    print(f"  {inserted} inserted")
    return inserted


# ─── Main ─────────────────────────────────────────────────────────────────────

COLLECTORS = {
    "orders":           collect_orders,
    "order_details":    collect_order_details,
    "customers":        collect_customers,
    "addresses":        collect_addresses,
    "order_histories":  collect_order_histories,
    "order_payments":   collect_order_payments,
    "order_invoices":   collect_order_invoices,
    "products":         collect_products,
    "stock_movements":  collect_stock_movements,
    "carts":            collect_carts,
    "cart_rules":       collect_cart_rules,
}


def main(resource_filter=None):
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    print("Setting up tables...")
    cur.execute(DDL)
    cur.execute(INDEXES)
    conn.commit()
    print("✓ Tables ready")

    targets = [resource_filter] if resource_filter else list(COLLECTORS.keys())
    total = 0

    for name in targets:
        if name not in COLLECTORS:
            print(f"Unknown resource: {name}")
            continue
        try:
            n = COLLECTORS[name](cur)
            conn.commit()
            total += n
            print(f"  ✓ {name}: {n} new rows committed")
        except Exception as e:
            conn.rollback()
            print(f"  ✗ {name}: {e}")

    # Final counts
    print("\n" + "="*50)
    print("FINAL COUNTS:")
    for t in [f"presta_{r.replace('_', '_')}" for r in COLLECTORS.keys()]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t}: {cur.fetchone()[0]}")

    conn.close()
    print(f"\nDone! Total new rows: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resource", help="Collect only one resource (e.g. orders)")
    args = parser.parse_args()
    main(args.resource)

#!/usr/bin/env python3
"""
pipeline_20_print_history.py — gold.print_history

Source principale: gold.print_runs (bills Zoho déjà importés)
Enrichit avec:
  - print_month    : extrait du description_raw (JUIN 2024, MARS 2024, 2025-SEPT, etc.)
  - run_number     : numéro de tirage par catalog_id (1=1er tirage, 2=réimpression, etc.)
  - first_presta_sale_date : 1ère commande PrestaShop valide par produit
  - days_to_market : délai entre print_month et 1ère vente

À relancer après chaque import de nouveaux bills dans gold.print_runs.
"""

import re
import os
import psycopg2
from datetime import date

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

# Mapping nom de mois (français + anglais, prefixes) → numéro
MONTH_MAP = {
    'jan': 1, 'january': 1, 'janvier': 1,
    'fev': 2, 'feb': 2, 'february': 2, 'février': 2, 'fevrier': 2,
    'mar': 3, 'mars': 3, 'march': 3,
    'avr': 4, 'apr': 4, 'april': 4, 'avril': 4,
    'mai': 5, 'may': 5,
    'juin': 6, 'jun': 6, 'june': 6,
    'juil': 7, 'jul': 7, 'july': 7, 'juillet': 7,
    'aou': 8, 'aug': 8, 'august': 8, 'août': 8, 'aout': 8,
    'sep': 9, 'sept': 9, 'september': 9, 'septembre': 9,
    'oct': 10, 'october': 10, 'octobre': 10,
    'nov': 11, 'november': 11, 'novembre': 11,
    'dec': 12, 'december': 12, 'décembre': 12, 'decembre': 12,
}

MONTH_PATTERN = (
    r'jan(?:vier|uary)?|f[eé]v(?:rier)?|feb(?:ruary)?|mars|march|'
    r'avr(?:il)?|apr(?:il)?|mai|may|juin|jun(?:e)?|juil(?:let)?|jul(?:y)?|'
    r'ao[uû]t?|aug(?:ust)?|sept?(?:embre)?|sep(?:tember)?|'
    r'oct(?:obre|ober)?|nov(?:embre|ember)?|d[eé]c(?:embre|ember)?'
)

# Priorité 1: "YYYY[-\s]MONTH" ou "MONTH[-\s]YYYY"  (année explicite)
RE_YEAR_MONTH = re.compile(
    r'(20\d{2})\s*[-_]?\s*(' + MONTH_PATTERN + r')\b',
    re.IGNORECASE
)
RE_MONTH_YEAR = re.compile(
    r'\b(' + MONTH_PATTERN + r')\s*[-_]?\s*(20\d{2})\b',
    re.IGNORECASE
)
# Priorité 2: mois seul en début de description (format JUIN / MARS / etc.)
RE_MONTH_ONLY = re.compile(
    r'^(' + MONTH_PATTERN + r')\b',
    re.IGNORECASE
)


def _month_num(token: str) -> int | None:
    t = token.lower().strip()
    for key, num in MONTH_MAP.items():
        if t.startswith(key):
            return num
    return None


def extract_print_month(desc: str, bill_date: date) -> date | None:
    """Extrait la date de tirage (1er du mois) depuis description_raw."""
    if not desc:
        return None

    # Priorité 1a: YYYY-Month
    m = RE_YEAR_MONTH.search(desc)
    if m:
        year = int(m.group(1))
        month = _month_num(m.group(2))
        if month:
            return date(year, month, 1)

    # Priorité 1b: Month YYYY
    m = RE_MONTH_YEAR.search(desc)
    if m:
        month = _month_num(m.group(1))
        year = int(m.group(2))
        if month:
            return date(year, month, 1)

    # Priorité 2: mois seul en début (ex: "JUIN - FAMILLE FOULANE #1")
    m = RE_MONTH_ONLY.match(desc.strip())
    if m:
        month = _month_num(m.group(1))
        if month:
            # Année = bill_date.year sauf si le mois est nettement après la date de facture
            # (tolérance 2 mois : le print précède toujours la facture)
            year = bill_date.year
            if bill_date and month > bill_date.month + 2:
                year = bill_date.year - 1
            return date(year, month, 1)

    return None


DDL = """
CREATE TABLE IF NOT EXISTS gold.print_history (
    id                   SERIAL PRIMARY KEY,
    catalog_id           INTEGER REFERENCES gold.catalog(catalog_id),
    canonical_name       TEXT,
    run_number           INTEGER,        -- 1 = 1er tirage, 2 = 1ère réimpression, …
    print_month          DATE,           -- 1er du mois d'impression (ex: 2024-06-01)
    bill_date            DATE,           -- date facture Sofiadis/Zoho
    quantity             INTEGER,
    cost_eur             NUMERIC(12,2),
    source               TEXT,           -- 'zoho_print_runs'
    bill_ref             TEXT,
    run_id               INTEGER,        -- FK gold.print_runs.run_id
    first_presta_sale_date DATE,         -- 1ère commande PrestaShop valide
    days_to_market       INTEGER,        -- first_presta_sale_date - print_month
    created_at           TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ph_catalog_id  ON gold.print_history(catalog_id);
CREATE INDEX IF NOT EXISTS ph_print_month ON gold.print_history(print_month);
"""

INSERT_SQL = """
INSERT INTO gold.print_history
    (catalog_id, canonical_name, run_number, print_month, bill_date,
     quantity, cost_eur, source, bill_ref, run_id,
     first_presta_sale_date, days_to_market)
VALUES %s
"""


def get_first_sale_dates(cur) -> dict:
    """Retourne {catalog_id: first_sale_date} depuis gold.orders + presta_order_details."""
    cur.execute("""
        SELECT c.catalog_id, MIN(o.ordered_at)::date AS first_sale
        FROM gold.catalog c
        JOIN public.presta_order_details od
          ON od.product_ean13 = c.ean13 AND c.ean13 IS NOT NULL AND c.ean13 <> ''
        JOIN gold.orders o ON o.order_id = od.id_order AND o.is_valid = TRUE
        GROUP BY c.catalog_id
        UNION
        SELECT c.catalog_id, MIN(o.ordered_at)::date AS first_sale
        FROM gold.catalog c
        JOIN public.presta_order_details od
          ON od.product_reference = c.ref_presta
          AND c.ref_presta IS NOT NULL AND c.ref_presta <> ''
        JOIN gold.orders o ON o.order_id = od.id_order AND o.is_valid = TRUE
        GROUP BY c.catalog_id
    """)
    result = {}
    for row in cur.fetchall():
        cid, fs = row
        if cid not in result or (fs and (result[cid] is None or fs < result[cid])):
            result[cid] = fs
    return result


def run():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Créer la table
    cur.execute(DDL)
    conn.commit()
    print("✓ gold.print_history table ready")

    # Charger les print_runs (impressions uniquement, pas logistics)
    cur.execute("""
        SELECT run_id, catalog_id, matched_canonical, bill_date,
               description_raw, quantity, item_total, bill_number
        FROM gold.print_runs
        WHERE line_kind = 'print'
          AND catalog_id IS NOT NULL
        ORDER BY catalog_id, bill_date
    """)
    runs = cur.fetchall()
    print(f"  {len(runs)} print runs chargés depuis gold.print_runs")

    # Charger les premières dates de vente PrestaShop
    first_sales = get_first_sale_dates(cur)
    print(f"  {len(first_sales)} catalog_id avec 1ère vente PrestaShop")

    # Extraire print_month + calculer run_number par catalog_id
    by_catalog: dict[int, list] = {}
    for (run_id, catalog_id, canonical, bill_date, desc_raw, qty, cost, bill_ref) in runs:
        pm = extract_print_month(desc_raw or '', bill_date)
        by_catalog.setdefault(catalog_id, []).append({
            'run_id':    run_id,
            'canonical': canonical,
            'bill_date': bill_date,
            'print_month': pm,
            'qty':       int(qty) if qty else None,
            'cost':      float(cost) if cost else None,
            'bill_ref':  bill_ref,
        })

    # Trier par print_month (None en dernier) pour attribuer run_number
    rows = []
    for catalog_id, entries in by_catalog.items():
        entries.sort(key=lambda x: (x['print_month'] or date(9999, 1, 1)))
        fs = first_sales.get(catalog_id)
        for i, e in enumerate(entries, start=1):
            pm = e['print_month']
            dtm = (fs - pm).days if (fs and pm) else None
            rows.append((
                catalog_id, e['canonical'], i,
                pm, e['bill_date'],
                e['qty'], e['cost'],
                'zoho_print_runs', e['bill_ref'], e['run_id'],
                fs, dtm,
            ))

    # Upsert: vider et réinsérer (idempotent)
    cur.execute("TRUNCATE gold.print_history RESTART IDENTITY")
    from psycopg2.extras import execute_values as ev
    ev(cur, INSERT_SQL, rows)
    conn.commit()
    print(f"✓ {len(rows)} lignes insérées dans gold.print_history")

    # Rapport
    cur.execute("""
        SELECT canonical_name, run_number, print_month, quantity,
               first_presta_sale_date, days_to_market
        FROM gold.print_history
        ORDER BY print_month NULLS LAST, canonical_name, run_number
    """)
    print(f"\n{'Titre':<45} {'Run':>3} {'Print':>10} {'Qté':>6} {'1ère vente':>12} {'J':>5}")
    print("-" * 90)
    for r in cur.fetchall():
        name, rn, pm, qty, fs, dtm = r
        print(f"{(name or '')[:44]:<45} {rn:>3} {str(pm or '?'):>10} {(qty or 0):>6,} {str(fs or '?'):>12} {str(dtm or '?'):>5}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    run()

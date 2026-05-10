#!/usr/bin/env python3
"""
pipeline_22_catalog_performance.py — gold.catalog_performance + gold.sales_timeline

Pour chaque titre du catalogue :
  - Date de 1ère mise en vente (PrestaShop)
  - Email de lancement (date, audience, taux d'ouverture)
  - Historique des tirages (nb runs, 1er tirage, dernier tirage, total imprimé)
  - Ventes B2C individuelles (PrestaShop, unités seules)
  - Ventes B2C packs (approximation — réconciliation à la marge)
  - Ventes B2B Sofiadis (mensuel, depuis 2020)
  - Totaux et stock résiduel estimé

Tables produites :
  gold.catalog_performance  — 1 ligne par catalog_id
  gold.sales_timeline       — 1 ligne par catalog_id × mois × canal
"""

import os
import psycopg2
from psycopg2.extras import execute_values

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

DDL = """
CREATE TABLE IF NOT EXISTS gold.catalog_performance (
    catalog_id              INTEGER PRIMARY KEY REFERENCES gold.catalog(catalog_id),
    canonical_name          TEXT,
    series                  TEXT,
    tome_number             INTEGER,
    is_pack                 BOOLEAN,
    prix_public_ttc         NUMERIC(10,4),

    -- Lancement
    first_presta_sale_date  DATE,
    launch_email_date       DATE,
    launch_email_audience   INTEGER,
    launch_email_open_rate  NUMERIC(5,2),

    -- Tirages (depuis gold.print_history)
    print_runs_count        INTEGER  DEFAULT 0,
    first_print_month       DATE,
    latest_print_month      DATE,
    total_printed           INTEGER  DEFAULT 0,
    total_print_cost_eur    NUMERIC(12,2),

    -- B2C PrestaShop — unités individuelles
    b2c_qty_sold            INTEGER  DEFAULT 0,
    b2c_qty_returned        INTEGER  DEFAULT 0,
    b2c_qty_net             INTEGER  DEFAULT 0,
    b2c_revenue_ttc         NUMERIC(12,2),
    b2c_first_sale          DATE,
    b2c_last_sale           DATE,

    -- B2C packs (approximatif — unités de pack, pas livres individuels)
    b2c_pack_units_sold     INTEGER  DEFAULT 0,

    -- B2B Sofiadis
    b2b_qty_sold            INTEGER  DEFAULT 0,
    b2b_qty_returned        INTEGER  DEFAULT 0,
    b2b_qty_net             INTEGER  DEFAULT 0,
    b2b_revenue_ht          NUMERIC(12,2),
    b2b_first_sale_month    TEXT,
    b2b_last_sale_month     TEXT,

    -- Totaux
    total_qty_net           INTEGER  DEFAULT 0,
    total_revenue_eur       NUMERIC(12,2),

    -- Stock résiduel estimé (tirage - ventes totales)
    stock_remaining_est     INTEGER,

    updated_at              TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS cp_series ON gold.catalog_performance(series);

CREATE TABLE IF NOT EXISTS gold.sales_timeline (
    catalog_id  INTEGER REFERENCES gold.catalog(catalog_id),
    month       DATE NOT NULL,          -- 1er du mois
    channel     TEXT NOT NULL,          -- 'b2c' / 'b2b' / 'b2c_pack'
    qty_sold    INTEGER  DEFAULT 0,
    qty_net     INTEGER  DEFAULT 0,
    revenue     NUMERIC(12,2),
    PRIMARY KEY (catalog_id, month, channel)
);
CREATE INDEX IF NOT EXISTS st_catalog ON gold.sales_timeline(catalog_id);
CREATE INDEX IF NOT EXISTS st_month   ON gold.sales_timeline(month);
"""


def run():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute(DDL)
    conn.commit()
    print("✓ Tables créées")

    # ------------------------------------------------------------------ #
    # 1. Base catalogue
    # ------------------------------------------------------------------ #
    cur.execute("""
        SELECT catalog_id, canonical_name, series, tome_number, is_pack, prix_public_ttc
        FROM gold.catalog
        WHERE canonical_name IS NOT NULL
        ORDER BY catalog_id
    """)
    catalog = {r[0]: r for r in cur.fetchall()}
    print(f"  {len(catalog)} entrées catalogue")

    # ------------------------------------------------------------------ #
    # 2. Launch email (1er email de lancement par catalog_id)
    # ------------------------------------------------------------------ #
    cur.execute("""
        SELECT DISTINCT ON (cid)
            unnest(catalog_ids) AS cid,
            event_date, audience_size, open_rate
        FROM gold.marketing_events
        WHERE catalog_ids IS NOT NULL
          AND event_type IN ('launch', 'newsletter')
        ORDER BY cid, event_date ASC
    """)
    launch_email = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}
    print(f"  {len(launch_email)} titres avec email de lancement")

    # ------------------------------------------------------------------ #
    # 3. Tirages (gold.print_history)
    # ------------------------------------------------------------------ #
    cur.execute("""
        SELECT catalog_id,
               COUNT(*)                AS runs,
               MIN(print_month)        AS first_pm,
               MAX(print_month)        AS last_pm,
               SUM(quantity)           AS total_qty,
               SUM(cost_eur)           AS total_cost
        FROM gold.print_history
        WHERE catalog_id IS NOT NULL
        GROUP BY catalog_id
    """)
    prints = {r[0]: r[1:] for r in cur.fetchall()}
    print(f"  {len(prints)} titres avec historique de tirage")

    B2B_EMAILS = ('km@sofiadis.fr', 'darelfikr13@outlook.fr', 'safwaboutique13@gmail.com')

    # ------------------------------------------------------------------ #
    # 4. B2C — unités individuelles (non-packs, non-lots)
    # ------------------------------------------------------------------ #
    cur.execute("""
        SELECT
            c.catalog_id,
            SUM(od.product_quantity)                          AS qty_sold,
            SUM(od.product_quantity_return)                   AS qty_ret,
            SUM(od.product_quantity - od.product_quantity_return) AS qty_net,
            SUM(od.total_price_tax_incl)                      AS rev_ttc,
            MIN(o.ordered_at)::date                           AS first_sale,
            MAX(o.ordered_at)::date                           AS last_sale
        FROM presta_order_details od
        JOIN gold.orders o ON o.order_id = od.id_order AND o.is_valid = TRUE
        JOIN gold.catalog c
            ON (c.ean13 = od.product_ean13 AND od.product_ean13 IS NOT NULL AND od.product_ean13 <> '')
            OR (c.ref_presta = od.product_reference AND od.product_reference IS NOT NULL AND od.product_reference <> '')
        WHERE c.is_pack = FALSE
          AND od.product_name NOT ILIKE 'Lot -%%'
          AND o.email NOT IN %s
        GROUP BY c.catalog_id
    """, (B2B_EMAILS,))
    b2c_ind = {r[0]: r[1:] for r in cur.fetchall()}
    print(f"  {len(b2c_ind)} titres avec ventes B2C individuelles")

    # ------------------------------------------------------------------ #
    # 5. B2C — packs (unités de pack vendues, pas livres individuels)
    # ------------------------------------------------------------------ #
    cur.execute("""
        SELECT
            c.catalog_id,
            SUM(od.product_quantity)    AS pack_units
        FROM presta_order_details od
        JOIN gold.orders o ON o.order_id = od.id_order AND o.is_valid = TRUE
        JOIN gold.catalog c
            ON (c.ean13 = od.product_ean13 AND od.product_ean13 IS NOT NULL AND od.product_ean13 <> '')
            OR (c.ref_presta = od.product_reference AND od.product_reference IS NOT NULL AND od.product_reference <> '')
        WHERE c.is_pack = TRUE
          AND o.email NOT IN %s
        GROUP BY c.catalog_id
    """, (B2B_EMAILS,))
    b2c_packs = {r[0]: r[1] for r in cur.fetchall()}
    print(f"  {len(b2c_packs)} packs avec ventes B2C")

    # ------------------------------------------------------------------ #
    # 6. B2B Sofiadis
    # ------------------------------------------------------------------ #
    cur.execute("""
        SELECT
            catalog_id,
            SUM(ventes)         AS qty_sold,
            SUM(retours)        AS qty_ret,
            SUM(total_net)      AS qty_net,
            SUM(montant_ht)     AS rev_ht,
            MIN(month)          AS first_month,
            MAX(month)          AS last_month
        FROM gold.b2b_sofiadis_sales
        WHERE catalog_id IS NOT NULL
        GROUP BY catalog_id
    """)
    b2b = {r[0]: r[1:] for r in cur.fetchall()}
    print(f"  {len(b2b)} titres avec ventes B2B Sofiadis")

    # ------------------------------------------------------------------ #
    # 7. Assembler gold.catalog_performance
    # ------------------------------------------------------------------ #
    perf_rows = []
    for cid, (cid2, name, series, tome, is_pack, prix) in catalog.items():
        le = launch_email.get(cid, (None, None, None))
        pr = prints.get(cid, (0, None, None, 0, 0))
        bi = b2c_ind.get(cid, (0, 0, 0, 0, None, None))
        bp = b2c_packs.get(cid, 0)
        bb = b2b.get(cid, (0, 0, 0, 0, None, None))

        b2c_net = int(bi[2] or 0)
        b2b_net = int(bb[2] or 0)
        total_net = b2c_net + b2b_net
        total_rev = (bi[3] or 0) + (bb[3] or 0)
        total_printed = int(pr[3] or 0)
        stock_est = (total_printed - total_net) if total_printed > 0 else None

        perf_rows.append((
            cid, name, series, tome, is_pack, prix,
            # launch
            bi[4],            # first_presta_sale_date
            le[0],            # launch_email_date
            le[1],            # launch_email_audience
            float(le[2]) if le[2] else None,  # launch_email_open_rate
            # prints
            int(pr[0] or 0), pr[1], pr[2],
            int(pr[3] or 0), float(pr[4] or 0),
            # b2c individual
            int(bi[0] or 0), int(bi[1] or 0), b2c_net,
            float(bi[3] or 0), bi[4], bi[5],
            # b2c packs
            int(bp or 0),
            # b2b
            int(bb[0] or 0), int(bb[1] or 0), b2b_net,
            float(bb[3] or 0),
            str(bb[4]) if bb[4] else None,
            str(bb[5]) if bb[5] else None,
            # totals
            total_net, float(total_rev), stock_est,
        ))

    cur.execute("TRUNCATE gold.catalog_performance")
    execute_values(cur, """
        INSERT INTO gold.catalog_performance VALUES %s
    """, perf_rows)
    conn.commit()
    print(f"✓ {len(perf_rows)} titres dans gold.catalog_performance")

    # ------------------------------------------------------------------ #
    # 8. gold.sales_timeline — mensuel par canal
    # ------------------------------------------------------------------ #

    # B2C mensuel
    cur.execute("""
        SELECT c.catalog_id,
               DATE_TRUNC('month', o.ordered_at)::date               AS month,
               SUM(od.product_quantity)                               AS qty_sold,
               SUM(od.product_quantity - od.product_quantity_return)  AS qty_net,
               SUM(od.total_price_tax_incl)                           AS revenue
        FROM presta_order_details od
        JOIN gold.orders o ON o.order_id = od.id_order AND o.is_valid = TRUE
        JOIN gold.catalog c
            ON (c.ean13 = od.product_ean13 AND od.product_ean13 IS NOT NULL AND od.product_ean13 <> '')
            OR (c.ref_presta = od.product_reference AND od.product_reference IS NOT NULL AND od.product_reference <> '')
        WHERE c.is_pack = FALSE
          AND od.product_name NOT ILIKE 'Lot -%%'
          AND o.email NOT IN %s
        GROUP BY c.catalog_id, DATE_TRUNC('month', o.ordered_at)
    """, (B2B_EMAILS,))
    tl_b2c = [(r[0], r[1], 'b2c', int(r[2] or 0), int(r[3] or 0), float(r[4] or 0))
              for r in cur.fetchall()]

    # B2C packs mensuel
    cur.execute("""
        SELECT c.catalog_id,
               DATE_TRUNC('month', o.ordered_at)::date               AS month,
               SUM(od.product_quantity)                               AS qty_sold,
               SUM(od.product_quantity - od.product_quantity_return)  AS qty_net,
               SUM(od.total_price_tax_incl)                           AS revenue
        FROM presta_order_details od
        JOIN gold.orders o ON o.order_id = od.id_order AND o.is_valid = TRUE
        JOIN gold.catalog c
            ON (c.ean13 = od.product_ean13 AND od.product_ean13 IS NOT NULL AND od.product_ean13 <> '')
            OR (c.ref_presta = od.product_reference AND od.product_reference IS NOT NULL AND od.product_reference <> '')
        WHERE c.is_pack = TRUE
          AND o.email NOT IN %s
        GROUP BY c.catalog_id, DATE_TRUNC('month', o.ordered_at)
    """, (B2B_EMAILS,))
    tl_pack = [(r[0], r[1], 'b2c_pack', int(r[2] or 0), int(r[3] or 0), float(r[4] or 0))
               for r in cur.fetchall()]

    # B2B mensuel
    cur.execute("""
        SELECT catalog_id,
               TO_DATE(month, 'YYYY-MM') AS month,
               SUM(ventes)               AS qty_sold,
               SUM(total_net)            AS qty_net,
               SUM(montant_ht)           AS revenue
        FROM gold.b2b_sofiadis_sales
        WHERE catalog_id IS NOT NULL
        GROUP BY catalog_id, month
    """)
    tl_b2b = [(r[0], r[1], 'b2b', int(r[2] or 0), int(r[3] or 0), float(r[4] or 0))
              for r in cur.fetchall()]

    all_tl = tl_b2c + tl_pack + tl_b2b
    cur.execute("TRUNCATE gold.sales_timeline")
    execute_values(cur, """
        INSERT INTO gold.sales_timeline (catalog_id, month, channel, qty_sold, qty_net, revenue)
        VALUES %s
        ON CONFLICT (catalog_id, month, channel) DO UPDATE
            SET qty_sold = EXCLUDED.qty_sold,
                qty_net  = EXCLUDED.qty_net,
                revenue  = EXCLUDED.revenue
    """, all_tl)
    conn.commit()
    print(f"✓ {len(all_tl)} lignes dans gold.sales_timeline "
          f"({len(tl_b2c)} b2c + {len(tl_pack)} b2c_pack + {len(tl_b2b)} b2b)")

    # ------------------------------------------------------------------ #
    # 9. Rapport
    # ------------------------------------------------------------------ #
    cur.execute("""
        SELECT
            canonical_name,
            series,
            first_presta_sale_date,
            launch_email_date,
            print_runs_count,
            total_printed,
            b2c_qty_net,
            b2b_qty_net,
            total_qty_net,
            stock_remaining_est
        FROM gold.catalog_performance
        WHERE is_pack = FALSE
          AND (total_qty_net > 0 OR total_printed > 0)
        ORDER BY first_presta_sale_date ASC NULLS LAST, canonical_name
    """)
    print(f"\n{'Titre':<45} {'1ère vente':>10} {'Email':>10} "
          f"{'Runs':>4} {'Imprimé':>8} {'B2C':>6} {'B2B':>6} {'Total':>6} {'Stock':>6}")
    print("─" * 115)
    for r in cur.fetchall():
        name, series, fs, ed, runs, printed, b2c, b2b, tot, stock = r
        print(
            f"{(name or '')[:44]:<45} "
            f"{str(fs or '?'):>10} "
            f"{str(ed or '?'):>10} "
            f"{(runs or 0):>4} "
            f"{(printed or 0):>8,} "
            f"{(b2c or 0):>6,} "
            f"{(b2b or 0):>6,} "
            f"{(tot or 0):>6,} "
            f"{str(stock or '?'):>6}"
        )

    cur.close()
    conn.close()


if __name__ == '__main__':
    run()

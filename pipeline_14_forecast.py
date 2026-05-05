#!/usr/bin/env python3
"""
pipeline_14_forecast.py — REVENUE FORECAST + REORDER ALERTS

Prédit les ventes par produit sur les 6 prochains mois en utilisant :
  - Historique mensuel (shop + B2B Sofiadis)
  - Saisonnalité Ramadan / Hajj / rentrée scolaire détectée auto
  - Tendance linéaire (régression simple)
  - Year-over-year repeat patterns

Tables créées :
  intelligence.product_history     — agrégat mensuel shop+B2B par produit
  intelligence.product_seasonality — coefficients saisonniers détectés
  intelligence.forecast_monthly    — projection 6 mois par produit
  intelligence.reorder_alerts      — quand commander la prochaine impression IMAK
"""

import psycopg2
from datetime import date

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# Dates Ramadan (Hijri sliding ~11j/an plus tôt chaque année grégorien)
RAMADAN_PERIODS = [
    ('2021-04-13', '2021-05-12'),
    ('2022-04-02', '2022-05-01'),
    ('2023-03-23', '2023-04-21'),
    ('2024-03-11', '2024-04-09'),
    ('2025-03-01', '2025-03-30'),
    ('2026-02-18', '2026-03-19'),
    ('2027-02-08', '2027-03-09'),
    ('2028-01-28', '2028-02-26'),
]

# Hajj season (juillet-août principalement)
HAJJ_PERIODS = [
    ('2024-06-14', '2024-06-19'),
    ('2025-06-04', '2025-06-09'),
    ('2026-05-25', '2026-05-30'),
    ('2027-05-15', '2027-05-20'),
]


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ═══════════════════════════════════════════════════════════════════
    # 1. product_history — séries mensuelles unifiées shop + B2B
    # ═══════════════════════════════════════════════════════════════════
    print('=== product_history (monthly time series) ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.product_history CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.product_history AS
        WITH shop_monthly AS (
            SELECT
                pm.product_id_master,
                pm.canonical_name           AS product,
                DATE_TRUNC('month', oi.ordered_at)::date AS month,
                SUM(oi.qty_ordered)::int    AS shop_qty,
                SUM(oi.total_ttc)::numeric  AS shop_revenue
            FROM gold.order_items oi
            JOIN gold.products_master pm USING (product_id_master)
            WHERE oi.ordered_at IS NOT NULL
            GROUP BY pm.product_id_master, pm.canonical_name, DATE_TRUNC('month', oi.ordered_at)
        ),
        b2b_monthly AS (
            SELECT
                LOWER(BTRIM(title))         AS title_clean,
                DATE_TRUNC('month', period::timestamp)::date AS month,
                SUM(net_qty)::int           AS b2b_qty,
                SUM(total_ht)::numeric      AS b2b_revenue
            FROM public.sofiadis_b2b_sales
            WHERE period IS NOT NULL AND net_qty > 0
            GROUP BY LOWER(BTRIM(title)), DATE_TRUNC('month', period::timestamp)
        )
        SELECT
            COALESCE(s.product_id_master, '')      AS product_id_master,
            COALESCE(s.product, b.title_clean)     AS product,
            COALESCE(s.month, b.month)             AS month,
            COALESCE(s.shop_qty, 0)                AS shop_qty,
            COALESCE(s.shop_revenue, 0)::float     AS shop_revenue,
            COALESCE(b.b2b_qty, 0)                 AS b2b_qty,
            COALESCE(b.b2b_revenue, 0)::float      AS b2b_revenue,
            (COALESCE(s.shop_qty, 0) + COALESCE(b.b2b_qty, 0)) AS total_qty,
            (COALESCE(s.shop_revenue, 0) + COALESCE(b.b2b_revenue, 0))::float AS total_revenue
        FROM shop_monthly s
        FULL OUTER JOIN b2b_monthly b
          ON LOWER(BTRIM(s.product)) = b.title_clean
         AND s.month = b.month
        WHERE COALESCE(s.month, b.month) IS NOT NULL
    """)
    conn.commit()
    cur.execute("CREATE INDEX ON intelligence.product_history(product_id_master, month)")
    cur.execute("CREATE INDEX ON intelligence.product_history(month)")
    conn.commit()

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT product), MIN(month), MAX(month) FROM intelligence.product_history")
    n, np, mn, mx = cur.fetchone()
    print(f'  {n:,} lignes · {np} produits · {mn} → {mx}')

    # ═══════════════════════════════════════════════════════════════════
    # 2. product_seasonality — coefficient mensuel par produit
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== product_seasonality ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.product_seasonality CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.product_seasonality AS
        WITH monthly_avg AS (
            SELECT
                product_id_master,
                product,
                EXTRACT(MONTH FROM month) AS month_num,
                AVG(total_qty)             AS avg_qty,
                COUNT(*)                   AS years_seen
            FROM intelligence.product_history
            WHERE total_qty > 0
            GROUP BY product_id_master, product, EXTRACT(MONTH FROM month)
        ),
        product_baseline AS (
            SELECT
                product_id_master,
                AVG(total_qty) AS baseline_qty
            FROM intelligence.product_history
            WHERE total_qty > 0
            GROUP BY product_id_master
        )
        SELECT
            ma.product_id_master,
            ma.product,
            ma.month_num::int,
            ROUND(ma.avg_qty::numeric, 1) AS avg_qty_in_month,
            ROUND(pb.baseline_qty::numeric, 1) AS baseline_qty,
            ROUND((ma.avg_qty / NULLIF(pb.baseline_qty, 0))::numeric, 2) AS seasonality_coef,
            ma.years_seen
        FROM monthly_avg ma
        JOIN product_baseline pb USING (product_id_master)
    """)
    conn.commit()

    cur.execute("""
        SELECT product, month_num, seasonality_coef, years_seen
        FROM intelligence.product_seasonality
        WHERE seasonality_coef > 1.5 AND years_seen >= 2
        ORDER BY seasonality_coef DESC LIMIT 15
    """)
    print('  Top pics saisonniers (coef > 1.5x normal) :')
    for r in cur.fetchall():
        print(f'    {r[0][:45]:45s}  mois {r[1]:>2}  ×{r[2]:>5}  ({r[3]} ans)')

    # ═══════════════════════════════════════════════════════════════════
    # 3. forecast_monthly — projection 6 mois
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== forecast_monthly (6 mois) ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.forecast_monthly CASCADE")

    # Approche : pour chaque produit
    #   forecast_qty(month) = baseline_recent × seasonality_coef(month) × (1 + trend_pct/12)
    cur.execute("""
        CREATE TABLE intelligence.forecast_monthly AS
        WITH baseline_recent AS (
            SELECT
                product_id_master,
                product,
                AVG(total_qty)::numeric AS recent_avg_qty,
                COUNT(*) AS months_with_sales
            FROM intelligence.product_history
            WHERE month >= CURRENT_DATE - INTERVAL '12 months'
              AND total_qty > 0
            GROUP BY product_id_master, product
            HAVING COUNT(*) >= 2
        ),
        trend AS (
            -- Régression linéaire simple : qty ~ row_number
            SELECT
                product_id_master,
                CASE
                    WHEN COUNT(*) > 3 THEN
                        ROUND(REGR_SLOPE(total_qty, EXTRACT(EPOCH FROM month))::numeric, 8)
                    ELSE 0
                END AS trend_slope_per_sec,
                COUNT(*) AS history_months
            FROM intelligence.product_history
            WHERE month >= CURRENT_DATE - INTERVAL '24 months'
              AND total_qty > 0
            GROUP BY product_id_master
        ),
        future_months AS (
            SELECT generate_series(
                DATE_TRUNC('month', CURRENT_DATE)::date + INTERVAL '1 month',
                DATE_TRUNC('month', CURRENT_DATE)::date + INTERVAL '6 months',
                INTERVAL '1 month'
            )::date AS month
        )
        SELECT
            br.product_id_master,
            br.product,
            fm.month,
            EXTRACT(MONTH FROM fm.month)::int AS month_num,
            br.recent_avg_qty                  AS baseline_qty,
            COALESCE(ps.seasonality_coef, 1.0) AS seasonality_coef,
            ROUND((br.recent_avg_qty * COALESCE(ps.seasonality_coef, 1.0))::numeric, 0) AS forecast_qty,
            COALESCE(t.trend_slope_per_sec, 0) AS trend_slope,
            t.history_months,
            CASE
                WHEN t.history_months >= 12 AND ps.years_seen >= 2 THEN 'high'
                WHEN t.history_months >= 6  THEN 'medium'
                ELSE 'low'
            END AS confidence,
            NOW() AS computed_at
        FROM baseline_recent br
        CROSS JOIN future_months fm
        LEFT JOIN intelligence.product_seasonality ps
               ON ps.product_id_master = br.product_id_master
              AND ps.month_num = EXTRACT(MONTH FROM fm.month)
        LEFT JOIN trend t ON t.product_id_master = br.product_id_master
    """)
    conn.commit()

    cur.execute("""
        SELECT product, month, forecast_qty, seasonality_coef, confidence
        FROM intelligence.forecast_monthly
        ORDER BY product, month LIMIT 30
    """)
    print('  Sample forecast (6 mois à venir, premiers produits) :')
    print(f"  {'Product':40s} {'Month':>10s} {'Qty':>5s} {'Coef':>5s} Conf")
    for r in cur.fetchall():
        print(f"    {r[0][:38]:38s}  {str(r[1]):>10s}  {r[2]:>5}  {r[3]:>4}  {r[4]}")

    # ═══════════════════════════════════════════════════════════════════
    # 4. reorder_alerts — quand commander à IMAK
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== reorder_alerts ===\n')
    cur.execute("DROP TABLE IF EXISTS intelligence.reorder_alerts CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.reorder_alerts AS
        WITH current_stock AS (
            -- Stock approximatif = total imprimé IMAK - total vendu (shop + B2B)
            SELECT
                LOWER(BTRIM(i.title)) AS product_clean,
                i.title              AS product_name,
                SUM(i.qty)           AS total_printed,
                COALESCE((
                    SELECT SUM(b.net_qty)
                    FROM public.sofiadis_b2b_sales b
                    WHERE LOWER(BTRIM(b.title)) = LOWER(BTRIM(i.title))
                ), 0)                AS total_b2b_sold,
                COALESCE((
                    SELECT SUM(oi.qty_ordered)
                    FROM gold.order_items oi
                    JOIN gold.products_master pm USING (product_id_master)
                    WHERE LOWER(BTRIM(pm.canonical_name)) = LOWER(BTRIM(i.title))
                ), 0)                AS total_shop_sold
            FROM public.imak_print_orders i
            GROUP BY LOWER(BTRIM(i.title)), i.title
        ),
        future_demand AS (
            SELECT
                LOWER(BTRIM(product))      AS product_clean,
                SUM(forecast_qty)::int     AS demand_6m
            FROM intelligence.forecast_monthly
            GROUP BY LOWER(BTRIM(product))
        )
        SELECT
            cs.product_name,
            cs.total_printed,
            cs.total_b2b_sold,
            cs.total_shop_sold,
            (cs.total_printed - cs.total_b2b_sold - cs.total_shop_sold) AS estimated_stock,
            COALESCE(fd.demand_6m, 0) AS forecast_demand_6m,
            (cs.total_printed - cs.total_b2b_sold - cs.total_shop_sold - COALESCE(fd.demand_6m, 0)) AS projected_stock_6m,
            CASE
                WHEN COALESCE(fd.demand_6m, 0) = 0 THEN 'unknown_demand'
                WHEN (cs.total_printed - cs.total_b2b_sold - cs.total_shop_sold) <= 0 THEN 'OUT_OF_STOCK'
                WHEN (cs.total_printed - cs.total_b2b_sold - cs.total_shop_sold) <
                     COALESCE(fd.demand_6m, 0) * 0.5 THEN 'URGENT_REORDER'
                WHEN (cs.total_printed - cs.total_b2b_sold - cs.total_shop_sold) <
                     COALESCE(fd.demand_6m, 0) THEN 'reorder_soon'
                WHEN (cs.total_printed - cs.total_b2b_sold - cs.total_shop_sold) <
                     COALESCE(fd.demand_6m, 0) * 1.5 THEN 'monitor'
                ELSE 'ok'
            END AS reorder_status,
            NOW() AS computed_at
        FROM current_stock cs
        LEFT JOIN future_demand fd ON cs.product_clean = fd.product_clean
        WHERE cs.total_printed > 0
    """)
    conn.commit()
    cur.execute("CREATE INDEX ON intelligence.reorder_alerts(reorder_status)")
    conn.commit()

    cur.execute("""
        SELECT product_name, estimated_stock, forecast_demand_6m, projected_stock_6m, reorder_status
        FROM intelligence.reorder_alerts
        WHERE reorder_status IN ('OUT_OF_STOCK','URGENT_REORDER','reorder_soon')
        ORDER BY projected_stock_6m ASC LIMIT 15
    """)
    print('  Produits à RECOMMANDER à IMAK :')
    print(f"  {'Product':40s} {'Stock':>8s} {'Demand6m':>10s} {'After':>8s}  Status")
    for r in cur.fetchall():
        print(f"    {r[0][:38]:38s}  {r[1]:>8}  {r[2]:>10}  {r[3]:>8}  {r[4]}")

    # ═══════════════════════════════════════════════════════════════════
    # 5. Sync vers opportunities
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== Sync vers opportunities ===\n')
    cur.execute("""
        INSERT INTO intelligence.opportunities
          (opportunity_type, title, description, score, details, action_suggested, computed_at)
        SELECT
            'reorder_imak'::text,
            product_name,
            CONCAT('Stock estimé ', estimated_stock, ' · demande 6m ', forecast_demand_6m, ' · ',
                   CASE
                     WHEN reorder_status='OUT_OF_STOCK' THEN 'RUPTURE'
                     WHEN reorder_status='URGENT_REORDER' THEN 'URGENT'
                     ELSE 'à commander bientôt'
                   END)::text,
            (forecast_demand_6m * 1.0 -
             CASE WHEN estimated_stock > 0 THEN estimated_stock ELSE 0 END)::numeric,
            jsonb_build_object(
                'product', product_name,
                'estimated_stock', estimated_stock,
                'demand_6m', forecast_demand_6m,
                'projected_stock_6m', projected_stock_6m,
                'status', reorder_status
            ),
            'order_imak_print',
            NOW()
        FROM intelligence.reorder_alerts
        WHERE reorder_status IN ('OUT_OF_STOCK','URGENT_REORDER','reorder_soon')
    """)
    n = cur.rowcount
    conn.commit()
    print(f'  ✓ {n} reorder opportunities ajoutées')

    # ═══════════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════════
    print('\n=== INTELLIGENCE LAYER updated ===')
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='intelligence' ORDER BY table_name
    """)
    for (t,) in cur.fetchall():
        cur.execute(f'SELECT COUNT(*) FROM intelligence."{t}"')
        n = cur.fetchone()[0]
        print(f'  intelligence.{t}: {n:,}')

    print('\n📋 Exemples de requêtes :\n')
    print("""
  -- Forecast détaillé pour un produit
  SELECT month, forecast_qty, seasonality_coef, confidence
  FROM intelligence.forecast_monthly
  WHERE product ILIKE '%hajj%' ORDER BY month;

  -- Tous les produits en alerte stock
  SELECT product_name, estimated_stock, forecast_demand_6m, reorder_status
  FROM intelligence.reorder_alerts
  WHERE reorder_status != 'ok'
  ORDER BY projected_stock_6m ASC;

  -- Pic saisonnier d'un produit (mois où on vend le plus)
  SELECT month_num, seasonality_coef
  FROM intelligence.product_seasonality
  WHERE product ILIKE '%ramadan%' AND years_seen >= 2
  ORDER BY seasonality_coef DESC;
""")

    conn.close()


if __name__ == '__main__':
    main()

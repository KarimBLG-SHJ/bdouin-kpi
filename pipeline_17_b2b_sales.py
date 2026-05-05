#!/usr/bin/env python3
"""
pipeline_17_b2b_sales.py — Ventes B2B Sofiadis (relevés mensuels)

Source : gmail_raw — "SOFIADIS : RELEVE DE VENTES ET RETOURS"
         Sheet "LIBRAIRIES CLASSIQUES" (ou "Feuil1")
         Colonnes : Editeur | CodeBarre | Désignation | Ventes | Retours | Total | Prix HT | Mntnt HT

Produit :
  gold.b2b_sofiadis_sales  — une ligne par EAN × mois
  gold.sales_unified       — vue shop + b2b par catalog_id × mois
"""

import os, re, unicodedata
import psycopg2
from psycopg2.extras import execute_values

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

def normalize(s):
    if not s: return ''
    s = unicodedata.normalize('NFD', str(s))
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'\s+', ' ', s).strip().lower()

def clean_ean(v):
    if not v: return None
    s = str(v).replace('.0', '').strip()
    return s if re.match(r'^\d{13}$', s) else None

def parse_month_from_subject(subject):
    """Extrait YYYY-MM depuis le sujet du mail."""
    months_fr = {
        'janvier': '01', 'fevrier': '02', 'février': '02', 'mars': '03',
        'avril': '04', 'mai': '05', 'juin': '06', 'juillet': '07',
        'aout': '08', 'août': '08', 'septembre': '09', 'octobre': '10',
        'novembre': '11', 'decembre': '12', 'décembre': '12',
    }
    s = normalize(subject)
    year_m = re.search(r'(\d{4})', s)
    year = year_m.group(1) if year_m else None
    for mname, mnum in months_fr.items():
        if mname in s:
            return f'{year}-{mnum}' if year else None
    return None


def run():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ── Charger le catalogue (EAN → catalog_id) ───────────────────────────────
    cur.execute("SELECT catalog_id, ean13, canonical_name FROM gold.catalog WHERE ean13 IS NOT NULL")
    ean_to_cat = {r[1]: (r[0], r[2]) for r in cur.fetchall()}
    # Aliases
    cur.execute("""
        SELECT a.alias_name, a.catalog_id, c.ean13
        FROM gold.catalog_aliases a
        JOIN gold.catalog c ON c.catalog_id = a.catalog_id
        WHERE c.ean13 IS NOT NULL
    """)
    alias_norm_to_cat = {}
    for alias, cid, ean in cur.fetchall():
        alias_norm_to_cat[normalize(alias)] = cid
    print(f'Catalogue : {len(ean_to_cat)} EANs, {len(alias_norm_to_cat)} aliases')

    # ── Parser tous les relevés de ventes ─────────────────────────────────────
    cur.execute("""
        SELECT id, subject, date_sent::date, attachments
        FROM public.gmail_raw
        WHERE subject ILIKE '%RELEVE DE VENTES%'
          AND attachments::text LIKE '%content_json%'
        ORDER BY date_sent ASC
    """)
    emails = cur.fetchall()
    print(f'{len(emails)} relevés de ventes trouvés')

    sales_rows = []   # (month, ean, product_name, catalog_id, ventes, retours, total_net, prix_ht, montant_ht, source_email_id)
    seen = set()      # (month, ean) — dédoublonnage

    for email_id, subject, date_sent, attachments in emails:
        month = parse_month_from_subject(subject)
        if not month:
            # Fallback: use date_sent month
            month = str(date_sent)[:7]

        for att in (attachments or []):
            if 'content_json' not in att:
                continue
            for sheet in att['content_json']:
                sname = (sheet.get('name') or '').strip()
                # Prendre les sheets de ventes (LIBRAIRIES ou Feuil1)
                if sname not in ('LIBRAIRIES CLASSIQUES', 'Feuil1') and 'librairie' not in sname.lower():
                    continue

                for row in sheet.get('rows', []):
                    if not row or len(row) < 6:
                        continue
                    # Ligne de données : editeur='BDOUIN', col1=EAN, col2=désig, col3=ventes, col4=retours, col5=total, col6=prix, col7=montant
                    editeur = str(row[0]).strip() if row[0] else ''
                    if editeur.upper() != 'BDOUIN':
                        continue

                    ean = clean_ean(row[1] if len(row) > 1 else None)
                    product_name = str(row[2]).strip() if len(row) > 2 and row[2] else ''
                    if not product_name or product_name in ('Désignation', ''):
                        continue

                    try:
                        ventes = float(row[3]) if len(row) > 3 and row[3] is not None else 0
                        retours = float(row[4]) if len(row) > 4 and row[4] is not None else 0
                        total_net = float(row[5]) if len(row) > 5 and row[5] is not None else 0
                        prix_ht = float(row[6]) if len(row) > 6 and row[6] is not None else None
                        montant_ht = float(row[7]) if len(row) > 7 and row[7] is not None else None
                    except (ValueError, TypeError):
                        continue

                    if ventes == 0 and retours == 0:
                        continue

                    # Match catalog_id
                    # 1) ean direct sur catalog.ean13 ; 2) ean dans aliases (Sofiadis) ;
                    # 3) nom produit normalisé dans aliases
                    catalog_id = None
                    if ean and ean in ean_to_cat:
                        catalog_id = ean_to_cat[ean][0]
                    elif ean and ean in alias_norm_to_cat:
                        catalog_id = alias_norm_to_cat[ean]
                    else:
                        catalog_id = alias_norm_to_cat.get(normalize(product_name))

                    # Dédoublonnage par (month, ean, product_name) — garder premier
                    key = (month, ean or normalize(product_name)[:40])
                    if key in seen:
                        continue
                    seen.add(key)

                    sales_rows.append((
                        month, ean, product_name, catalog_id,
                        ventes, retours, total_net, prix_ht, montant_ht, email_id
                    ))

    print(f'Lignes de ventes parsées : {len(sales_rows)}')

    # ── Créer gold.b2b_sofiadis_sales ────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS gold.b2b_sofiadis_sales CASCADE")
    cur.execute("""
        CREATE TABLE gold.b2b_sofiadis_sales (
            sale_id         SERIAL PRIMARY KEY,
            month           TEXT NOT NULL,          -- YYYY-MM
            ean13           TEXT,
            product_name    TEXT,
            catalog_id      INTEGER REFERENCES gold.catalog(catalog_id),
            ventes          NUMERIC,
            retours         NUMERIC,
            total_net       NUMERIC,
            prix_ht         NUMERIC(10,4),
            montant_ht      NUMERIC(10,4),
            source_email_id TEXT,
            created_at      TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX ON gold.b2b_sofiadis_sales (month)")
    cur.execute("CREATE INDEX ON gold.b2b_sofiadis_sales (catalog_id)")
    cur.execute("CREATE INDEX ON gold.b2b_sofiadis_sales (ean13)")

    execute_values(cur, """
        INSERT INTO gold.b2b_sofiadis_sales
          (month, ean13, product_name, catalog_id, ventes, retours, total_net, prix_ht, montant_ht, source_email_id)
        VALUES %s
    """, sales_rows)
    conn.commit()
    print(f'✅ gold.b2b_sofiadis_sales : {len(sales_rows)} lignes insérées')

    # ── Créer gold.sales_unified (vue shop + b2b) ─────────────────────────────
    cur.execute("DROP VIEW IF EXISTS gold.sales_unified CASCADE")
    cur.execute("""
        CREATE VIEW gold.sales_unified AS

        -- Canal shop PrestaShop (gold.order_items)
        SELECT
            'shop'                               AS channel,
            TO_CHAR(ordered_at, 'YYYY-MM')       AS month,
            oi.product_id_master::TEXT           AS product_id_master,
            c.catalog_id                         AS catalog_id,
            c.canonical_name                     AS canonical_name,
            c.series                             AS series,
            c.tome_number                        AS tome_number,
            SUM(oi.qty_ordered)::NUMERIC         AS qty_sold,
            0::NUMERIC                           AS qty_returned,
            SUM(oi.qty_ordered)::NUMERIC         AS qty_net,
            SUM(oi.total_ttc)                    AS revenue_ttc,
            NULL::NUMERIC                        AS revenue_ht
        FROM gold.order_items oi
        LEFT JOIN gold.catalog c
            ON c.ean13 = oi.ean13
        WHERE oi.ordered_at IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7

        UNION ALL

        -- Canal B2B Sofiadis (gold.b2b_sofiadis_sales)
        SELECT
            'b2b_sofiadis'                       AS channel,
            b.month                              AS month,
            NULL::TEXT                           AS product_id_master,
            b.catalog_id                         AS catalog_id,
            COALESCE(c.canonical_name, b.product_name) AS canonical_name,
            c.series                             AS series,
            c.tome_number                        AS tome_number,
            SUM(b.ventes)                        AS qty_sold,
            SUM(b.retours)                       AS qty_returned,
            SUM(b.total_net)                     AS qty_net,
            NULL::NUMERIC                        AS revenue_ttc,
            SUM(b.montant_ht)                    AS revenue_ht
        FROM gold.b2b_sofiadis_sales b
        LEFT JOIN gold.catalog c ON c.catalog_id = b.catalog_id
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    """)
    conn.commit()
    print('✅ gold.sales_unified view créée')

    # ── Rapport ───────────────────────────────────────────────────────────────
    cur.execute("""
        SELECT channel, COUNT(DISTINCT month) as mois, SUM(qty_net) as unites, SUM(COALESCE(revenue_ttc, revenue_ht)) as ca
        FROM gold.sales_unified
        GROUP BY channel
    """)
    print(f'\n{"Canal":<20} {"Mois":>5} {"Unités":>8} {"CA (€)":>12}')
    print('-'*50)
    for ch, mois, units, ca in cur.fetchall():
        print(f'{ch:<20} {mois:>5} {int(units or 0):>8} {float(ca or 0):>12,.0f}')

    # Top 10 titres B2B
    cur.execute("""
        SELECT canonical_name, SUM(qty_net) as units, SUM(revenue_ht) as ca
        FROM gold.sales_unified
        WHERE channel = 'b2b_sofiadis'
        GROUP BY canonical_name
        ORDER BY units DESC NULLS LAST
        LIMIT 10
    """)
    print('\nTop 10 titres B2B Sofiadis (toutes périodes) :')
    for name, units, ca in cur.fetchall():
        print(f'  {str(name or "—"):<45} {int(units or 0):>6} ex  {float(ca or 0):>8,.0f}€')

    cur.execute("""
        SELECT COUNT(*) FROM gold.b2b_sofiadis_sales WHERE catalog_id IS NULL
    """)
    unmatched = cur.fetchone()[0]
    print(f'\nLignes sans catalog_id : {unmatched}')

    conn.close()
    print('\n✅ Pipeline B2B terminé.')


if __name__ == '__main__':
    run()

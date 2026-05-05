#!/usr/bin/env python3
"""
pipeline_16_catalog.py — Catalogue canonique BDouin

Sources :
  1. presta_products   → liste officielle, EAN, date lancement shop, prix public
  2. gmail_raw         → feuilles Articles des LOGISTIQUE Sofiaco
                         → feuilles RECAP/onglets mensuels du point BDOUIN
  3. imak_print_orders → titres imprimés (enrichissement)

Produit :
  gold.catalog         → un produit = une ligne, avec ref canonic, EAN, serie, tome,
                         prix, dates, ref_sofiaco, ref_imak, flags
"""

import os, re, json, unicodedata
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def normalize(s):
    """Lowercase, strip accents, collapse spaces."""
    if not s:
        return ''
    s = unicodedata.normalize('NFD', str(s))
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'\s+', ' ', s).strip().lower()

def clean_ean(v):
    if not v:
        return None
    s = str(v).replace('.0', '').strip()
    if re.match(r'^\d{13}$', s):
        return s
    return None

def detect_series(name):
    """Return (series_label, tome_number, is_pack) from a product name."""
    n = normalize(name)
    series = None
    tome = None
    is_pack = 'pack' in n or 'lot ' in n

    if 'foulane' in n and 'agence' not in n:
        series = 'Famille Foulane'
        m = re.search(r'(?:tome|t\.|t)\s*(\d+)', n)
        if m:
            tome = int(m.group(1))
    elif 'walad et binti' in n or 'walad & binti' in n:
        series = 'Walad & Binti'
        m = re.search(r't(\d+)|tome\s*(\d+)', n)
        if m:
            tome = int(m.group(1) or m.group(2))
    elif 'walad decouvre' in n or 'walad découvre' in n:
        series = 'Walad Découvre'
        if 'medine' in n or 'médine' in n:
            tome = 1
        elif 'mecque' in n or 'mekke' in n:
            tome = 2
    elif 'awlad school' in n:
        series = 'Awlad School'
        m = re.search(r'tome\s*(\d+)|t(\d+)', n)
        if m:
            tome = int(m.group(1) or m.group(2))
    elif 'agence regle tout' in n or 'agence règle tout' in n:
        series = 'Agence Règle Tout'
        m = re.search(r'tome\s*(\d+)|v(\d+)', n)
        if m:
            tome = int(m.group(1) or m.group(2))
    elif 'recueil' in n and ('muslim' in n or 'show' in n):
        series = 'Recueil Muslim Show'
        m = re.search(r't\.?\s*(\d+)|[\(#](\d+)', n)
        if m:
            tome = int(m.group(1) or m.group(2))
    elif 'muslim show' in n and 'recueil' not in n:
        series = 'Muslim Show'
    elif 'mini guide' in n or 'salat' in n:
        series = 'Mini Guides'
    elif 'hajj' in n or 'omra' in n or 'umra' in n:
        series = 'Guide Hajj'
    elif 'cahier' in n and 'arabe' in n:
        series = 'Cahiers Écriture Arabe'
    elif 'citadelle' in n:
        series = 'Citadelle'
    elif 'ramadan' in n:
        series = 'Ramadan'
    elif 'bonnes actions' in n or 'recueil des bonnes' in n:
        series = 'Bonnes Actions'
    elif 'super etudiant' in n or 'super étudiant' in n:
        series = 'Super Étudiant'
    elif 'dialogue' in n:
        series = 'Dialogue'
    elif '100 bonnes manieres' in n or '100 bonnes manières' in n:
        series = '100 Bonnes Manières'

    return series, tome, is_pack


def parse_sofiaco_articles(attachments):
    """Extract product list from Sofiaco LOGISTIQUE 'Articles' sheet."""
    products = {}
    for att in (attachments or []):
        if 'content_json' not in att:
            continue
        for sheet in att['content_json']:
            if sheet.get('name', '') != 'Articles':
                continue
            for row in sheet.get('rows', []):
                if not row or len(row) < 3:
                    continue
                name = str(row[0]).strip() if row[0] else ''
                if not name or name in ('Désignation*', 'Ref', 'None', ''):
                    continue
                # EAN col index 2
                ean = clean_ean(row[2] if len(row) > 2 else None)
                if not ean:
                    ean = clean_ean(row[1] if len(row) > 1 else None)
                prix_ht = None
                try:
                    prix_ht = float(row[3]) if len(row) > 3 and row[3] else None
                except (ValueError, TypeError):
                    pass
                key = ean or normalize(name)[:50]
                if key not in products:
                    products[key] = {'name': name, 'ean': ean, 'prix_ht_sofiadis': prix_ht}
    return products


# ─── Main ─────────────────────────────────────────────────────────────────────

def run():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ── 1. Base PrestaShop ──────────────────────────────────────────────────
    cur.execute("""
        SELECT reference, ean13, isbn, name, price, wholesale_price, active, date_add
        FROM public.presta_products
        WHERE name NOT ILIKE 'Test%'
        ORDER BY date_add ASC
    """)
    presta_rows = cur.fetchall()
    print(f'PrestaShop : {len(presta_rows)} produits')

    # ── 2. Sofiaco Articles (enrichissement) ───────────────────────────────
    cur.execute("""
        SELECT attachments FROM public.gmail_raw
        WHERE attachments::text LIKE '%content_json%'
          AND (subject ILIKE '%LOGISTIQUE%' OR subject ILIKE '%logistique%'
               OR subject ILIKE '%Relevé logistique%' OR subject ILIKE '%FACTURATION%')
        ORDER BY date_sent ASC
    """)
    sofiaco_products = {}
    for (attachments,) in cur.fetchall():
        for k, v in parse_sofiaco_articles(attachments).items():
            sofiaco_products[k] = v

    # Build EAN → sofiaco name map
    sofiaco_by_ean = {v['ean']: v for v in sofiaco_products.values() if v.get('ean')}
    print(f'Sofiaco Articles : {len(sofiaco_by_ean)} produits avec EAN')

    # ── 3. IMAK titres (enrichissement) ────────────────────────────────────
    cur.execute("SELECT DISTINCT title FROM public.imak_print_orders ORDER BY title")
    imak_titles = {normalize(r[0]) for r in cur.fetchall()}
    print(f'IMAK : {len(imak_titles)} titres distincts')

    # ── 4. Construire le catalogue canonique ───────────────────────────────
    catalog = []
    for row in presta_rows:
        ref_presta, ean13, isbn, name, price_ttc, wprice, active, date_add = row
        ean = clean_ean(ean13) or clean_ean(isbn)
        series, tome, is_pack = detect_series(name)

        # Enrichissement Sofiaco
        prix_ht_sofiadis = None
        if ean and ean in sofiaco_by_ean:
            prix_ht_sofiadis = sofiaco_by_ean[ean].get('prix_ht_sofiadis')

        # Flag IMAK
        n_norm = normalize(name)
        in_imak = any(imak_t in n_norm or n_norm in imak_t for imak_t in imak_titles)

        catalog.append({
            'ref_presta': ref_presta,
            'ean13': ean,
            'canonical_name': name,
            'series': series,
            'tome_number': tome,
            'is_pack': is_pack,
            'prix_public_ttc': float(price_ttc) if price_ttc else None,
            'prix_ht_sofiadis': prix_ht_sofiadis,
            'shop_launch_date': date_add.date() if date_add else None,
            'active_on_shop': bool(active),
            'in_imak': in_imak,
        })

    print(f'Catalogue : {len(catalog)} entrées')

    # ── 5. Créer gold.catalog ──────────────────────────────────────────────
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
    cur.execute("DROP TABLE IF EXISTS gold.catalog CASCADE")
    cur.execute("""
        CREATE TABLE gold.catalog (
            catalog_id          SERIAL PRIMARY KEY,
            ref_presta          TEXT,
            ean13               TEXT,
            canonical_name      TEXT NOT NULL,
            series              TEXT,
            tome_number         INTEGER,
            is_pack             BOOLEAN DEFAULT FALSE,
            prix_public_ttc     NUMERIC(10,4),
            prix_ht_sofiadis    NUMERIC(10,4),
            shop_launch_date    DATE,
            active_on_shop      BOOLEAN DEFAULT TRUE,
            in_imak             BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMP DEFAULT NOW()
        )
    """)

    execute_values(cur, """
        INSERT INTO gold.catalog
          (ref_presta, ean13, canonical_name, series, tome_number, is_pack,
           prix_public_ttc, prix_ht_sofiadis, shop_launch_date, active_on_shop, in_imak)
        VALUES %s
    """, [(
        p['ref_presta'], p['ean13'], p['canonical_name'], p['series'],
        p['tome_number'], p['is_pack'], p['prix_public_ttc'], p['prix_ht_sofiadis'],
        p['shop_launch_date'], p['active_on_shop'], p['in_imak']
    ) for p in catalog])

    conn.commit()

    # ── 6. Rapport ──────────────────────────────────────────────────────────
    cur.execute("""
        SELECT series, COUNT(*) as nb, COUNT(ean13) as avec_ean,
               COUNT(prix_ht_sofiadis) as avec_prix_sofiadis,
               SUM(in_imak::int) as in_imak
        FROM gold.catalog
        WHERE NOT is_pack AND canonical_name NOT ILIKE 'Lot -%'
        GROUP BY series ORDER BY nb DESC
    """)
    print('\n=== Catalogue par série (hors packs/lots) ===')
    print(f'{"Série":<30} {"Titres":>6} {"EAN":>5} {"Prix Sof":>9} {"IMAK":>5}')
    print('-' * 60)
    for series, nb, ean, prix_sof, imak in cur.fetchall():
        print(f'{str(series or "—"):<30} {nb:>6} {ean:>5} {prix_sof:>9} {imak:>5}')

    cur.execute("SELECT COUNT(*) FROM gold.catalog WHERE is_pack OR canonical_name ILIKE 'Lot -%'")
    print(f'\nDont packs/lots : {cur.fetchone()[0]}')

    cur.execute("SELECT COUNT(*) FROM gold.catalog WHERE ean13 IS NULL")
    print(f'Sans EAN : {cur.fetchone()[0]}')

    conn.close()
    print('\n✅ gold.catalog créé.')


if __name__ == '__main__':
    run()

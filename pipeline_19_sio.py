#!/usr/bin/env python3
"""
pipeline_19_sio.py — Import des données SIO (PrestaShop export + TAG segments)

Sources locales (~/Downloads/Archive-SIO/) :
  Export-all-transactions.csv    — transactions PrestaShop (264 lignes)
  TAG-EN_ADP-*.csv               — contacts en abandon de panier (pas acheté)
  TAG-ACHAT-*.csv                — achats directs (sans ADP)
  TAG-ACHAT_ADP-*.csv            — achats après abandon de panier

Produit :
  public.sio_transactions        — transactions nettoyées
  public.sio_contacts            — contacts segmentés (1 ligne par email × pack × segment)
  gold.sio_customer_segments     — par user_id_master : packs touchés, CA, multi-pack
  gold.sio_conversion_funnel     — par pack : taux de conversion ADP, CA direct vs récupéré
"""

import os, csv, re
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

SIO_DIR = os.path.expanduser('~/Downloads/Archive-SIO')

# Emails de test à exclure partout
TEST_EMAILS = {
    'abdelhakim@hoo-pow.com',
    'aahh.limited@gmail.com',
    'contact@abdelhakim.dev',
    'xosako1714@newtrea.com',
    'mosedop655@iaciu.com',
    'sawob93584@ostahie.com',
    'jonathan.coach.hpi+1@gmail.com',
}

TAG_FILES = {
    'TAG-EN_ADP-Guides.csv':     ('adp_no_purchase', 'mini_guides'),
    'TAG-EN_ADP-Foulane.csv':    ('adp_no_purchase', 'famille_foulane'),
    'TAG-EN_ADP-Awlad.csv':      ('adp_no_purchase', 'awlad_school'),
    'TAG-ACHAT-Guides.csv':      ('direct_purchase', 'mini_guides'),
    'TAG-ACHAT-Foulane.csv':     ('direct_purchase', 'famille_foulane'),
    'TAG-ACHAT-Awlad.csv':       ('direct_purchase', 'awlad_school'),
    'TAG-ACHAT_ADP-Guides.csv':  ('adp_converted',   'mini_guides'),
    'TAG-ACHAT_ADP-Foulane.csv': ('adp_converted',   'famille_foulane'),
    'TAG-ACHAT_ADP-Awlad.csv':   ('adp_converted',   'awlad_school'),
}

PACK_MAP = {
    'PACK-01': 'awlad_school',
    'PACK-02': 'famille_foulane',
    'PACK-03': 'mini_guides',
}


def run(cur, sql, label=''):
    try:
        cur.execute(sql)
        cur.connection.commit()
        if label:
            print(f'  ✓ {label}')
    except Exception as e:
        cur.connection.rollback()
        print(f'  ✗ {label}: {str(e)[:200]}')


def norm_col(name):
    """Normalise un nom de colonne CSV → clé de lookup."""
    return name.strip().lower().replace(' ', '_').replace('/', '_')


def get(row, *keys, default=''):
    """Récupère une valeur dans un dict CSV en essayant plusieurs noms de colonne."""
    for k in keys:
        v = row.get(k) or row.get(norm_col(k))
        if v is not None:
            return v.strip()
    return default


def parse_dt(s):
    """Parse une date ISO ou retourne None."""
    if not s:
        return None
    s = s.strip()
    for fmt in ('%Y-%m-%d %H:%M:%S (UTC+1)', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def extract_pack_code(product_name):
    """'Pack Awlad SchoolPACK-01' → ('PACK-01', 'awlad_school')"""
    m = re.search(r'(PACK-\d+)', product_name or '')
    if m:
        code = m.group(1)
        return code, PACK_MAP.get(code, 'unknown')
    return None, 'unknown'


# ─── PARSE TRANSACTIONS ───────────────────────────────────────────────────────

def parse_transactions():
    path = os.path.join(SIO_DIR, 'Export-all-transactions.csv')
    rows = []
    with open(path, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for raw in reader:
            email = get(raw, 'Adresse email').lower()
            if not email or email in TEST_EMAILS:
                continue
            # Exclure les lignes test (TVA non zéro = test interne)
            tva = get(raw, 'TVA')
            if tva and tva != '0%':
                continue
            amount_str = get(raw, 'Montant')
            try:
                amount = float(amount_str.replace(',', '.'))
            except (ValueError, AttributeError):
                continue
            if amount < 20:
                continue

            product_name = get(raw, 'Nom du produit')
            pack_code, pack = extract_pack_code(product_name)
            invoice = get(raw, 'Numéro de facture')
            country = get(raw, 'Pays du client')
            zone = get(raw, 'Nom de la zone d\'expédition')

            rows.append((
                email,
                parse_dt(get(raw, 'Date de la transaction')),
                amount,
                pack_code,
                pack,
                product_name,
                invoice,
                get(raw, 'Prénom du client'),
                get(raw, 'Nom du client'),
                country,
                get(raw, 'Ville du client'),
                get(raw, 'Code postal du client'),
                get(raw, 'Adresse du client'),
                get(raw, 'Code promo') or None,
                zone,
                zone not in ('France', ''),
            ))
    return rows


# ─── PARSE CONTACTS ───────────────────────────────────────────────────────────

def parse_contacts():
    rows = []
    for fname, (segment, pack) in TAG_FILES.items():
        path = os.path.join(SIO_DIR, fname)
        with open(path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Normalise les clés
            for raw in reader:
                nrow = {norm_col(k): v.strip() if v else '' for k, v in raw.items()}
                email = nrow.get('email', '').lower()
                if not email or email in TEST_EMAILS:
                    continue
                tags = nrow.get('tag', '')
                if 'Beta-Test' in tags:
                    continue

                firstname = nrow.get('first_name') or nrow.get('firstname', '')
                lastname  = nrow.get('last_name')  or nrow.get('lastname', '')
                country   = nrow.get('country', '')
                city      = nrow.get('city', '')
                postal    = nrow.get('postal_code', '')
                phone     = nrow.get('phone_number', '')
                reg_date  = parse_dt(nrow.get('date_registered', ''))

                rows.append((
                    email,
                    segment,
                    pack,
                    firstname,
                    lastname,
                    country,
                    city,
                    postal,
                    phone,
                    tags,
                    reg_date,
                    fname,
                ))
    return rows


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ══════════════════════════════════════════════════════════════════════
    # 1. public.sio_transactions
    # ══════════════════════════════════════════════════════════════════════
    print('\n=== 1. public.sio_transactions ===')

    run(cur, "DROP TABLE IF EXISTS public.sio_transactions CASCADE")
    run(cur, """
        CREATE TABLE public.sio_transactions (
            id               SERIAL PRIMARY KEY,
            email            TEXT NOT NULL,
            transaction_date TIMESTAMPTZ,
            amount_eur       NUMERIC(10,2),
            pack_code        TEXT,
            pack             TEXT,
            product_name     TEXT,
            invoice_number   TEXT UNIQUE,
            customer_firstname TEXT,
            customer_lastname  TEXT,
            country          TEXT,
            city             TEXT,
            postal_code      TEXT,
            address          TEXT,
            promo_code       TEXT,
            shipping_zone    TEXT,
            is_international BOOLEAN,
            imported_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """, 'table créée')

    txn_rows = parse_transactions()
    execute_values(cur, """
        INSERT INTO public.sio_transactions
          (email, transaction_date, amount_eur, pack_code, pack, product_name,
           invoice_number, customer_firstname, customer_lastname, country, city,
           postal_code, address, promo_code, shipping_zone, is_international)
        VALUES %s
        ON CONFLICT (invoice_number) DO NOTHING
    """, txn_rows)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM public.sio_transactions")
    print(f'  → {cur.fetchone()[0]} transactions importées')

    # ══════════════════════════════════════════════════════════════════════
    # 2. public.sio_contacts
    # ══════════════════════════════════════════════════════════════════════
    print('\n=== 2. public.sio_contacts ===')

    run(cur, "DROP TABLE IF EXISTS public.sio_contacts CASCADE")
    run(cur, """
        CREATE TABLE public.sio_contacts (
            id           SERIAL PRIMARY KEY,
            email        TEXT NOT NULL,
            segment      TEXT NOT NULL,
            pack         TEXT NOT NULL,
            first_name   TEXT,
            last_name    TEXT,
            country      TEXT,
            city         TEXT,
            postal_code  TEXT,
            phone        TEXT,
            tags         TEXT,
            registered_at TIMESTAMPTZ,
            source_file  TEXT,
            imported_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """, 'table créée')
    run(cur, "CREATE INDEX ON public.sio_contacts(email)")
    run(cur, "CREATE INDEX ON public.sio_contacts(segment, pack)")

    contact_rows = parse_contacts()
    execute_values(cur, """
        INSERT INTO public.sio_contacts
          (email, segment, pack, first_name, last_name, country, city,
           postal_code, phone, tags, registered_at, source_file)
        VALUES %s
    """, contact_rows)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM public.sio_contacts")
    print(f'  → {cur.fetchone()[0]} contacts importés')

    cur.execute("""
        SELECT segment, pack, COUNT(*) FROM public.sio_contacts
        GROUP BY segment, pack ORDER BY segment, pack
    """)
    for seg, pack, n in cur.fetchall():
        print(f'     {seg:20s}  {pack:20s}  {n:4d}')

    # ══════════════════════════════════════════════════════════════════════
    # 3. gold.sio_customer_segments
    # ══════════════════════════════════════════════════════════════════════
    print('\n=== 3. gold.sio_customer_segments ===')

    run(cur, "DROP TABLE IF EXISTS gold.sio_customer_segments CASCADE")
    run(cur, """
        CREATE TABLE gold.sio_customer_segments AS
        WITH purchases AS (
            SELECT email,
                   SUM(amount_eur)                        AS total_spent_eur,
                   COUNT(DISTINCT invoice_number)          AS nb_transactions,
                   COUNT(DISTINCT pack)                    AS nb_packs_purchased,
                   ARRAY_AGG(DISTINCT pack ORDER BY pack)  AS packs_purchased,
                   MIN(transaction_date)::DATE             AS first_purchase_date,
                   MAX(transaction_date)::DATE             AS last_purchase_date
            FROM public.sio_transactions
            GROUP BY email
        ),
        contacts_agg AS (
            SELECT email,
                   ARRAY_AGG(DISTINCT pack) FILTER (WHERE segment = 'adp_no_purchase')
                       AS packs_adp_abandoned,
                   ARRAY_AGG(DISTINCT pack) FILTER (WHERE segment = 'adp_converted')
                       AS packs_adp_converted,
                   ARRAY_AGG(DISTINCT pack) FILTER (WHERE segment = 'direct_purchase')
                       AS packs_direct,
                   MIN(registered_at)::DATE AS first_contact_date,
                   MAX(country) FILTER (WHERE country != '') AS country
            FROM public.sio_contacts
            GROUP BY email
        ),
        all_emails AS (
            SELECT email FROM public.sio_transactions
            UNION
            SELECT email FROM public.sio_contacts
        )
        SELECT
            ae.email,
            md5(ae.email)                       AS user_id_master,
            COALESCE(p.total_spent_eur, 0)      AS total_spent_eur,
            COALESCE(p.nb_transactions, 0)      AS nb_transactions,
            COALESCE(p.nb_packs_purchased, 0)   AS nb_packs_purchased,
            p.packs_purchased,
            p.first_purchase_date,
            p.last_purchase_date,
            c.packs_adp_abandoned,
            c.packs_adp_converted,
            c.packs_direct,
            c.first_contact_date,
            c.country,
            (p.nb_packs_purchased IS NOT NULL AND p.nb_packs_purchased > 1) AS is_multi_pack,
            (p.email IS NOT NULL)               AS has_purchased,
            (c.packs_adp_abandoned IS NOT NULL) AS has_abandoned,
            NOW()                               AS computed_at
        FROM all_emails ae
        LEFT JOIN purchases p USING (email)
        LEFT JOIN contacts_agg c USING (email)
    """, 'gold.sio_customer_segments créée')

    run(cur, "CREATE INDEX ON gold.sio_customer_segments(email)")
    run(cur, "CREATE INDEX ON gold.sio_customer_segments(user_id_master)")

    cur.execute("SELECT COUNT(*) FROM gold.sio_customer_segments")
    n = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM gold.sio_customer_segments WHERE has_purchased")
    n_bought = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM gold.sio_customer_segments WHERE is_multi_pack")
    n_multi = cur.fetchone()[0]
    print(f'  → {n} contacts uniques  |  {n_bought} acheteurs  |  {n_multi} multi-packs')

    # ══════════════════════════════════════════════════════════════════════
    # 4. gold.sio_conversion_funnel
    # ══════════════════════════════════════════════════════════════════════
    print('\n=== 4. gold.sio_conversion_funnel ===')

    run(cur, "DROP TABLE IF EXISTS gold.sio_conversion_funnel CASCADE")
    run(cur, """
        CREATE TABLE gold.sio_conversion_funnel AS
        WITH adp_contacts AS (
            -- Tous ceux qui sont entrés dans le funnel ADP (converti ou non)
            SELECT pack AS pack,
                   COUNT(DISTINCT email) FILTER (WHERE segment = 'adp_no_purchase') AS adp_abandoned,
                   COUNT(DISTINCT email) FILTER (WHERE segment = 'adp_converted')   AS adp_converted
            FROM public.sio_contacts
            GROUP BY pack
        ),
        revenue AS (
            SELECT t.pack,
                   COUNT(DISTINCT t.invoice_number)
                       FILTER (WHERE c.segment = 'direct_purchase')    AS nb_direct,
                   COUNT(DISTINCT t.invoice_number)
                       FILTER (WHERE c.segment = 'adp_converted')      AS nb_adp_conv,
                   COALESCE(SUM(t.amount_eur)
                       FILTER (WHERE c.segment = 'direct_purchase'), 0) AS revenue_direct,
                   COALESCE(SUM(t.amount_eur)
                       FILTER (WHERE c.segment = 'adp_converted'), 0)   AS revenue_adp,
                   COALESCE(SUM(t.amount_eur), 0)                        AS revenue_total,
                   ROUND(AVG(t.amount_eur), 2)                           AS avg_order_eur
            FROM public.sio_transactions t
            LEFT JOIN (
                SELECT DISTINCT email, segment, pack AS cpak FROM public.sio_contacts
            ) c ON c.email = t.email AND c.cpak = t.pack
            GROUP BY t.pack
        )
        SELECT
            a.pack,
            a.adp_abandoned,
            a.adp_converted,
            a.adp_abandoned + a.adp_converted                             AS total_adp,
            CASE WHEN (a.adp_abandoned + a.adp_converted) > 0
                 THEN ROUND(a.adp_converted::NUMERIC /
                            (a.adp_abandoned + a.adp_converted) * 100, 1)
                 ELSE 0 END                                               AS adp_conversion_rate_pct,
            COALESCE(r.nb_direct, 0)                                      AS nb_direct,
            COALESCE(r.nb_adp_conv, 0)                                    AS nb_adp_conv,
            COALESCE(r.nb_direct, 0) + COALESCE(r.nb_adp_conv, 0)        AS total_purchased,
            COALESCE(r.revenue_direct, 0)                                 AS revenue_direct_eur,
            COALESCE(r.revenue_adp, 0)                                    AS revenue_adp_eur,
            COALESCE(r.revenue_total, 0)                                  AS revenue_total_eur,
            r.avg_order_eur,
            NOW()                                                         AS computed_at
        FROM adp_contacts a
        LEFT JOIN revenue r USING (pack)
        ORDER BY a.pack
    """, 'gold.sio_conversion_funnel créée')

    cur.execute("""
        SELECT pack, adp_abandoned, adp_converted, adp_conversion_rate_pct,
               nb_direct, revenue_total_eur
        FROM gold.sio_conversion_funnel
        ORDER BY pack
    """)
    print(f'\n  {"Pack":<20} {"ADP abandon":>11} {"ADP converti":>12} {"Taux conv.":>10} {"Direct":>8} {"CA total":>10}')
    print('  ' + '-' * 80)
    for row in cur.fetchall():
        pack, aband, conv, rate, direct, ca = row
        print(f'  {pack:<20} {aband or 0:>11} {conv or 0:>12} {rate or 0:>9}% {direct or 0:>8} {ca or 0:>9}€')

    cur.execute("SELECT SUM(revenue_total_eur) FROM gold.sio_conversion_funnel")
    total = cur.fetchone()[0]
    print(f'\n  CA total SIO : {total}€')

    # ══════════════════════════════════════════════════════════════════════
    # 5. Qualification : croisement avec PrestaShop, MailerLite, users_master
    # ══════════════════════════════════════════════════════════════════════
    print('\n=== 5. Qualification ===')

    # SIO_START = première vraie transaction (avant = client existant)
    SIO_START = '2026-02-21'

    # Ajouter colonnes de qualification
    run(cur, """
        ALTER TABLE gold.sio_customer_segments
          ADD COLUMN IF NOT EXISTS is_presta_existing     BOOLEAN,
          ADD COLUMN IF NOT EXISTS presta_orders_pre_sio  INT,
          ADD COLUMN IF NOT EXISTS presta_spend_pre_sio   NUMERIC(10,2),
          ADD COLUMN IF NOT EXISTS presta_first_order     DATE,
          ADD COLUMN IF NOT EXISTS is_presta_newsletter   BOOLEAN,
          ADD COLUMN IF NOT EXISTS is_ml_subscriber       BOOLEAN,
          ADD COLUMN IF NOT EXISTS ml_status              TEXT,
          ADD COLUMN IF NOT EXISTS ml_subscribed_at       DATE,
          ADD COLUMN IF NOT EXISTS ml_groups              TEXT,
          ADD COLUMN IF NOT EXISTS is_in_users_master     BOOLEAN,
          ADD COLUMN IF NOT EXISTS customer_type          TEXT
    """, 'colonnes qualification ajoutées')

    # Remplir depuis gold.orders (historique PrestaShop)
    run(cur, f"""
        UPDATE gold.sio_customer_segments s
        SET
            presta_orders_pre_sio = o.nb_orders,
            presta_spend_pre_sio  = o.total_spend,
            presta_first_order    = o.first_order,
            is_presta_existing    = (o.nb_orders > 0)
        FROM (
            SELECT email,
                   COUNT(*)                         AS nb_orders,
                   COALESCE(SUM(total_paid_eur), 0) AS total_spend,
                   MIN(ordered_at)::DATE             AS first_order
            FROM gold.orders
            WHERE ordered_at < '{SIO_START}'
              AND email IS NOT NULL
            GROUP BY email
        ) o
        WHERE LOWER(s.email) = LOWER(o.email)
    """, 'gold.orders → PrestaShop historique')

    # Mettre is_presta_existing = FALSE là où aucun match
    run(cur, """
        UPDATE gold.sio_customer_segments
        SET is_presta_existing = FALSE,
            presta_orders_pre_sio = 0,
            presta_spend_pre_sio  = 0
        WHERE is_presta_existing IS NULL
    """, 'is_presta_existing = FALSE par défaut')

    # Newsletter PrestaShop
    run(cur, """
        UPDATE gold.sio_customer_segments s
        SET is_presta_newsletter = pc.is_newsletter
        FROM (
            SELECT LOWER(email) AS email, MAX(is_newsletter::int)::bool AS is_newsletter
            FROM clean.presta_customers
            WHERE email IS NOT NULL
            GROUP BY LOWER(email)
        ) pc
        WHERE LOWER(s.email) = pc.email
    """, 'presta newsletter')

    run(cur, """
        UPDATE gold.sio_customer_segments
        SET is_presta_newsletter = FALSE
        WHERE is_presta_newsletter IS NULL
    """)

    # MailerLite
    run(cur, """
        UPDATE gold.sio_customer_segments s
        SET
            is_ml_subscriber  = TRUE,
            ml_status         = ml.status,
            ml_subscribed_at  = ml.date_subscribe::DATE,
            ml_groups         = ml.groups::TEXT
        FROM (
            SELECT LOWER(email) AS email,
                   status,
                   MIN(date_subscribe) AS date_subscribe,
                   STRING_AGG(DISTINCT groups::TEXT, ', ') AS groups
            FROM clean.ml_subscribers
            WHERE email IS NOT NULL
            GROUP BY LOWER(email), status
        ) ml
        WHERE LOWER(s.email) = ml.email
    """, 'MailerLite subscribers')

    run(cur, """
        UPDATE gold.sio_customer_segments
        SET is_ml_subscriber = FALSE
        WHERE is_ml_subscriber IS NULL
    """)

    # users_master
    run(cur, """
        UPDATE gold.sio_customer_segments s
        SET is_in_users_master = TRUE
        FROM gold.users_master um
        WHERE LOWER(s.email) = LOWER(um.primary_email)
    """, 'users_master match')

    run(cur, """
        UPDATE gold.sio_customer_segments
        SET is_in_users_master = FALSE
        WHERE is_in_users_master IS NULL
    """)

    # customer_type dérivé
    run(cur, """
        UPDATE gold.sio_customer_segments
        SET customer_type = CASE
            WHEN is_presta_existing THEN 'existing_customer'
            WHEN has_purchased AND NOT is_presta_existing THEN 'new_customer'
            WHEN NOT has_purchased AND is_ml_subscriber THEN 'ml_only'
            ELSE 'unknown'
        END
    """, 'customer_type calculé')

    # ── Résumé qualification ───────────────────────────────────────────
    cur.execute("""
        SELECT
            customer_type,
            COUNT(*)                                                 AS total,
            COUNT(*) FILTER (WHERE has_purchased)                    AS purchased,
            COUNT(*) FILTER (WHERE is_ml_subscriber)                 AS ml_sub,
            COUNT(*) FILTER (WHERE ml_status = 'active')             AS ml_active,
            ROUND(AVG(CASE WHEN has_purchased THEN total_spent_eur END), 2) AS avg_spend
        FROM gold.sio_customer_segments
        GROUP BY customer_type
        ORDER BY total DESC
    """)
    print(f'\n  {"Type":<22} {"Total":>6} {"Acheté":>7} {"ML sub":>7} {"ML actif":>9} {"CA moy":>8}')
    print('  ' + '-' * 65)
    for row in cur.fetchall():
        typ, tot, purch, ml, mla, avg = row
        print(f'  {typ or "?":<22} {tot:>6} {purch or 0:>7} {ml or 0:>7} {mla or 0:>9} {avg or 0:>7}€')

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE is_presta_existing)     AS existing_presta,
            COUNT(*) FILTER (WHERE NOT is_presta_existing AND has_purchased) AS new_buyers,
            COUNT(*) FILTER (WHERE is_ml_subscriber)       AS ml_subs,
            COUNT(*) FILTER (WHERE is_presta_newsletter)   AS presta_nl,
            COUNT(*) FILTER (WHERE is_in_users_master)     AS in_master
        FROM gold.sio_customer_segments
    """)
    ex, new_b, ml, nl, master = cur.fetchone()
    print(f'''
  Clients existants PrestaShop  : {ex}
  Nouveaux acheteurs SIO        : {new_b}
  Abonnés MailerLite            : {ml}
  Abonnés newsletter PrestaShop : {nl}
  Déjà dans users_master        : {master}
''')

    print('Done.')
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()

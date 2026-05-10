#!/usr/bin/env python3
"""
pipeline_21_marketing_timeline.py — gold.marketing_events + gold.product_launches

Sources:
  - ml_campaigns (déjà en base) → emails de lancement
  - Mapping manuel : campagne → catalog_id(s)

Tables produites:
  gold.marketing_events   — toutes les campagnes classifiées (channel, type, stats)
  gold.product_launches   — vue croisée : launch_date + print_month + 1ère vente PrestaShop

Corrélation finale disponible dans gold.launch_timeline (VIEW).
"""

import os
import psycopg2
from psycopg2.extras import execute_values

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

# ---------------------------------------------------------------------------
# Mapping campagne ML → canonical_name(s) du catalogue
# Basé sur lecture des sujets + noms de campagnes
# ---------------------------------------------------------------------------
LAUNCH_MAPPING = {
    # Foulane
    'LE NOUVEAU FOULANE':                        ['Famille Foulane'],          # T? (2021, avant Zoho)
    'TOME 7 DE LA FAMILLE FOULANE':              ['Famille Foulane T07 — Le Voleur'],
    'TOME 9 DE LA FAMILLE FOULANE':              ['Famille Foulane T09 — Tempête'],
    'Tome 10 de la Famille Foulane':             ['Famille Foulane T10 — En \'Omra à Médine'],
    'TOME 11 de la Famille Foulane':             ['Famille Foulane T11 — En \'Omra à La Mecque'],
    'Foulane Tome 10':                           ['Famille Foulane T10 — En \'Omra à Médine'],
    # Walad & Binti
    "Walad & Binti' en format BD":               ['Walad & Binti T1'],
    'WLD.BNT':                                   ['Muslim Show WT2 Manga'],
    'premier manga du Studio BDouin':            ['Muslim Show WT2 Manga'],
    'Walad & Binti se prennent pour des détect': ['Walad & Binti T3 — Enquêtes et Imbroglios'],
    'Walad & Binti T3':                          ['Walad & Binti T3 — Enquêtes et Imbroglios'],
    # Walad Découvre
    'Aventure à Médine avec Walad':              ['Walad Découvre Médine'],
    'cadeau idéal de l\'aïd vient d\'arriver':   ['Walad Découvre La Mecque'],
    # Muslim Show
    '15 ans de MuslimShow':                      ['Muslim Show Collector'],
    'édition premium disponible':                ['Muslim Show Collector'],
    # Guides
    'guide de la Salat et les Ablutions':        ['Guide Salat Fille', 'Guide Salat Garçon'],
    'Lancement "Bonnes manières"':               ['Guide 100 Bonnes Manières en Islam'],
    'Bonnes Manières à vos enfants':             ['Guide 100 Bonnes Manières en Islam'],
    'Agenda de la Famille Foulane':              ['Agenda de la Famille Foulane'],
    'citadelle du Petit Muslim':                 ['Guide Citadelle du Petit Muslim'],
    # Awlad School (papier)
    'livres Awlad School sont enfin disponibles':  ['Awlad School'],
    "cahiers d'écritures Awlad Kitaba":            ['Cahier Écriture Arabe'],
    'Awlad School en format papier':               ['Awlad School'],
    # Agence Règle Tout
    "L'Agence Règle Tout":                       ['Agence Règle Tout V1 — Mauvaise Influence',
                                                   'Agence Règle Tout V2 — Le Sorcier des Maréca',
                                                   'Agence Règle Tout V3 — Les Harceleurs'],
    'nouvelle série jeunesse':                   ['Agence Règle Tout V1 — Mauvaise Influence',
                                                   'Agence Règle Tout V2 — Le Sorcier des Maréca',
                                                   'Agence Règle Tout V3 — Les Harceleurs'],
    # HALUA
    'HALUA':                                     ['HALUA'],
    # Guide Hajj
    'Guide du Hajj':                             ['Guide Hajj & Omra'],
}

# Types d'événements marketing
def classify_event(name: str, subject: str) -> str:
    s = (name + ' ' + subject).lower()
    if any(k in s for k in ['lancement', 'arrivé', 'disponible', 'sorti', 'nouveauté',
                              'nouveau ', 'nouvelle ', 'vient de sortir', 'est arrivé',
                              'enfin disponible', 'juste arrivée', 'vient d\'arriver',
                              'découvrez', 'vient d\'arriver']):
        return 'launch'
    if any(k in s for k in ['relance', 'no open', 'non opener', 'dernière chance',
                              'encore temps', 'avez manqué']):
        return 'relaunch'
    if any(k in s for k in ['offre', 'ramadan', 'aïd', 'promo', 'pack', 'vente privée',
                              'livraison', 'réduction']):
        return 'promotion'
    if any(k in s for k in ['concours', 'tournoi', 'contest', 'gagnant', 'résultat']):
        return 'contest'
    if any(k in s for k in ['app', 'android', 'iphone', 'ipad', 'plateforme', 'hoopow',
                              'awlad quiz', 'awlad games', 'awlad salat', 'story maker']):
        return 'app_launch'
    return 'newsletter'


DDL = """
CREATE TABLE IF NOT EXISTS gold.marketing_events (
    id              SERIAL PRIMARY KEY,
    ml_campaign_id  BIGINT,
    event_date      DATE NOT NULL,
    channel         TEXT DEFAULT 'email',
    event_type      TEXT,           -- launch / relaunch / promotion / contest / app_launch / newsletter
    campaign_name   TEXT,
    subject         TEXT,
    audience_size   INTEGER,
    open_count      INTEGER,
    open_rate       NUMERIC(5,2),
    click_count     INTEGER,
    click_rate      NUMERIC(5,2),
    titles_mentioned TEXT[],        -- canonical_names des produits mentionnés
    catalog_ids     INTEGER[],      -- IDs dans gold.catalog
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS me_event_date  ON gold.marketing_events(event_date);
CREATE INDEX IF NOT EXISTS me_event_type  ON gold.marketing_events(event_type);
CREATE INDEX IF NOT EXISTS me_catalog_ids ON gold.marketing_events USING gin(catalog_ids);
"""

LAUNCH_TIMELINE_VIEW = """
CREATE OR REPLACE VIEW gold.launch_timeline AS
SELECT
    c.catalog_id,
    c.canonical_name,
    c.series,
    c.tome_number,
    -- Print
    ph.print_month,
    ph.run_number,
    ph.quantity      AS print_qty,
    ph.cost_eur      AS print_cost_eur,
    -- Launch email
    me.event_date    AS launch_email_date,
    me.audience_size AS launch_email_audience,
    me.open_rate     AS launch_email_open_rate,
    -- Délais
    (me.event_date - ph.print_month::date) AS days_print_to_email,
    (ph.first_presta_sale_date - ph.print_month::date) AS days_print_to_first_sale,
    (ph.first_presta_sale_date - me.event_date)        AS days_email_to_first_sale,
    -- 1ère vente PrestaShop
    ph.first_presta_sale_date
FROM gold.catalog c
LEFT JOIN gold.print_history ph
    ON ph.catalog_id = c.catalog_id AND ph.run_number = 1
LEFT JOIN LATERAL (
    SELECT me2.event_date, me2.audience_size, me2.open_rate
    FROM gold.marketing_events me2
    WHERE c.catalog_id = ANY(me2.catalog_ids)
      AND me2.event_type IN ('launch', 'newsletter')
    ORDER BY me2.event_date ASC
    LIMIT 1
) me ON TRUE
WHERE ph.print_month IS NOT NULL OR me.event_date IS NOT NULL
ORDER BY COALESCE(me.event_date, ph.print_month::date) ASC NULLS LAST;
"""


def get_catalog_ids_for_titles(cur, titles: list[str]) -> list[int]:
    ids = []
    for title in titles:
        cur.execute("""
            SELECT catalog_id FROM gold.catalog
            WHERE canonical_name ILIKE %s
               OR canonical_name ILIKE %s
            LIMIT 1
        """, (title, f"%{title}%"))
        row = cur.fetchone()
        if row:
            ids.append(row[0])
        else:
            # Essayer via aliases
            cur.execute("""
                SELECT catalog_id FROM gold.catalog_aliases
                WHERE alias_name ILIKE %s LIMIT 1
            """, (f"%{title}%",))
            row = cur.fetchone()
            if row:
                ids.append(row[0])
    return ids


def match_titles(name: str, subject: str) -> list[str]:
    combined = name + ' ' + subject
    matched = []
    for keyword, titles in LAUNCH_MAPPING.items():
        if keyword.lower() in combined.lower():
            matched.extend(titles)
    return list(set(matched))


def run():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute(DDL)
    conn.commit()
    print("✓ gold.marketing_events table ready")

    # Charger campagnes uniques depuis ml_campaigns
    cur.execute("""
        SELECT DISTINCT ON (date_send::date, name)
            id, name, date_send, subject,
            opened_count, clicked_count, opened_rate, clicked_rate, total_recipients
        FROM ml_campaigns
        WHERE date_send IS NOT NULL
        ORDER BY date_send::date, name, date_send
    """)
    campaigns = cur.fetchall()
    print(f"  {len(campaigns)} campagnes chargées")

    rows = []
    launches_found = 0

    for (cid, name, ds, subject, opens, clicks, open_rate, click_rate, total) in campaigns:
        event_type = classify_event(name or '', subject or '')
        titles = match_titles(name or '', subject or '')
        cat_ids = get_catalog_ids_for_titles(cur, titles) if titles else []

        if titles:
            launches_found += 1

        rows.append((
            cid,
            ds.date() if ds else None,
            'email',
            event_type,
            name,
            subject,
            total,
            opens,
            float(open_rate) if open_rate else None,
            clicks,
            float(click_rate) if click_rate else None,
            titles or None,
            cat_ids or None,
        ))

    cur.execute("TRUNCATE gold.marketing_events RESTART IDENTITY")
    execute_values(cur, """
        INSERT INTO gold.marketing_events
            (ml_campaign_id, event_date, channel, event_type, campaign_name, subject,
             audience_size, open_count, open_rate, click_count, click_rate,
             titles_mentioned, catalog_ids)
        VALUES %s
    """, rows)
    conn.commit()
    print(f"✓ {len(rows)} campagnes insérées ({launches_found} avec titres matchés)")

    # Créer la vue de corrélation
    cur.execute(LAUNCH_TIMELINE_VIEW)
    conn.commit()
    print("✓ VIEW gold.launch_timeline créée")

    # Rapport : timeline des lancements produits
    cur.execute("""
        SELECT event_date, titles_mentioned, audience_size, open_rate, event_type
        FROM gold.marketing_events
        WHERE titles_mentioned IS NOT NULL
          AND event_type IN ('launch', 'promotion')
        ORDER BY event_date
    """)
    print(f"\n{'Date':>10} | {'Type':<10} | {'Titres':<50} | {'Dest':>7} | {'Ouv%':>5}")
    print("-" * 95)
    for (dt, titles, aud, rate, etype) in cur.fetchall():
        t = ', '.join(titles or [])[:50]
        print(f"{str(dt):>10} | {(etype or '')::<10} | {t:<50} | {(aud or 0):>7,} | {str(round(rate or 0))+'%':>5}")

    # Vue corrélation complète
    print("\n=== LAUNCH TIMELINE (corrélation print → email → 1ère vente) ===")
    cur.execute("""
        SELECT canonical_name, print_month, launch_email_date,
               days_print_to_email, days_email_to_first_sale,
               launch_email_audience, launch_email_open_rate
        FROM gold.launch_timeline
        ORDER BY COALESCE(launch_email_date, print_month::date) NULLS LAST
    """)
    print(f"\n{'Titre':<45} | {'Print':>10} | {'Email':>10} | {'J print→email':>14} | {'J email→vente':>14} | {'Dest':>7} | {'Ouv%':>5}")
    print("-" * 130)
    for r in cur.fetchall():
        name, pm, ed, d1, d2, aud, rate = r
        print(f"{(name or '')[:44]:<45} | {str(pm or '?'):>10} | {str(ed or '?'):>10} | {str(d1 or '?'):>14} | {str(d2 or '?'):>14} | {(aud or 0):>7,} | {str(round(rate or 0))+'%':>5}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    run()

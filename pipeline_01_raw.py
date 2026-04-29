#!/usr/bin/env python3
"""
pipeline_01_raw.py — Étape 1 : préparer la couche RAW.

Stratégie : on garde public.* tel quel (immutable de fait),
            on crée raw.* en VIEW + traceability columns matérialisées
            via une vue augmentée. Disque économisé.

Approche pratique :
  - Pour chaque table public.X :
    * CREATE VIEW raw.X AS SELECT *, source_name, NOW() AS import_date FROM public.X
  - Le row_id_raw est stocké dans clean.* car les vues ne le persistent pas

→ public = source de vérité immuable
→ raw = vue pivot avec source_name et import_date
"""

import psycopg2

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# Mapping table → source_name canonique
SOURCE_MAP = {
    'presta_': 'prestashop',
    'ga4_':    'ga4',
    'gsc_':    'search_console',
    'ml_':     'mailerlite',
    'meta_':   'meta',
    'asc_':    'app_store_connect',
    'playstore_': 'play_console',
    'drive_':  'google_drive',
    'gmail_':  'gmail',
    'imak_':   'imak',
    'sofiadis_': 'sofiadis',
    'web_mentions': 'web_mentions',
    'reviews': 'reviews_apps',
    'raw_':    'raw_documents',
}


def get_source(tbl):
    for prefix, name in SOURCE_MAP.items():
        if tbl.startswith(prefix):
            return name
    return 'unknown'


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        SELECT tablename FROM pg_tables WHERE schemaname='public'
        ORDER BY tablename
    """)
    tables = [r[0] for r in cur.fetchall()]

    print(f'Creating {len(tables)} raw views...')
    for tbl in tables:
        source = get_source(tbl)
        view_name = f'raw."{tbl}"'
        try:
            cur.execute(f'DROP VIEW IF EXISTS {view_name}')
            cur.execute(f"""
                CREATE VIEW {view_name} AS
                SELECT *,
                       '{source}'::text AS source_name,
                       'public.{tbl}'::text AS file_origin,
                       NOW() AS view_built_at
                FROM public."{tbl}"
            """)
            print(f'  ✓ raw.{tbl} ({source})')
        except Exception as e:
            print(f'  ✗ raw.{tbl}: {e}')

    print('\n✓ All raw views created')

    # Stats
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.views WHERE table_schema='raw'
    """)
    print(f'Total raw views: {cur.fetchone()[0]}')
    conn.close()


if __name__ == '__main__':
    main()

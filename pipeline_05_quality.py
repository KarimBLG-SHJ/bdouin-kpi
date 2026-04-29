#!/usr/bin/env python3
"""
pipeline_05_quality.py — Étape 5 : rapport qualité + dictionnaire de données.

Génère :
  logs.data_quality_report — table avec métriques qualité par table/colonne
  dict.data_dictionary     — description tables + champs + transformations
  /Users/karim/Downloads/BDouin_Quality_Report.json
"""

import psycopg2
import json
from datetime import datetime

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ─── Quality metrics ──────────────────────────────────────────────
    print('Computing quality metrics...\n')

    metrics = {}

    # Customers email validity
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE is_valid_email)::float / COUNT(*)::float AS pct_valid,
            COUNT(*) FILTER (WHERE is_missing_email) AS missing,
            COUNT(*) FILTER (WHERE is_duplicate) AS duplicates
        FROM clean.presta_customers
    """)
    pct_valid, missing, duplicates = cur.fetchone()
    metrics['presta_customers'] = {
        'total': sum(cur.execute("SELECT COUNT(*) FROM clean.presta_customers") or [0]) or cur.fetchone()[0],
        'pct_valid_email': round(pct_valid * 100, 2),
        'missing_email': missing,
        'duplicate_emails': duplicates,
    }
    cur.execute("SELECT COUNT(*) FROM clean.presta_customers")
    metrics['presta_customers']['total'] = cur.fetchone()[0]

    # MailerLite
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE is_valid_email) AS valid,
            COUNT(*) FILTER (WHERE is_missing_email) AS missing,
            COUNT(*) FILTER (WHERE is_duplicate) AS duplicates,
            COUNT(*) FILTER (WHERE is_active) AS active,
            COUNT(*) FILTER (WHERE is_unsubscribed) AS unsubscribed,
            COUNT(*) FILTER (WHERE is_bounced) AS bounced
        FROM clean.ml_subscribers
    """)
    total, valid, missing, dup, active, unsub, bounce = cur.fetchone()
    metrics['ml_subscribers'] = {
        'total': total,
        'valid_email': valid,
        'pct_valid_email': round(100 * valid / total, 2),
        'missing_email': missing,
        'duplicate_emails': dup,
        'active': active,
        'unsubscribed': unsub,
        'bounced': bounce,
    }

    # Addresses phone validity
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE is_valid_phone) AS valid_phone,
            COUNT(*) FILTER (WHERE is_missing_city) AS missing_city,
            COUNT(*) FILTER (WHERE is_missing_phone) AS missing_phone
        FROM clean.presta_addresses
    """)
    total, valid_p, miss_c, miss_p = cur.fetchone()
    metrics['presta_addresses'] = {
        'total': total,
        'valid_phone': valid_p,
        'pct_valid_phone': round(100 * valid_p / total, 2),
        'missing_city': miss_c,
        'missing_phone': miss_p,
    }

    # User matching quality
    cur.execute("""
        SELECT
            COUNT(*) AS total_users,
            COUNT(*) FILTER (WHERE in_prestashop AND in_mailerlite) AS in_both,
            COUNT(*) FILTER (WHERE in_prestashop AND NOT in_mailerlite) AS shop_only,
            COUNT(*) FILTER (WHERE NOT in_prestashop AND in_mailerlite) AS ml_only
        FROM gold.users_master
    """)
    total, both, sp, mo = cur.fetchone()
    metrics['users_master'] = {
        'total_unique_users': total,
        'in_both_shop_and_ml': both,
        'pct_in_both': round(100 * both / total, 2),
        'shop_only': sp,
        'mailerlite_only': mo,
    }

    # Orders linked to user_master
    cur.execute("""
        SELECT
            COUNT(*) AS total_orders,
            COUNT(*) FILTER (WHERE user_id_master IS NOT NULL) AS linked,
            COUNT(*) FILTER (WHERE is_unpaid) AS unpaid
        FROM gold.orders
    """)
    total, linked, unpaid = cur.fetchone()
    metrics['orders'] = {
        'total': total,
        'linked_to_user_master': linked,
        'pct_linked_to_user': round(100 * linked / total, 2) if total else 0,
        'unpaid': unpaid,
    }

    # Order_items linked to product_master
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE product_id_master IS NOT NULL) AS linked,
            COUNT(*) FILTER (WHERE ean13 IS NOT NULL) AS with_ean
        FROM gold.order_items
    """)
    total, linked, with_ean = cur.fetchone()
    metrics['order_items'] = {
        'total': total,
        'linked_to_product_master': linked,
        'pct_linked_to_product': round(100 * linked / total, 2) if total else 0,
        'with_ean': with_ean,
    }

    # Display
    print(json.dumps(metrics, indent=2, ensure_ascii=False))

    # Save to logs schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs.data_quality_report (
            run_at      TIMESTAMP DEFAULT NOW(),
            metrics     JSONB,
            PRIMARY KEY (run_at)
        )
    """)
    cur.execute("INSERT INTO logs.data_quality_report (metrics) VALUES (%s)", (json.dumps(metrics),))
    conn.commit()

    # Save JSON
    out = '/Users/karim/Downloads/BDouin_Quality_Report.json'
    with open(out, 'w') as f:
        json.dump({
            'generated_at': datetime.utcnow().isoformat(),
            'metrics': metrics,
        }, f, indent=2, ensure_ascii=False)
    print(f'\n✓ Saved: {out}')

    # ─── Data dictionary ──────────────────────────────────────────────
    print('\nBuilding data dictionary...\n')

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dict.data_dictionary (
            schema_name     TEXT,
            table_name      TEXT,
            column_name     TEXT,
            data_type       TEXT,
            is_nullable     TEXT,
            description     TEXT,
            transformation  TEXT,
            updated_at      TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (schema_name, table_name, column_name)
        )
    """)
    conn.commit()

    cur.execute("""
        INSERT INTO dict.data_dictionary
          (schema_name, table_name, column_name, data_type, is_nullable)
        SELECT table_schema, table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema IN ('public','clean','gold','logs','dict')
        ON CONFLICT (schema_name, table_name, column_name) DO UPDATE SET
          data_type = EXCLUDED.data_type,
          is_nullable = EXCLUDED.is_nullable,
          updated_at = NOW()
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM dict.data_dictionary")
    print(f'  ✓ {cur.fetchone()[0]} entrées dans dict.data_dictionary')

    # Add canonical descriptions for gold tables
    DESCRIPTIONS = {
        ('gold', 'users_master', 'user_id_master'): ('TEXT', 'Hash MD5 de l\'email — clé universelle d\'identification utilisateur'),
        ('gold', 'orders', 'user_id_master'): ('TEXT', 'Lien vers gold.users_master — permet de croiser commandes/abonnements'),
        ('gold', 'order_items', 'product_id_master'): ('TEXT', 'Lien vers gold.products_master — permet de croiser ventes/stock'),
        ('gold', 'feedback', 'feedback_type'): ('TEXT', 'app_review (Apple/Google) ou ig_comment'),
        ('clean', 'presta_customers', 'is_valid_email'): ('BOOLEAN', 'Email matche regex RFC simplifiée'),
        ('clean', 'presta_customers', 'is_duplicate'): ('BOOLEAN', 'Email partagé avec >=1 autre customer'),
    }
    for (schema, tbl, col), (dtype, desc) in DESCRIPTIONS.items():
        cur.execute("""
            UPDATE dict.data_dictionary
            SET description = %s
            WHERE schema_name=%s AND table_name=%s AND column_name=%s
        """, (desc, schema, tbl, col))
    conn.commit()
    print('  ✓ Descriptions canoniques ajoutées')

    conn.close()
    print('\n✓ Pipeline complete')


if __name__ == '__main__':
    main()

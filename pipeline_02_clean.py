#!/usr/bin/env python3
"""
pipeline_02_clean.py — Étape 2 : couche CLEAN avec normalisation + flags.

Pour chaque table importante :
  - Création clean.{table}
  - Normalisation : lowercase emails, trim, format date, ISO-2 pays
  - Flags : is_valid_email, is_valid_phone, is_missing_*, is_duplicate
  - Traceability : row_id_raw (UUID), source_name, import_date

NE SUPPRIME RIEN. Garde tous les nulls, tous les doublons.
"""

import psycopg2

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'


def run(cur, sql, label=''):
    """Exécute et log."""
    try:
        cur.execute(sql)
        cur.connection.commit()
        if label:
            print(f'  ✓ {label}')
    except Exception as e:
        cur.connection.rollback()
        print(f'  ✗ {label}: {str(e)[:200]}')


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ─── Helpers SQL réutilisables ────────────────────────────────────
    cur.execute("""
        CREATE OR REPLACE FUNCTION clean_email(t text) RETURNS text AS $$
            SELECT NULLIF(LOWER(BTRIM(t)), '')
        $$ LANGUAGE sql IMMUTABLE;

        CREATE OR REPLACE FUNCTION is_valid_email(t text) RETURNS boolean AS $$
            SELECT t ~ '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$'
        $$ LANGUAGE sql IMMUTABLE;

        CREATE OR REPLACE FUNCTION clean_phone(t text) RETURNS text AS $$
            SELECT NULLIF(REGEXP_REPLACE(t, '[^0-9+]', '', 'g'), '')
        $$ LANGUAGE sql IMMUTABLE;

        CREATE OR REPLACE FUNCTION is_valid_phone(t text) RETURNS boolean AS $$
            SELECT LENGTH(REGEXP_REPLACE(COALESCE(t,''), '[^0-9]', '', 'g')) BETWEEN 7 AND 15
        $$ LANGUAGE sql IMMUTABLE;

        CREATE OR REPLACE FUNCTION clean_text(t text) RETURNS text AS $$
            SELECT NULLIF(REGEXP_REPLACE(BTRIM(t), '\\s+', ' ', 'g'), '')
        $$ LANGUAGE sql IMMUTABLE;
    """)
    conn.commit()
    print('✓ Helper functions ready\n')

    # ─── 1. clean.presta_customers ────────────────────────────────────
    print('[clean.presta_customers]')
    run(cur, "DROP TABLE IF EXISTS clean.presta_customers CASCADE")
    run(cur, """
        CREATE TABLE clean.presta_customers AS
        SELECT
            gen_random_uuid()                AS row_id_raw,
            'prestashop'::text               AS source_name,
            'public.presta_customers'::text  AS source_table,
            NOW()                            AS imported_at,
            id                               AS source_id,
            clean_email(email)               AS email,
            is_valid_email(clean_email(email)) AS is_valid_email,
            clean_text(firstname)            AS firstname,
            clean_text(lastname)             AS lastname,
            id_gender,
            CASE WHEN birthday::text IN ('0001-01-01','1970-01-01') THEN NULL ELSE birthday END AS birthday,
            (newsletter = 1)                 AS newsletter_optin,
            (optin = 1)                      AS marketing_optin,
            clean_text(company)              AS company,
            (is_guest = 1)                   AS is_guest,
            (active = 1)                     AS is_active,
            (deleted = 1)                    AS is_deleted,
            date_add                         AS created_at,
            date_upd                         AS updated_at,
            (email IS NULL OR BTRIM(email) = '') AS is_missing_email,
            (firstname IS NULL OR BTRIM(firstname) = '') AS is_missing_firstname,
            (birthday IS NULL OR birthday::text IN ('0001-01-01','1970-01-01')) AS is_missing_birthday
        FROM public.presta_customers
    """, 'clean.presta_customers created')

    cur.execute("""
        UPDATE clean.presta_customers c
        SET is_duplicate = TRUE,
            duplicate_group_id = sub.gid
        FROM (
            SELECT email,
                   md5(email)::text AS gid,
                   COUNT(*) OVER (PARTITION BY email) AS n
            FROM clean.presta_customers
            WHERE email IS NOT NULL AND is_valid_email
        ) sub
        WHERE c.email = sub.email AND sub.n > 1
    """) if False else None  # need to add columns first

    run(cur, """
        ALTER TABLE clean.presta_customers
          ADD COLUMN is_duplicate BOOLEAN DEFAULT FALSE,
          ADD COLUMN duplicate_group_id TEXT
    """, 'duplicate columns added')

    run(cur, """
        UPDATE clean.presta_customers c
        SET is_duplicate = TRUE,
            duplicate_group_id = md5(c.email)
        FROM (
            SELECT email
            FROM clean.presta_customers
            WHERE email IS NOT NULL AND is_valid_email
            GROUP BY email
            HAVING COUNT(*) > 1
        ) dup
        WHERE c.email = dup.email
    """, 'duplicates flagged')

    run(cur, "CREATE INDEX ON clean.presta_customers(email)")
    run(cur, "CREATE INDEX ON clean.presta_customers(source_id)")

    # ─── 2. clean.presta_addresses ────────────────────────────────────
    print('\n[clean.presta_addresses]')
    run(cur, "DROP TABLE IF EXISTS clean.presta_addresses CASCADE")
    run(cur, """
        CREATE TABLE clean.presta_addresses AS
        SELECT
            gen_random_uuid()                AS row_id_raw,
            'prestashop'::text               AS source_name,
            'public.presta_addresses'::text  AS source_table,
            NOW()                            AS imported_at,
            id                               AS source_id,
            id_customer                      AS source_customer_id,
            id_country                       AS country_id,
            clean_text(alias)                AS alias,
            clean_text(firstname)            AS firstname,
            clean_text(lastname)             AS lastname,
            clean_text(company)              AS company,
            clean_text(address1)             AS address1,
            clean_text(address2)             AS address2,
            clean_text(postcode)             AS postcode,
            clean_text(city)                 AS city,
            clean_phone(phone)               AS phone,
            is_valid_phone(phone)            AS is_valid_phone,
            clean_phone(phone_mobile)        AS phone_mobile,
            is_valid_phone(phone_mobile)     AS is_valid_phone_mobile,
            clean_text(vat_number)           AS vat_number,
            (deleted = 1)                    AS is_deleted,
            date_add                         AS created_at,
            date_upd                         AS updated_at,
            (city IS NULL OR BTRIM(city) = '') AS is_missing_city,
            (postcode IS NULL OR BTRIM(postcode) = '') AS is_missing_postcode,
            (phone IS NULL AND phone_mobile IS NULL) AS is_missing_phone
        FROM public.presta_addresses
    """, 'clean.presta_addresses created')

    run(cur, "CREATE INDEX ON clean.presta_addresses(source_customer_id)")

    # ─── 3. clean.presta_orders ───────────────────────────────────────
    print('\n[clean.presta_orders]')
    run(cur, "DROP TABLE IF EXISTS clean.presta_orders CASCADE")
    run(cur, """
        CREATE TABLE clean.presta_orders AS
        SELECT
            gen_random_uuid()                AS row_id_raw,
            'prestashop'::text               AS source_name,
            'public.presta_orders'::text     AS source_table,
            NOW()                            AS imported_at,
            id                               AS source_id,
            id_customer                      AS source_customer_id,
            id_address_delivery              AS source_address_delivery_id,
            id_address_invoice               AS source_address_invoice_id,
            clean_text(reference)            AS reference,
            clean_text(payment)              AS payment_method,
            clean_text(module)               AS payment_module,
            current_state                    AS state_id,
            date_add                         AS ordered_at,
            date_upd                         AS updated_at,
            invoice_date                     AS invoiced_at,
            total_paid                       AS total_paid_eur,
            total_paid_real                  AS total_paid_real_eur,
            total_products                   AS total_products_eur,
            total_shipping                   AS total_shipping_eur,
            total_discounts                  AS total_discounts_eur,
            (valid = 1)                      AS is_valid,
            (gift = 1)                       AS is_gift,
            note,
            (id_customer IS NULL)            AS is_missing_customer,
            (total_paid_real IS NULL OR total_paid_real = 0) AS is_unpaid
        FROM public.presta_orders
    """, 'clean.presta_orders created')

    run(cur, "CREATE INDEX ON clean.presta_orders(source_customer_id)")
    run(cur, "CREATE INDEX ON clean.presta_orders(ordered_at)")

    # ─── 4. clean.presta_order_details ────────────────────────────────
    print('\n[clean.presta_order_details]')
    run(cur, "DROP TABLE IF EXISTS clean.presta_order_details CASCADE")
    run(cur, """
        CREATE TABLE clean.presta_order_details AS
        SELECT
            gen_random_uuid()                AS row_id_raw,
            'prestashop'::text               AS source_name,
            'public.presta_order_details'::text AS source_table,
            NOW()                            AS imported_at,
            id                               AS source_id,
            id_order                         AS source_order_id,
            product_id                       AS source_product_id,
            clean_text(product_name)         AS product_name,
            NULLIF(BTRIM(product_ean13),'')  AS ean13,
            NULLIF(BTRIM(product_reference),'') AS sku,
            product_quantity                 AS qty_ordered,
            product_quantity_return          AS qty_returned,
            product_quantity_refunded        AS qty_refunded,
            unit_price_tax_incl              AS unit_price_ttc,
            unit_price_tax_excl              AS unit_price_ht,
            total_price_tax_incl             AS total_ttc,
            total_price_tax_excl             AS total_ht,
            reduction_percent                AS reduction_pct,
            reduction_amount                 AS reduction_amount,
            (product_ean13 IS NULL OR BTRIM(product_ean13) = '') AS is_missing_ean
        FROM public.presta_order_details
    """, 'clean.presta_order_details created')

    run(cur, "CREATE INDEX ON clean.presta_order_details(source_order_id)")
    run(cur, "CREATE INDEX ON clean.presta_order_details(ean13)")

    # ─── 5. clean.presta_products ─────────────────────────────────────
    print('\n[clean.presta_products]')
    run(cur, "DROP TABLE IF EXISTS clean.presta_products CASCADE")
    run(cur, """
        CREATE TABLE clean.presta_products AS
        SELECT
            gen_random_uuid()                AS row_id_raw,
            'prestashop'::text               AS source_name,
            'public.presta_products'::text   AS source_table,
            NOW()                            AS imported_at,
            id                               AS source_id,
            NULLIF(BTRIM(reference),'')      AS sku,
            NULLIF(BTRIM(ean13),'')          AS ean13,
            NULLIF(BTRIM(isbn),'')           AS isbn,
            clean_text(name)                 AS name,
            description,
            description_short                AS description_short,
            price                            AS price_ttc_eur,
            wholesale_price                  AS wholesale_price_eur,
            (active = 1)                     AS is_active,
            quantity                         AS stock_qty,
            date_add                         AS created_at,
            date_upd                         AS updated_at
        FROM public.presta_products
    """, 'clean.presta_products created')

    run(cur, "CREATE INDEX ON clean.presta_products(ean13)")
    run(cur, "CREATE INDEX ON clean.presta_products(sku)")

    # ─── 6. clean.ml_subscribers ──────────────────────────────────────
    print('\n[clean.ml_subscribers]')
    run(cur, "DROP TABLE IF EXISTS clean.ml_subscribers CASCADE")
    run(cur, """
        CREATE TABLE clean.ml_subscribers AS
        SELECT
            gen_random_uuid()                AS row_id_raw,
            'mailerlite'::text               AS source_name,
            'public.ml_subscribers'::text    AS source_table,
            NOW()                            AS imported_at,
            id                               AS source_id,
            clean_email(email)               AS email,
            is_valid_email(clean_email(email)) AS is_valid_email,
            clean_text(name)                 AS name,
            status,
            UPPER(NULLIF(BTRIM(country),'')) AS country_code,
            clean_text(city)                 AS city,
            language,
            signup_ip                        AS signup_ip,
            date_subscribe                   AS subscribed_at,
            date_unsubscribe                 AS unsubscribed_at,
            fields                           AS custom_fields,
            (status = 'active')              AS is_active,
            (status = 'unsubscribed')        AS is_unsubscribed,
            (status = 'bounced')             AS is_bounced,
            (email IS NULL OR BTRIM(email)='') AS is_missing_email
        FROM public.ml_subscribers
    """, 'clean.ml_subscribers created')

    run(cur, "ALTER TABLE clean.ml_subscribers ADD COLUMN is_duplicate BOOLEAN DEFAULT FALSE, ADD COLUMN duplicate_group_id TEXT")
    run(cur, """
        UPDATE clean.ml_subscribers c
        SET is_duplicate = TRUE, duplicate_group_id = md5(c.email)
        FROM (SELECT email FROM clean.ml_subscribers WHERE email IS NOT NULL AND is_valid_email
              GROUP BY email HAVING COUNT(*) > 1) dup
        WHERE c.email = dup.email
    """, 'duplicates flagged')

    run(cur, "CREATE INDEX ON clean.ml_subscribers(email)")
    run(cur, "CREATE INDEX ON clean.ml_subscribers(country_code)")
    run(cur, "CREATE INDEX ON clean.ml_subscribers(status)")

    # ─── 7. clean.reviews ────────────────────────────────────────────
    print('\n[clean.reviews]')
    run(cur, "DROP TABLE IF EXISTS clean.reviews CASCADE")
    run(cur, """
        CREATE TABLE clean.reviews AS
        SELECT
            gen_random_uuid()                AS row_id_raw,
            'reviews_apps'::text             AS source_name,
            'public.reviews'::text           AS source_table,
            NOW()                            AS imported_at,
            id                               AS source_id,
            clean_text(app)                  AS app,
            store,
            UPPER(NULLIF(BTRIM(country),'')) AS country_code,
            rating,
            clean_text(title)                AS title,
            content,
            clean_text(version)              AS version,
            clean_text(author)               AS author,
            review_date                      AS reviewed_at,
            thumbs_up                        AS thumbs_up_count,
            reply_content                    AS developer_reply
        FROM public.reviews
    """, 'clean.reviews created')

    run(cur, "CREATE INDEX ON clean.reviews(app)")
    run(cur, "CREATE INDEX ON clean.reviews(rating)")
    run(cur, "CREATE INDEX ON clean.reviews(reviewed_at)")

    # ─── Stats ───────────────────────────────────────────────────────
    print('\n=== STATS ===')
    cur.execute("""
        SELECT table_name,
               (SELECT COUNT(*) FROM clean.presta_customers WHERE clean.presta_customers.* IS NOT NULL)
        FROM information_schema.tables
        WHERE table_schema='clean'
        ORDER BY table_name
    """)
    for tbl, in cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='clean' ORDER BY table_name") or []:
        pass

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='clean' ORDER BY table_name")
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        cur.execute(f'SELECT COUNT(*) FROM clean."{t}"')
        n = cur.fetchone()[0]
        print(f'  clean.{t}: {n:,}')
    conn.close()


if __name__ == '__main__':
    main()

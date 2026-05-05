"""
pipeline_18_demographics.py — enrichit intelligence.features_user avec :
  - firstname_extracted   (TEXT)
  - gender                ('M'/'F'/'U')
  - culture               ('maghreb'/'europe'/'mixed'/'unknown')
  - is_oum                (BOOL)
  - is_abou               (BOOL)

Sources de prénom (hiérarchie) : presta_customers.firstname > ml_subscribers.name > email.
Logique de classification dans demographics.py.

À exécuter APRÈS pipeline_10_intelligence.py.
"""

import time
import psycopg2
from psycopg2.extras import execute_values

from demographics import (
    extract_firstname,
    classify_gender,
    classify_culture,
    detect_oum_abou,
    classify_email_domain,
)

DB_URL = "postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway"


def main():
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    # 1. Ajouter colonnes
    print("[1/4] ALTER TABLE features_user ADD COLUMN ...")
    cur.execute("""
        ALTER TABLE intelligence.features_user
          ADD COLUMN IF NOT EXISTS firstname_extracted TEXT,
          ADD COLUMN IF NOT EXISTS gender              TEXT,
          ADD COLUMN IF NOT EXISTS culture             TEXT,
          ADD COLUMN IF NOT EXISTS is_oum              BOOLEAN,
          ADD COLUMN IF NOT EXISTS is_abou             BOOLEAN,
          ADD COLUMN IF NOT EXISTS is_pro_email        BOOLEAN,
          ADD COLUMN IF NOT EXISTS email_tld_country   TEXT,
          ADD COLUMN IF NOT EXISTS is_education        BOOLEAN,
          ADD COLUMN IF NOT EXISTS is_apple            BOOLEAN
    """)
    conn.commit()

    # 2. Récupérer les sources de prénom pour tous les users
    print("[2/4] Loading user sources...")
    t0 = time.time()
    cur.execute("""
        SELECT
            um.user_id_master,
            um.primary_email,
            pc_join.firstname,
            ml_join.name
        FROM gold.users_master um
        LEFT JOIN (
            SELECT DISTINCT ON (ul.user_id_master)
                   ul.user_id_master, pc.firstname
            FROM gold.user_link ul
            JOIN clean.presta_customers pc ON pc.id::text = ul.source_id
            WHERE ul.source_table = 'presta_customers'
              AND pc.firstname IS NOT NULL AND pc.firstname <> ''
            ORDER BY ul.user_id_master, pc.id DESC
        ) pc_join ON pc_join.user_id_master = um.user_id_master
        LEFT JOIN (
            SELECT DISTINCT ON (ul.user_id_master)
                   ul.user_id_master, s.name
            FROM gold.user_link ul
            JOIN public.ml_subscribers s ON s.id::text = ul.source_id
            WHERE ul.source_table = 'ml_subscribers'
              AND s.name IS NOT NULL AND s.name <> ''
            ORDER BY ul.user_id_master, s.id DESC
        ) ml_join ON ml_join.user_id_master = um.user_id_master
    """)
    rows = cur.fetchall()
    print(f"  → {len(rows):,} users loaded in {time.time()-t0:.1f}s")

    # 3. Calculer les features en mémoire
    print("[3/4] Classifying...")
    t0 = time.time()
    enriched = []
    counts = {"M": 0, "F": 0, "U": 0,
              "maghreb": 0, "europe": 0, "mixed": 0, "unknown": 0,
              "oum": 0, "abou": 0, "pro": 0, "personal": 0, "edu": 0, "apple": 0}
    for uid, email, shop_first, ml_name in rows:
        first = extract_firstname(shop_first or "", ml_name or "", email or "")
        gender = classify_gender(first)
        culture = classify_culture(first)
        is_oum, is_abou = detect_oum_abou(email or "", shop_first or "", ml_name or "")
        ed = classify_email_domain(email or "")
        enriched.append((
            uid, first, gender, culture, is_oum, is_abou,
            ed["is_pro_email"], ed["email_tld_country"], ed["is_education"], ed["is_apple"],
        ))
        counts[gender] += 1
        counts[culture] += 1
        if is_oum:  counts["oum"]  += 1
        if is_abou: counts["abou"] += 1
        if ed["is_pro_email"] is True:  counts["pro"]      += 1
        elif ed["is_pro_email"] is False: counts["personal"] += 1
        if ed["is_education"]:  counts["edu"]   += 1
        if ed["is_apple"]:      counts["apple"] += 1
    print(f"  → done in {time.time()-t0:.1f}s")
    print(f"     gender   M={counts['M']:>7,}  F={counts['F']:>7,}  U={counts['U']:>7,}")
    print(f"     culture  maghreb={counts['maghreb']:>6,}  europe={counts['europe']:>6,}  "
          f"mixed={counts['mixed']:>6,}  unknown={counts['unknown']:>6,}")
    print(f"     prefix   oum={counts['oum']:>5,}  abou={counts['abou']:>5,}")
    print(f"     email    pro={counts['pro']:>7,}  personal={counts['personal']:>7,}  "
          f"edu={counts['edu']:>5,}  apple={counts['apple']:>6,}")

    # 4. UPDATE par batches
    print("[4/4] Bulk UPDATE features_user...")
    t0 = time.time()
    BATCH = 5000
    total = 0
    for i in range(0, len(enriched), BATCH):
        chunk = enriched[i:i+BATCH]
        execute_values(cur, """
            UPDATE intelligence.features_user AS fu
            SET firstname_extracted = v.first,
                gender              = v.gender,
                culture             = v.culture,
                is_oum              = v.is_oum,
                is_abou             = v.is_abou,
                is_pro_email        = v.is_pro_email,
                email_tld_country   = v.email_tld_country,
                is_education        = v.is_education,
                is_apple            = v.is_apple
            FROM (VALUES %s) AS v(uid, first, gender, culture, is_oum, is_abou,
                                   is_pro_email, email_tld_country, is_education, is_apple)
            WHERE fu.user_id_master = v.uid
        """, chunk)
        conn.commit()
        total += cur.rowcount
        if (i // BATCH) % 10 == 0:
            print(f"  {i+len(chunk):>7,}/{len(enriched):,} processed")
    print(f"  → {total:,} rows updated in {time.time()-t0:.1f}s")

    # Indexes
    print("[+] CREATE INDEX...")
    for col in ("gender", "culture", "is_oum", "is_abou",
                "is_pro_email", "email_tld_country", "is_education", "is_apple"):
        try:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_features_user_{col} ON intelligence.features_user({col})")
            conn.commit()
        except Exception as e:
            print(f"  ✗ idx {col}: {e}")
            conn.rollback()

    print("\nDONE.")
    conn.close()


if __name__ == "__main__":
    main()

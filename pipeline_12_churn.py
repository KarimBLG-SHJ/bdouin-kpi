#!/usr/bin/env python3
"""
pipeline_12_churn.py — Churn risk score sur TOUS les users

Calcule un score 0-1 pour chaque user (325k).
PAS de "top N figé" — c'est toi qui filtres dynamiquement.

Score basé sur :
  - Recency : combien de temps depuis la dernière activité (commande OU email)
  - Frequency decay : a-t-il ralenti son rythme d'achat ?
  - Engagement ML : est-il encore actif sur les emails ?
  - Lifecycle stage : ancien client vs nouveau ?
  - Activity gap : écart vs sa fréquence habituelle ?

Output :
  intelligence.features_user enrichie avec churn_risk_score (0..1)
                                       + churn_factors (jsonb explicatif)
                                       + last_activity_at (date dernier signal)

Pour interroger :

  -- Tous users à risque > 0.7 dans un segment
  SELECT * FROM intelligence.features_user
  WHERE churn_risk_score > 0.7 AND segment IN ('regular','loyal_customer')
  ORDER BY churn_risk_score DESC;

  -- Distribution
  SELECT WIDTH_BUCKET(churn_risk_score, 0, 1, 10) AS bucket,
         COUNT(*) FROM intelligence.features_user GROUP BY 1 ORDER BY 1;
"""

import psycopg2

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print('=== Adding churn columns to intelligence.features_user ===\n')
    cur.execute("""
        ALTER TABLE intelligence.features_user
          ADD COLUMN IF NOT EXISTS churn_risk_score    NUMERIC(4,3),
          ADD COLUMN IF NOT EXISTS churn_factors       JSONB,
          ADD COLUMN IF NOT EXISTS last_activity_at    TIMESTAMP,
          ADD COLUMN IF NOT EXISTS days_since_activity INTEGER,
          ADD COLUMN IF NOT EXISTS expected_interval_days INTEGER,
          ADD COLUMN IF NOT EXISTS activity_gap_ratio  NUMERIC(6,2);
    """)
    conn.commit()
    print('  ✓ Columns added\n')

    print('=== Computing churn risk for all users ===\n')

    # Step 1 : compute last_activity_at (orders OU subscribe events)
    cur.execute("""
        UPDATE intelligence.features_user fu
        SET last_activity_at = sub.last_activity,
            days_since_activity = EXTRACT(DAY FROM NOW() - sub.last_activity)::int
        FROM (
            SELECT
                user_id_master,
                GREATEST(
                    MAX(event_time) FILTER (WHERE event_type IN ('order_placed','order_paid')),
                    MAX(event_time) FILTER (WHERE event_type = 'email_present')
                ) AS last_activity
            FROM gold.user_journey
            WHERE user_id_master IS NOT NULL
            GROUP BY user_id_master
        ) sub
        WHERE fu.user_id_master = sub.user_id_master
    """)
    print(f'  ✓ last_activity_at computed ({cur.rowcount:,} users)')
    conn.commit()

    # Step 2 : expected interval (avg days between orders)
    cur.execute("""
        UPDATE intelligence.features_user fu
        SET expected_interval_days = sub.avg_interval
        FROM (
            SELECT
                user_id_master,
                CASE
                    WHEN COUNT(*) >= 2 THEN
                        EXTRACT(DAY FROM (MAX(ordered_at) - MIN(ordered_at)) / NULLIF(COUNT(*) - 1, 0))::int
                    ELSE NULL
                END AS avg_interval
            FROM gold.orders
            WHERE user_id_master IS NOT NULL AND is_valid AND NOT is_unpaid
            GROUP BY user_id_master
        ) sub
        WHERE fu.user_id_master = sub.user_id_master
    """)
    print(f'  ✓ expected_interval_days computed ({cur.rowcount:,} users)')
    conn.commit()

    # Step 3 : activity gap ratio (jours since last / interval moyen)
    cur.execute("""
        UPDATE intelligence.features_user
        SET activity_gap_ratio = CASE
            WHEN expected_interval_days > 0 AND days_since_activity IS NOT NULL
            THEN ROUND((days_since_activity::numeric / expected_interval_days), 2)
            ELSE NULL
        END
    """)
    print(f'  ✓ activity_gap_ratio computed')
    conn.commit()

    # Step 4 : churn_risk_score 0..1 — combine multiple signals
    cur.execute("""
        UPDATE intelligence.features_user
        SET churn_risk_score = LEAST(1.0, GREATEST(0.0, (
            -- A. Recency (40% du score)
            CASE
                WHEN days_since_activity IS NULL              THEN 0.20  -- inconnu = baseline
                WHEN days_since_activity > 730                THEN 0.40  -- 2+ ans
                WHEN days_since_activity > 365                THEN 0.30
                WHEN days_since_activity > 180                THEN 0.20
                WHEN days_since_activity > 90                 THEN 0.10
                ELSE 0.0
            END

            -- B. Frequency decay (30% du score) — gap vs leur rythme habituel
            + CASE
                WHEN activity_gap_ratio IS NULL              THEN 0.05  -- 1 seule cmd
                WHEN activity_gap_ratio > 5                  THEN 0.30
                WHEN activity_gap_ratio > 3                  THEN 0.20
                WHEN activity_gap_ratio > 2                  THEN 0.10
                WHEN activity_gap_ratio > 1.5                THEN 0.05
                ELSE 0.0
            END

            -- C. ML status (15% du score)
            + CASE
                WHEN ml_unsubscribed                          THEN 0.15
                WHEN ml_bounced                               THEN 0.10
                WHEN ml_active                                THEN 0.0
                ELSE 0.05
            END

            -- D. Lifecycle (15% du score) — un user à 1 cmd ancien est plus risqué qu'un loyal
            + CASE
                WHEN frequency = 1 AND days_since_activity > 365 THEN 0.15
                WHEN frequency = 0 AND ml_active                  THEN 0.05  -- never bought, fan
                WHEN frequency = 0                                THEN 0.10
                WHEN frequency >= 5                               THEN 0.0  -- protected
                ELSE 0.05
            END
        )))
    """)
    print(f'  ✓ churn_risk_score computed ({cur.rowcount:,} users)')
    conn.commit()

    # Step 5 : churn_factors (JSONB explicatif)
    cur.execute("""
        UPDATE intelligence.features_user
        SET churn_factors = jsonb_build_object(
            'days_since_activity', days_since_activity,
            'expected_interval_days', expected_interval_days,
            'activity_gap_ratio', activity_gap_ratio,
            'ml_status', CASE
                WHEN ml_unsubscribed THEN 'unsubscribed'
                WHEN ml_bounced      THEN 'bounced'
                WHEN ml_active       THEN 'active'
                ELSE 'unknown'
            END,
            'lifecycle', CASE
                WHEN frequency = 0 THEN 'never_purchased'
                WHEN frequency = 1 THEN 'one_time_buyer'
                WHEN frequency BETWEEN 2 AND 4 THEN 'occasional'
                ELSE 'loyal'
            END,
            'segment', segment,
            'monetary', monetary
        )
    """)
    conn.commit()
    print('  ✓ churn_factors computed\n')

    # ─── Stats sur la distribution ────────────────────────────────────
    print('=== Distribution churn_risk_score ===\n')
    cur.execute("""
        SELECT
            ROUND(LOWER(score_range)::numeric, 1) AS lo,
            ROUND(UPPER(score_range)::numeric, 1) AS hi,
            COUNT(*)                              AS n
        FROM intelligence.features_user fu, LATERAL (
            SELECT NUMRANGE(
                FLOOR(fu.churn_risk_score * 10) / 10,
                FLOOR(fu.churn_risk_score * 10) / 10 + 0.1
            ) AS score_range
        ) sub
        WHERE fu.churn_risk_score IS NOT NULL
        GROUP BY score_range ORDER BY 1
    """) if False else cur.execute("""
        SELECT
            CASE
                WHEN churn_risk_score < 0.1 THEN '0.0-0.1'
                WHEN churn_risk_score < 0.2 THEN '0.1-0.2'
                WHEN churn_risk_score < 0.3 THEN '0.2-0.3'
                WHEN churn_risk_score < 0.4 THEN '0.3-0.4'
                WHEN churn_risk_score < 0.5 THEN '0.4-0.5'
                WHEN churn_risk_score < 0.6 THEN '0.5-0.6'
                WHEN churn_risk_score < 0.7 THEN '0.6-0.7'
                WHEN churn_risk_score < 0.8 THEN '0.7-0.8'
                WHEN churn_risk_score < 0.9 THEN '0.8-0.9'
                ELSE                              '0.9-1.0'
            END                                AS bucket,
            COUNT(*)                            AS n
        FROM intelligence.features_user
        WHERE churn_risk_score IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)

    for r in cur.fetchall():
        bar = '█' * int(r[1] / 5000)
        print(f'  {r[0]}  {r[1]:>8,}  {bar}')

    # Top zones de risque par segment
    print('\n=== Churn par segment (avg score) ===\n')
    cur.execute("""
        SELECT segment,
               COUNT(*)                         AS n,
               ROUND(AVG(churn_risk_score)::numeric, 3) AS avg_score,
               COUNT(*) FILTER (WHERE churn_risk_score > 0.7) AS high_risk
        FROM intelligence.features_user
        WHERE segment IS NOT NULL
        GROUP BY segment ORDER BY avg_score DESC
    """)
    print(f'  {"Segment":25s} {"N":>8s} {"Avg":>6s} {"High>0.7":>10s}')
    for r in cur.fetchall():
        print(f'  {r[0]:25s} {r[1]:>8,} {r[2]:>6} {r[3]:>10,}')

    # Stats clés
    print('\n=== Stats globales ===\n')
    cur.execute("""
        SELECT
            COUNT(*)                                     AS total,
            COUNT(*) FILTER (WHERE churn_risk_score > 0.7) AS high_risk,
            COUNT(*) FILTER (WHERE churn_risk_score BETWEEN 0.4 AND 0.7) AS medium_risk,
            COUNT(*) FILTER (WHERE churn_risk_score < 0.4) AS low_risk,
            COUNT(*) FILTER (WHERE last_activity_at IS NULL) AS no_activity_data
        FROM intelligence.features_user
        WHERE churn_risk_score IS NOT NULL
    """)
    r = cur.fetchone()
    print(f'  Total scored:   {r[0]:,}')
    print(f'  ⚠️  High risk (>0.7):    {r[1]:,}  ({100*r[1]/r[0]:.1f}%)')
    print(f'  ⚠ Medium (0.4-0.7):     {r[2]:,}  ({100*r[2]/r[0]:.1f}%)')
    print(f'  ✓ Low risk (<0.4):       {r[3]:,}  ({100*r[3]/r[0]:.1f}%)')

    print('\n📋 Exemples de requêtes dynamiques :\n')
    print("""
  -- Top 100 users à risque qui ont déjà acheté
  SELECT primary_email, segment, monetary, days_since_activity, churn_risk_score
  FROM intelligence.features_user
  WHERE churn_risk_score > 0.7 AND frequency >= 1
  ORDER BY monetary DESC LIMIT 100;

  -- Users avec gap d'activité 3x leur normal
  SELECT primary_email, expected_interval_days, days_since_activity, activity_gap_ratio
  FROM intelligence.features_user
  WHERE activity_gap_ratio > 3
  ORDER BY monetary DESC;

  -- Champions à protéger (haute valeur, score risque montant)
  SELECT primary_email, monetary, frequency, churn_risk_score
  FROM intelligence.features_user
  WHERE segment='champion' AND churn_risk_score > 0.3
  ORDER BY monetary DESC;
""")

    conn.close()


if __name__ == '__main__':
    main()

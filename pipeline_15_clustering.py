#!/usr/bin/env python3
"""
pipeline_15_clustering.py — Customer clustering ML

Groupe les users en clusters comportementaux réels (pas RFM segmenté à la main).

Méthode :
  - K-Means sur features standardisées
  - K déterminé par elbow method (sur sous-échantillon)
  - Features : RFM + churn + ML engagement + lifetime
  - Filtrage : on cluster seulement les users avec activité (au moins 1 commande OU ML active)

Tables créées :
  intelligence.user_clusters   — user_id_master → cluster_id + cluster_label + features
  intelligence.cluster_summary — caractéristiques moyennes par cluster
"""

import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

DB_URL = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'

# Features utilisées pour clusteriser
FEATURES = [
    'monetary',
    'frequency',
    'recency_days',
    'group_count',
    'churn_risk_score',
    'lifetime_days',
    'rfm_recency',
    'rfm_frequency',
    'rfm_monetary',
]


def main():
    conn = psycopg2.connect(DB_URL)

    # ─── Charger les données ──────────────────────────────────────────
    print('Loading users...')
    df = pd.read_sql("""
        SELECT
            user_id_master,
            primary_email,
            country_code,
            in_prestashop, in_mailerlite,
            ml_active, ml_unsubscribed, ml_bounced,
            COALESCE(monetary, 0)            AS monetary,
            COALESCE(frequency, 0)           AS frequency,
            COALESCE(recency_days, 999)::float AS recency_days,
            COALESCE(group_count, 0)         AS group_count,
            COALESCE(churn_risk_score, 0)::float AS churn_risk_score,
            COALESCE(lifetime_days, 0)::float AS lifetime_days,
            rfm_recency, rfm_frequency, rfm_monetary,
            segment
        FROM intelligence.features_user
        WHERE frequency >= 1 OR ml_active = true
    """, conn)
    print(f'  {len(df):,} users to cluster (filtered: must have purchase or active ML)')

    if len(df) < 100:
        print('Not enough data')
        return

    # ─── Standardize ──────────────────────────────────────────────────
    X = df[FEATURES].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ─── Find optimal k via elbow + silhouette (sample 5000 for speed) ──
    print('\nFinding optimal k...')
    sample_size = min(5000, len(X_scaled))
    np.random.seed(42)
    idx = np.random.choice(len(X_scaled), sample_size, replace=False)
    X_sample = X_scaled[idx]

    inertias = []
    silhouettes = []
    k_range = range(3, 11)
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_sample)
        inertias.append(km.inertia_)
        silhouettes.append(silhouette_score(X_sample, labels))
        print(f'  k={k}: inertia={km.inertia_:.0f}, silhouette={silhouettes[-1]:.3f}')

    # Pick k with best silhouette (or fallback k=7 for interpretability)
    best_k = list(k_range)[int(np.argmax(silhouettes))]
    if best_k < 5: best_k = 7  # safety floor for business interpretability
    print(f'\n→ Best k by silhouette: {best_k}')

    # ─── Final clustering ─────────────────────────────────────────────
    print(f'\nClustering all {len(X_scaled):,} users with k={best_k}...')
    km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    df['cluster_id'] = km.fit_predict(X_scaled)

    # ─── Label clusters automatiquement ───────────────────────────────
    cluster_stats = df.groupby('cluster_id').agg({
        'monetary': 'mean',
        'frequency': 'mean',
        'recency_days': 'mean',
        'churn_risk_score': 'mean',
        'group_count': 'mean',
        'user_id_master': 'count',
    }).rename(columns={'user_id_master': 'size'}).round(2)

    # Auto-labels based on profile
    def label_cluster(row):
        if row['monetary'] > 200 and row['recency_days'] < 90:
            return 'champion'
        if row['monetary'] > 100 and row['frequency'] > 2:
            return 'loyal_buyer'
        if row['frequency'] >= 1 and row['recency_days'] < 90:
            return 'recent_buyer'
        if row['frequency'] >= 1 and row['recency_days'] > 365:
            return 'churned_buyer'
        if row['frequency'] == 0 and row['group_count'] > 3:
            return 'engaged_fan'
        if row['frequency'] == 0 and row['group_count'] >= 1:
            return 'casual_fan'
        if row['churn_risk_score'] > 0.5:
            return 'at_risk'
        return 'standard'

    cluster_stats['label'] = cluster_stats.apply(label_cluster, axis=1)

    # Make labels unique by appending number if duplicate
    seen = {}
    final_labels = {}
    for cid, lbl in cluster_stats['label'].items():
        if lbl in seen:
            seen[lbl] += 1
            final_labels[cid] = f'{lbl}_{seen[lbl]}'
        else:
            seen[lbl] = 1
            final_labels[cid] = lbl
    cluster_stats['label'] = pd.Series(final_labels)
    df['cluster_label'] = df['cluster_id'].map(final_labels)

    print('\nCluster summary :')
    print(cluster_stats.to_string())

    # ─── Save to DB ───────────────────────────────────────────────────
    print('\nSaving to intelligence.user_clusters...')
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS intelligence.user_clusters CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.user_clusters (
            user_id_master  TEXT PRIMARY KEY,
            cluster_id      INTEGER,
            cluster_label   TEXT,
            assigned_at     TIMESTAMP DEFAULT NOW()
        )
    """)
    rows = [(r.user_id_master, int(r.cluster_id), r.cluster_label) for r in df.itertuples()]
    BATCH = 10000
    for i in range(0, len(rows), BATCH):
        psycopg2.extras.execute_values(cur, """
            INSERT INTO intelligence.user_clusters (user_id_master, cluster_id, cluster_label)
            VALUES %s ON CONFLICT (user_id_master) DO UPDATE SET
              cluster_id    = EXCLUDED.cluster_id,
              cluster_label = EXCLUDED.cluster_label,
              assigned_at   = NOW()
        """, rows[i:i+BATCH])
        conn.commit()
        print(f'  ... {min(i+BATCH, len(rows)):,}/{len(rows):,}')
    cur.execute("CREATE INDEX ON intelligence.user_clusters(cluster_label)")
    cur.execute("CREATE INDEX ON intelligence.user_clusters(cluster_id)")
    conn.commit()
    print(f'  ✓ {len(rows):,} users tagged')

    # ─── Cluster summary table ────────────────────────────────────────
    print('\nSaving cluster_summary...')
    cur.execute("DROP TABLE IF EXISTS intelligence.cluster_summary CASCADE")
    cur.execute("""
        CREATE TABLE intelligence.cluster_summary (
            cluster_id      INTEGER PRIMARY KEY,
            cluster_label   TEXT,
            size            INTEGER,
            avg_monetary    NUMERIC(10,2),
            avg_frequency   NUMERIC(6,2),
            avg_recency_days NUMERIC(6,1),
            avg_churn_risk  NUMERIC(4,3),
            avg_group_count NUMERIC(5,2),
            top_country     TEXT,
            computed_at     TIMESTAMP DEFAULT NOW()
        )
    """)

    # Compute top country per cluster
    top_country_per_cluster = (df.dropna(subset=['country_code'])
                                .groupby(['cluster_id','country_code'])
                                .size().reset_index(name='n')
                                .sort_values(['cluster_id','n'], ascending=[True,False])
                                .drop_duplicates('cluster_id')
                                .set_index('cluster_id')['country_code']
                                .to_dict())

    summary_rows = []
    for cid, row in cluster_stats.iterrows():
        summary_rows.append((
            int(cid),
            row['label'],
            int(row['size']),
            float(row['monetary']),
            float(row['frequency']),
            float(row['recency_days']),
            float(row['churn_risk_score']),
            float(row['group_count']),
            top_country_per_cluster.get(cid, None),
        ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO intelligence.cluster_summary
          (cluster_id, cluster_label, size, avg_monetary, avg_frequency,
           avg_recency_days, avg_churn_risk, avg_group_count, top_country)
        VALUES %s
    """, summary_rows)
    conn.commit()

    # ─── Show final summary ───────────────────────────────────────────
    print('\n=== FINAL CLUSTERS ===\n')
    cur.execute("""
        SELECT cluster_id, cluster_label, size,
               avg_monetary, avg_frequency, avg_recency_days, avg_churn_risk, top_country
        FROM intelligence.cluster_summary
        ORDER BY size DESC
    """)
    print(f"  {'#':>2}  {'Label':18s} {'Size':>7s} {'€avg':>7s} {'F.avg':>5s} {'Rec.j':>6s} {'Churn':>5s} {'Pays':<5s}")
    for r in cur.fetchall():
        print(f'  {r[0]:>2}  {r[1]:18s} {r[2]:>7,} {r[3]:>7.0f} {r[4]:>5.2f} {r[5]:>6.0f} {r[6]:>5.2f} {r[7] or "-":<5}')

    print('\n📋 Requêtes possibles :\n')
    print("""
  -- Tous les users d'un cluster
  SELECT u.primary_email, u.monetary, u.frequency
  FROM intelligence.user_clusters c
  JOIN intelligence.features_user u USING (user_id_master)
  WHERE c.cluster_label = 'champion';

  -- Distribution clusters par pays
  SELECT c.cluster_label, u.country_code, COUNT(*)
  FROM intelligence.user_clusters c
  JOIN intelligence.features_user u USING (user_id_master)
  GROUP BY c.cluster_label, u.country_code
  ORDER BY 3 DESC;
""")

    conn.close()


if __name__ == '__main__':
    main()

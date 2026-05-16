#!/usr/bin/env python3
"""
parse_zoho_print_bills.py — Parse les 29 bills Hoo-Pow (IMAK + Sofiadis)
en lignes exploitables (timeline prints + frais logistique).

Source : zoho_dump/bills/*.json
Sortie : flask-app/out/print_lines.csv (inspection avant push Postgres)

Classification line_kind :
  - 'logistics' : Sofiadis frais mensuels
  - 'print_lump' : IMAK facture sans détail ("Print (see attached)")
  - 'print'     : IMAK ligne avec titre + qty (matchable au catalogue)
  - 'shipping'  : "Port HT" et autres frais annexes
"""

import json, os, csv, re, sys, unicodedata
import psycopg2
from psycopg2.extras import execute_values

DUMP_DIR = os.path.join(os.path.dirname(__file__), '..', 'zoho_dump', 'bills')

# Bill IMAK 2024-01-12 (€31 150) sans détail dans Zoho — détail extrait du PDF JPEG
# WhatsApp Image 2024-03-19 at 16.31.37.jpeg
LUMP_BILL_EXPANSIONS = {
    '4029527000006536135': [  # IHR 20240000000000012 — IMAK 2024-01-12
        # (description_pour_match, qty, rate, total)
        ("Awlad School — J'apprends à lire et écrire l'arabe", 4056, 1.38653, 5623.77),
        ("Awlad School — Vocabulaire",                          4082, 1.38653, 5659.42),
        ("Awlad School T1 — J'apprends à m'exprimer en arabe",  4014, 1.38653, 5565.55),
        ("Awlad School T2 — J'apprends à m'exprimer en arabe",  3588, 1.38653, 4974.48),
        ("Awlad School T3 — J'apprends à m'exprimer en arabe",  3120, 1.38653, 4325.98),
        ("Cahier Écriture Arabe — Mauve",                       2000, 0.5000, 1000.00),  # PDF "pink"
        ("Cahier Écriture Arabe — Bleu",                        2000, 0.5000, 1000.00),
        ("Cahier Écriture Arabe — Jaune",                       2000, 0.5000, 1000.00),
        ("Cahier Écriture Arabe — Orange",                      2000, 0.5000, 1000.00),
        ("Cahier Écriture Arabe — Vert",                        2000, 0.5000, 1000.00),
    ],
}

OUT_DIR  = os.path.join(os.path.dirname(__file__), 'out')
os.makedirs(OUT_DIR, exist_ok=True)

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)

def normalize(s):
    if not s: return ''
    s = unicodedata.normalize('NFD', str(s))
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'\s+', ' ', s).strip().lower()

def classify(desc, qty, item_total):
    n = normalize(desc)
    if 'logistique' in n or 'logiqtique' in n:
        return 'logistics'
    if 'port' in n and qty == 1:
        return 'shipping'
    if 'see attached' in n or n.startswith('print '):
        return 'print_lump'
    return 'print'


def extract_print_qty(desc, fallback_qty):
    """Pour les bills Sofiadis 'Travaux Impression', la vraie qty est dans la
    description ('3000 ex', '10 065 ex', '6045 Nos'). On la récupère."""
    n = normalize(desc)
    # Format : '{1-3 digits}({space|,}{3 digits})*' OU '\d+'. Prend la DERNIÈRE
    # occurrence avant 'ex/nos' pour éviter '10 5000 ex' → 105000.
    pat = re.compile(r'\b(\d{1,3}(?:[\s,]\d{3})+|\d+)\s*(?:ex|nos|n°|nbr)\b')
    matches = list(pat.finditer(n))
    if matches:
        raw = re.sub(r'[\s,]', '', matches[-1].group(1))
        try:
            return float(raw)
        except ValueError:
            pass
    return fallback_qty

def _find_by_canonical(catalog_index, canonical):
    for cid, (cn, _, _, is_pack) in catalog_index.items():
        if cn == canonical and not is_pack:
            return (cid, cn)
    return (None, None)


def _find_by_series_tome(catalog_index, series, tome):
    for cid, (cn, s, t, is_pack) in catalog_index.items():
        if s == series and t == tome and not is_pack:
            return (cid, cn)
    return (None, None)


def match_catalog(desc, catalog_index, alias_index):
    """Retourne (catalog_id, canonical_name) ou (None, None) — match best-effort."""
    n = normalize(desc)
    if not n: return (None, None)

    # 1. Match alias exact
    if n in alias_index:
        return alias_index[n]

    # 2. Walad & Binti — DOIT passer avant Famille Foulane (sinon "wld & bnt" matche rien)
    #    Patterns : "walad et binti tN", "walad binti tN", "wld & bnt tN"
    if 'manga' in n and ('walad' in n or 'wld' in n or 'muslim show' in n):
        # Walad Manga / Muslim Show Manga / WLD & BNT (MANGA) → Muslim Show WT2 Manga
        return _find_by_canonical(catalog_index, 'Muslim Show WT2 Manga')
    m = re.search(r'(?:walad\s*(?:et|&)?\s*binti|wld\s*&\s*bnt|walad\s*binti)\s*t?\s*(\d+)', n)
    if m:
        return _find_by_series_tome(catalog_index, 'Walad & Binti', int(m.group(1)))

    # 3. Walad Découvre Médine / La Mecque
    if 'walad' in n and ('medine' in n or 'medina' in n):
        return _find_by_canonical(catalog_index, 'Walad Découvre Médine')
    if 'walad' in n and ('mecque' in n or 'mekka' in n or 'mecca' in n):
        return _find_by_canonical(catalog_index, 'Walad Découvre La Mecque')

    # 4. Agence Règle Tout V{n} (description contient parfois "Tome 3" + "Vol 1" — on prend Vol)
    if 'agence' in n and 'regle' in n:
        m = re.search(r'vol\s*(\d+)', n)
        if m:
            return _find_by_series_tome(catalog_index, 'Agence Règle Tout', int(m.group(1)))
        m = re.search(r'tome\s*(\d+)', n)
        if m:
            return _find_by_series_tome(catalog_index, 'Agence Règle Tout', int(m.group(1)))

    # 5. Famille Foulane T{n} — accepte "famille foulane #N", "famille foulan tN", "famille foulane N"
    m = re.search(r'famille\s*foulan[e]?\s*(?:t|#)?\s*(\d+)', n)
    if m:
        return _find_by_series_tome(catalog_index, 'Famille Foulane', int(m.group(1)))

    # 6. Muslim Show #N / Recueil Muslim Show T{n}
    if 'muslim show' in n and 'collector' in n:
        return _find_by_canonical(catalog_index, 'Muslim Show Collector')
    m = re.search(r'(?:rec[ue]+il\s*(?:muslim\s*show)?|muslim\s*show)\s*[#t]?\s*(\d+)', n)
    if m:
        return _find_by_series_tome(catalog_index, 'Recueil Muslim Show', int(m.group(1)))

    # 7. Salat Fille / Garçon (Mini Guide Illustré aussi)
    if 'salat' in n and ('fille' in n or 'girl' in n):
        return _find_by_canonical(catalog_index, 'Guide Salat Fille')
    if 'salat' in n and ('garcon' in n or 'boy' in n):
        return _find_by_canonical(catalog_index, 'Guide Salat Garçon')

    # 8. Awlad School — lire/écrire arabe vs vocabulaire vs s'exprimer T{n}
    if 'awlad' in n:
        if 'lire' in n and 'ecrire' in n:
            return _find_by_canonical(catalog_index, "Awlad School — J'apprends à lire et écrire l'arabe")
        if 'vocabulaire' in n:
            return _find_by_canonical(catalog_index, 'Awlad School — Vocabulaire')
        m = re.search(r'awlad\s*school\s*t?\s*(\d+)', n)
        if m:
            return _find_by_series_tome(catalog_index, 'Awlad School', int(m.group(1)))

    # 9. Dialogue
    if re.search(r'\bdialogue\b', n):
        return _find_by_canonical(catalog_index, 'Dialogue')

    # 10. Guides (par mot-clé du titre)
    guide_keywords = {
        'super etudiant': 'Guide du Super Étudiant',
        'citadelle': 'Guide Citadelle du Petit Muslim',
        'bonnes actions': 'Guide Bonnes Actions',
        'mois beni': 'Guide Mois Béni de Ramadan',
        'ramadan': 'Guide Mois Béni de Ramadan',
        'hajj': 'Guide Hajj & Omra',
        'umra': 'Guide Hajj & Omra',
        'omra': 'Guide Hajj & Omra',
    }
    for kw, target in guide_keywords.items():
        if kw in n:
            res = _find_by_canonical(catalog_index, target)
            if res[0]:
                return res

    # 11. Agenda Famille Foulane
    if 'agenda' in n and 'foulane' in n:
        for cid, (cn, _, _, is_pack) in catalog_index.items():
            if 'agenda' in cn.lower() and not is_pack:
                return (cid, cn)

    return (None, None)

def expand_multi(desc, qty, rate, item_total, classify_kind):
    """
    Pour les descriptions du type "FAMILLE FOULAN 1,2,3,4,5,6,7,8" qty=16000
    ou "Receuil 1-4" qty=4000 → expanse en N lignes virtuelles.
    Retourne liste de (sub_desc, sub_qty).
    """
    if classify_kind != 'print':
        return [(desc, qty)]
    n = normalize(desc)

    # "famille foulan 1,2,3,4,5,6,7,8"
    m = re.search(r'famille foulan[e]?\s*((?:\d+\s*[,.\-]\s*)*\d+)', n)
    if m:
        nums_str = m.group(1)
        nums = [int(x) for x in re.findall(r'\d+', nums_str)]
        if len(nums) > 1:
            per = qty / len(nums)
            return [(f'Famille Foulane T{t}', per) for t in nums]

    # "receuil 1-4" → ambigu : "T1, qty=4" OU "T1-T4 mélangés" ?
    # Vu que rate identique pour "Receuil 1-4" et "Receuil 2-3" et qty=4000 each,
    # interprétation : le "-X" est juste un n° interne, pas une plage de tomes.
    # → On garde tel quel et on log non-matché si pertinent.
    m = re.search(r'rec[ue]+il\s+(\d+)\s*-\s*(\d+)', n)
    if m:
        # Si le 2e nombre est petit (1-9) on suppose c'est tome unique avec un suffixe qui ne représente pas les tomes
        # Garde tel quel — laisse le matcher essayer "rec[ue]+il\s*\d+" qui prendra le premier nombre
        return [(desc, qty)]

    return [(desc, qty)]


def run():
    # ── Charger catalogue ────────────────────────────────────────────────
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    # is_pack=False prioritaire (unitaire) — on impose l'ordre dans le dict
    cur.execute("SELECT catalog_id, canonical_name, series, tome_number, is_pack FROM gold.catalog ORDER BY is_pack ASC, catalog_id")
    catalog_index = {cid: (cn, s, t, p) for cid, cn, s, t, p in cur.fetchall()}
    cur.execute("""
        SELECT a.alias_name, a.catalog_id, c.canonical_name
        FROM gold.catalog_aliases a JOIN gold.catalog c USING (catalog_id)
    """)
    alias_index = {normalize(a): (cid, cn) for a, cid, cn in cur.fetchall()}
    conn.close()
    print(f'Catalog : {len(catalog_index)} titres, {len(alias_index)} aliases')

    # ── Parser les bills ────────────────────────────────────────────────
    rows = []
    files = sorted(f for f in os.listdir(DUMP_DIR) if f.endswith('.json'))
    for f in files:
        b = json.load(open(os.path.join(DUMP_DIR, f)))
        bill_id   = b.get('bill_id')
        bill_num  = b.get('bill_number')
        bill_date = b.get('date')
        vendor    = b.get('vendor_name','')
        currency  = b.get('currency_code')
        # Expansion lump (PDF JPEG) → on remplace les line_items par le détail
        if bill_id in LUMP_BILL_EXPANSIONS:
            virtual_lines = []
            for sub_desc, sub_qty, sub_rate, sub_total in LUMP_BILL_EXPANSIONS[bill_id]:
                virtual_lines.append({
                    'description': sub_desc,
                    'quantity': sub_qty,
                    'rate': sub_rate,
                    'item_total': sub_total,
                })
            line_items = virtual_lines
        else:
            line_items = b.get('line_items', [])

        for li in line_items:
            desc  = li.get('description','').strip()
            qty   = float(li.get('quantity', 0) or 0)
            rate  = float(li.get('rate', 0) or 0)
            total = float(li.get('item_total', 0) or 0)
            kind  = classify(desc, qty, total)

            # Sofiadis 'Travaux Impression' et variantes : qty=1 avec vraie qty
            # dans la description ('3000 ex', '6045 Nos', '10 024 ex')
            if kind == 'print' and qty <= 1:
                real_qty = extract_print_qty(desc, qty)
                if real_qty and real_qty > 1:
                    qty = real_qty
                    rate = total / real_qty if real_qty else rate

            # Expand multi-tome descriptions
            sub_lines = expand_multi(desc, qty, rate, total, kind)
            n_sub = len(sub_lines)
            for sub_desc, sub_qty in sub_lines:
                cid, cname = match_catalog(sub_desc, catalog_index, alias_index)
                rows.append({
                    'bill_id': bill_id,
                    'bill_number': bill_num,
                    'bill_date': bill_date,
                    'vendor': vendor,
                    'currency': currency,
                    'line_kind': kind,
                    'description_raw': desc,
                    'description_sub': sub_desc if n_sub > 1 else '',
                    'quantity': sub_qty,
                    'rate': rate,
                    'item_total': total / n_sub if n_sub > 1 else total,
                    'catalog_id': cid or '',
                    'matched_canonical': cname or '',
                })

    # ── Écrire CSV ──────────────────────────────────────────────────────
    out_path = os.path.join(OUT_DIR, 'print_lines.csv')
    fields = ['bill_date','vendor','bill_number','currency','line_kind',
              'quantity','rate','item_total','description_raw','description_sub',
              'catalog_id','matched_canonical','bill_id']
    with open(out_path, 'w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x['bill_date'], x['bill_number'])):
            w.writerow({k: r[k] for k in fields})
    print(f'✅ Écrit {len(rows)} lignes → {out_path}')

    # ── Stats ───────────────────────────────────────────────────────────
    from collections import Counter
    kind_counts = Counter(r['line_kind'] for r in rows)
    matched = sum(1 for r in rows if r['catalog_id'] and r['line_kind']=='print')
    total_print = sum(1 for r in rows if r['line_kind']=='print')
    print()
    print('=== Stats ===')
    for k, v in kind_counts.most_common():
        print(f'  {k:<14} : {v}')
    print(f'  print matchés au catalogue : {matched}/{total_print}')

    # ── Lignes print non matchées (à inspecter) ─────────────────────────
    print()
    print('=== Print lines NON matchées ===')
    for r in rows:
        if r['line_kind']=='print' and not r['catalog_id']:
            sd = r['description_sub'] or r['description_raw']
            print(f"  {r['bill_date']} | qty={r['quantity']:>6} | {sd}")

    return rows


def push_postgres(rows):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS gold.print_runs CASCADE")
    cur.execute("""
        CREATE TABLE gold.print_runs (
            run_id            SERIAL PRIMARY KEY,
            bill_id           TEXT NOT NULL,
            bill_number       TEXT,
            bill_date         DATE NOT NULL,
            vendor            TEXT,
            currency          TEXT,
            line_kind         TEXT NOT NULL,
            description_raw   TEXT,
            description_sub   TEXT,
            catalog_id        INTEGER REFERENCES gold.catalog(catalog_id),
            matched_canonical TEXT,
            quantity          NUMERIC,
            rate              NUMERIC(12,6),
            item_total        NUMERIC(12,4),
            created_at        TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX ON gold.print_runs (bill_date)")
    cur.execute("CREATE INDEX ON gold.print_runs (catalog_id)")
    cur.execute("CREATE INDEX ON gold.print_runs (line_kind)")

    payload = [(
        r['bill_id'], r['bill_number'], r['bill_date'], r['vendor'], r['currency'],
        r['line_kind'], r['description_raw'], r['description_sub'] or None,
        r['catalog_id'] or None, r['matched_canonical'] or None,
        r['quantity'], r['rate'], r['item_total'],
    ) for r in rows]

    execute_values(cur, """
        INSERT INTO gold.print_runs
          (bill_id, bill_number, bill_date, vendor, currency, line_kind,
           description_raw, description_sub, catalog_id, matched_canonical,
           quantity, rate, item_total)
        VALUES %s
    """, payload)
    conn.commit()
    print(f'✅ gold.print_runs : {len(payload)} lignes insérées')

    cur.execute("""
        SELECT line_kind, COUNT(*), SUM(item_total)
        FROM gold.print_runs GROUP BY line_kind ORDER BY 1
    """)
    print()
    print(f'{"line_kind":<14} {"rows":>6} {"total EUR":>12}')
    for k, n, t in cur.fetchall():
        print(f'  {k:<14} {n:>4}  {float(t or 0):>12.2f}')
    conn.close()


if __name__ == '__main__':
    rows = run()
    if '--push' in sys.argv:
        push_postgres(rows)

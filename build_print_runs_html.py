#!/usr/bin/env python3
"""Génère un HTML statique local pour visualiser gold.print_runs."""
import os, psycopg2, html
from collections import defaultdict

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
)
OUT = os.path.join(os.path.dirname(__file__), 'out', 'print_runs.html')

def fmt_eur(x):
    return f"€{float(x):,.2f}".replace(',', ' ')

def fmt_int(x):
    return f"{int(x):,}".replace(',', ' ')

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT bill_date, bill_number, vendor, currency, line_kind,
               description_raw, description_sub, matched_canonical,
               quantity, rate, item_total
        FROM gold.print_runs
        ORDER BY bill_date, line_kind, matched_canonical
    """)
    rows = cur.fetchall()
    conn.close()

    # Aggregations
    by_kind = defaultdict(lambda: {'n': 0, 'qty': 0, 'total': 0.0})
    by_session = defaultdict(lambda: {'titles': 0, 'qty': 0, 'total': 0.0, 'vendor': ''})
    by_title = defaultdict(lambda: {'qty': 0, 'total': 0.0, 'sessions': set()})
    by_month_log = []

    for r in rows:
        bd, bn, vendor, cur_, kind, dr, ds, mc, q, rate, tot = r
        by_kind[kind]['n'] += 1
        by_kind[kind]['qty'] += float(q or 0)
        by_kind[kind]['total'] += float(tot or 0)
        if kind == 'print':
            key = (bd, vendor)
            by_session[key]['titles'] += 1
            by_session[key]['qty'] += float(q or 0)
            by_session[key]['total'] += float(tot or 0)
            by_session[key]['vendor'] = vendor
            if mc:
                by_title[mc]['qty'] += float(q or 0)
                by_title[mc]['total'] += float(tot or 0)
                by_title[mc]['sessions'].add(str(bd))
        elif kind == 'logistics':
            by_month_log.append((bd, dr, float(tot or 0)))

    grand_total = sum(v['total'] for v in by_kind.values())

    # Build HTML
    h = []
    h.append('<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8">')
    h.append('<title>BDouin — Print Runs Zoho</title>')
    h.append('<style>')
    h.append('''
      :root { --bg:#0f1115; --card:#181c23; --border:#262b35; --text:#e6e8ec; --muted:#8c93a0; --accent:#7cc4ff; --green:#7ed99a; --orange:#ffb27c; }
      * { box-sizing:border-box; }
      body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background:var(--bg); color:var(--text); margin:0; padding:24px; line-height:1.5; }
      h1 { font-size:22px; margin:0 0 4px; }
      h2 { font-size:15px; color:var(--muted); margin:32px 0 12px; text-transform:uppercase; letter-spacing:.5px; font-weight:600; }
      .sub { color:var(--muted); font-size:13px; margin-bottom:24px; }
      .cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(180px,1fr)); gap:12px; margin-bottom:8px; }
      .card { background:var(--card); border:1px solid var(--border); border-radius:8px; padding:14px 16px; }
      .card .label { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.5px; }
      .card .value { font-size:20px; font-weight:600; margin-top:4px; }
      .card .value.eur { color:var(--accent); }
      table { width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--border); border-radius:8px; overflow:hidden; font-size:13px; }
      th { text-align:left; padding:10px 14px; background:#1f2330; color:var(--muted); font-weight:500; font-size:11px; text-transform:uppercase; letter-spacing:.5px; border-bottom:1px solid var(--border); }
      td { padding:10px 14px; border-top:1px solid var(--border); }
      td.num { text-align:right; font-variant-numeric:tabular-nums; }
      td.eur { color:var(--accent); }
      td.muted { color:var(--muted); font-size:12px; }
      .pill { display:inline-block; padding:2px 8px; border-radius:10px; font-size:11px; background:#262b35; color:var(--muted); }
      .pill.print { background:#1d3a26; color:var(--green); }
      .pill.logistics { background:#3a2c1d; color:var(--orange); }
      .pill.shipping { background:#262b35; color:var(--muted); }
      tr:hover td { background:#1d212a; }
    ''')
    h.append('</style></head><body>')

    h.append('<h1>BDouin — Print Runs (Zoho Books)</h1>')
    h.append(f'<div class="sub">Source : <code>gold.print_runs</code> ({len(rows)} lignes) — extracted from {len(set(r[1] for r in rows))} bills Hoo-Pow</div>')

    # Summary cards
    h.append('<div class="cards">')
    h.append(f'<div class="card"><div class="label">Total dépensé</div><div class="value eur">{fmt_eur(grand_total)}</div></div>')
    p = by_kind['print']
    h.append(f'<div class="card"><div class="label">Print IMAK</div><div class="value">{fmt_eur(p["total"])}</div><div class="muted" style="font-size:12px;color:var(--muted);margin-top:4px">{fmt_int(p["qty"])} exemplaires • {p["n"]} lignes</div></div>')
    l = by_kind['logistics']
    h.append(f'<div class="card"><div class="label">Logistique Sofiadis</div><div class="value">{fmt_eur(l["total"])}</div><div class="muted" style="font-size:12px;color:var(--muted);margin-top:4px">{l["n"]} mois</div></div>')
    h.append(f'<div class="card"><div class="label">Sessions print</div><div class="value">{len(by_session)}</div></div>')
    h.append(f'<div class="card"><div class="label">Titres distincts imprimés</div><div class="value">{len(by_title)}</div></div>')
    h.append('</div>')

    # Sessions print
    h.append('<h2>Sessions d\'impression IMAK</h2>')
    h.append('<table><thead><tr><th>Date</th><th>Vendor</th><th class="num">Titres</th><th class="num">Exemplaires</th><th class="num">Coût</th></tr></thead><tbody>')
    for (bd, vendor), v in sorted(by_session.items()):
        h.append(f'<tr><td>{bd}</td><td>{html.escape(vendor)}</td><td class="num">{v["titles"]}</td><td class="num">{fmt_int(v["qty"])}</td><td class="num eur">{fmt_eur(v["total"])}</td></tr>')
    h.append('</tbody></table>')

    # Détail toutes les print lines
    h.append('<h2>Détail des prints (27 lignes)</h2>')
    h.append('<table><thead><tr><th>Date</th><th>Titre</th><th class="num">Qty</th><th class="num">€/u</th><th class="num">Total</th></tr></thead><tbody>')
    for r in rows:
        bd, bn, vendor, cur_, kind, dr, ds, mc, q, rate, tot = r
        if kind != 'print': continue
        h.append(f'<tr><td>{bd}</td><td>{html.escape(mc or dr)}</td><td class="num">{fmt_int(q)}</td><td class="num muted">{float(rate):.4f}</td><td class="num eur">{fmt_eur(tot)}</td></tr>')
    h.append('</tbody></table>')

    # Totaux par titre
    h.append('<h2>Totaux par titre (cumul depuis 2023)</h2>')
    h.append('<table><thead><tr><th>Titre</th><th class="num">Sessions</th><th class="num">Exemplaires</th><th class="num">Coût total</th><th class="num muted">€/u moyen</th></tr></thead><tbody>')
    for title, v in sorted(by_title.items(), key=lambda x: -x[1]['total']):
        avg = v['total'] / v['qty'] if v['qty'] else 0
        h.append(f'<tr><td>{html.escape(title)}</td><td class="num">{len(v["sessions"])}</td><td class="num">{fmt_int(v["qty"])}</td><td class="num eur">{fmt_eur(v["total"])}</td><td class="num muted">{avg:.4f}</td></tr>')
    h.append('</tbody></table>')

    # Logistique mensuelle
    h.append('<h2>Logistique Sofiadis — par mois</h2>')
    h.append('<table><thead><tr><th>Date facture</th><th>Description</th><th class="num">Coût</th></tr></thead><tbody>')
    for bd, desc, tot in sorted(by_month_log):
        h.append(f'<tr><td>{bd}</td><td class="muted">{html.escape(desc)}</td><td class="num eur">{fmt_eur(tot)}</td></tr>')
    h.append('</tbody></table>')

    h.append('</body></html>')

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(h))
    print(f'✅ Écrit → {OUT}')


if __name__ == '__main__':
    main()

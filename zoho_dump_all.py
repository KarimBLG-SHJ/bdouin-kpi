"""
zoho_dump_all.py — Dump des factures Zoho BDouin.

- Phase 1 : dump détail de toutes les invoices ventes BDouin (whitelist clients).
- Phase 2 : liste tous les vendors (pour identifier vendors BDouin au-delà Sofiadis/IMAK).
- Phase 3 (séparée) : dump bills BDouin une fois la whitelist vendors finalisée.

Stockage : zoho_dump/invoices/{id}.json, zoho_dump/vendors_list.json
Idempotent : skip les fichiers déjà présents.

Usage :
  ZOHO_* env vars ... python3 -u zoho_dump_all.py [phase1|phase2|phase3] [--force]
"""
import json
import os
import sys
import time
from pathlib import Path

import zoho_client as zc

DUMP_DIR = Path(__file__).parent.parent / "zoho_dump"
INV_DIR = DUMP_DIR / "invoices"
BILL_DIR = DUMP_DIR / "bills"
BILL_EMP_DIR = DUMP_DIR / "bills_employees"
VENDORS_FILE = DUMP_DIR / "vendors_list.json"
RATE_LIMIT_SLEEP = 0.5

# Vendors print BDouin (déjà dans zoho_client.PRINT_VENDOR_IDS)
PRINT_VENDORS = {
    "4029527000010369013": "Sofiadis",
    "4029527000003084213": "İMAK OFSET",
}

# Vendors employés Allam/Ouaich (mis de côté — dump séparé)
EMPLOYEE_VENDORS = {
    "4029527000000322699": "Karim Allam",
    "4029527000000321871": "Noredine Allam",
    "4029527000002185424": "Kemil Allam",
    "4029527000004347361": "Rachid Allam",
    "4029527000000322411": "Anass Ouaich",
}


def dump_one(detail_fn, item_id, out_dir, force=False):
    out = out_dir / f"{item_id}.json"
    if out.exists() and not force:
        return "skip"
    try:
        data = detail_fn(item_id)
    except Exception as e:
        print(f"  ! {item_id}: {e}", flush=True)
        return "error"
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    time.sleep(RATE_LIMIT_SLEEP)
    return "ok"


def phase1_invoices(force=False):
    """Dump détail de toutes les invoices BDouin (whitelist clients)."""
    INV_DIR.mkdir(parents=True, exist_ok=True)
    print("→ Listing BDouin invoices (filter by whitelist)...", flush=True)
    invoices = zc.get_bdouin_invoices()
    print(f"  {len(invoices)} BDouin invoices total", flush=True)
    stats = {"ok": 0, "skip": 0, "error": 0}
    for i, inv in enumerate(invoices, 1):
        result = dump_one(zc.get_invoice_detail, inv["invoice_id"], INV_DIR, force)
        stats[result] += 1
        if i % 25 == 0:
            print(f"  [{i}/{len(invoices)}] ok={stats['ok']} skip={stats['skip']} err={stats['error']}", flush=True)
    print(f"✓ Invoices done: {stats}", flush=True)
    return stats


def phase2_vendors_list():
    """Liste tous les contacts vendors (pour identifier vendors BDouin)."""
    DUMP_DIR.mkdir(exist_ok=True)
    print("→ Listing all contacts (vendors)...", flush=True)
    all_contacts = []
    page = 1
    while True:
        data = zc._get("contacts", {"page": page, "per_page": 200, "contact_type": "vendor"})
        batch = data.get("contacts", [])
        all_contacts.extend(batch)
        print(f"  page {page}: {len(batch)} vendors (total {len(all_contacts)})", flush=True)
        if len(batch) < 200:
            break
        page += 1
        time.sleep(RATE_LIMIT_SLEEP)
    # Garde uniquement les champs utiles
    slim = [
        {
            "contact_id": c.get("contact_id"),
            "contact_name": c.get("contact_name"),
            "company_name": c.get("company_name"),
            "email": c.get("email"),
            "outstanding_payable_amount": c.get("outstanding_payable_amount"),
            "currency_code": c.get("currency_code"),
            "country": c.get("country"),
            "created_time": c.get("created_time"),
            "last_modified_time": c.get("last_modified_time"),
        }
        for c in all_contacts
    ]
    VENDORS_FILE.write_text(json.dumps(slim, ensure_ascii=False, indent=2))
    print(f"✓ Saved {len(slim)} vendors → {VENDORS_FILE}", flush=True)
    return slim


def phase3_bills(vendor_map, out_dir, label, force=False):
    """Dump détail des bills pour vendor_map = {vendor_id: name}."""
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"→ [{label}] Fetching bills for {len(vendor_map)} vendors...", flush=True)
    all_bills = []
    for vid, vname in vendor_map.items():
        page = 1
        while True:
            data = zc._get("bills", {"vendor_id": vid, "page": page, "per_page": 200})
            batch = data.get("bills", [])
            for b in batch:
                b["_vendor_name"] = vname
            all_bills.extend(batch)
            print(f"  {vname} page {page}: {len(batch)} bills", flush=True)
            if len(batch) < 200:
                break
            page += 1
            time.sleep(RATE_LIMIT_SLEEP)
    print(f"  [{label}] {len(all_bills)} bills total", flush=True)
    stats = {"ok": 0, "skip": 0, "error": 0}
    for i, b in enumerate(all_bills, 1):
        result = dump_one(zc.get_bill_detail, b["bill_id"], out_dir, force)
        stats[result] += 1
        if i % 25 == 0:
            print(f"  [{label}] [{i}/{len(all_bills)}] ok={stats['ok']} skip={stats['skip']} err={stats['error']}", flush=True)
    print(f"✓ [{label}] done: {stats}", flush=True)
    return stats


def summarize():
    for label, d in [("invoices", INV_DIR), ("bills", BILL_DIR), ("bills_employees", BILL_EMP_DIR)]:
        if not d.exists():
            continue
        files = list(d.glob("*.json"))
        with_lines = without_lines = with_attach = 0
        for f in files:
            try:
                obj = json.loads(f.read_text())
            except Exception:
                continue
            lines = obj.get("line_items") or []
            docs = obj.get("documents") or []
            if lines:
                with_lines += 1
            else:
                without_lines += 1
            if docs:
                with_attach += 1
        print(f"  {label}: {len(files)} files | with line_items: {with_lines} | empty: {without_lines} | with attachment: {with_attach}", flush=True)


def main():
    args = sys.argv[1:]
    force = "--force" in args
    phase = next((a for a in args if a in ("phase1", "phase2", "phase3", "all")), "phase1+2")

    DUMP_DIR.mkdir(exist_ok=True)
    print(f"Dump dir: {DUMP_DIR}", flush=True)

    if phase in ("phase1", "phase1+2", "all"):
        phase1_invoices(force=force)
    if phase in ("phase2", "phase1+2", "all"):
        phase2_vendors_list()
    if phase == "phase3" or phase == "all":
        phase3_bills(PRINT_VENDORS, BILL_DIR, "print", force=force)
        phase3_bills(EMPLOYEE_VENDORS, BILL_EMP_DIR, "employees", force=force)

    print("\n=== Summary ===", flush=True)
    summarize()


if __name__ == "__main__":
    main()

import os
import requests

# Whitelist des customer_id BDouin (NEXV DMCC = compte mixte HayHay+BDouin)
# Déterminée par analyse des factures — ne pas modifier sans revérification
BDOUIN_CUSTOMER_IDS = {
    "4029527000000082139",  # Atelier BLG
    "4029527000016297125",  # Nexv DMCC
    "4029527000019366064",  # McGraw Hill
    "4029527000005941351",  # Rachid Ouaich
    "4029527000007431001",  # Fouad Ouaich
    "4029527000004572117",  # Karim ALLAM
    "4029527000019402370",  # Noredine Allam
    "4029527000004347429",  # Sofiadis (distribution France)
}

# Vendor IDs des imprimeurs BDouin
PRINT_VENDOR_IDS = {
    "4029527000010369013": "Sofiadis",    # Imprimeur France
    "4029527000003084213": "İMAK OFSET",  # Imprimeur Turquie
}

CLIENT_ID = os.environ["ZOHO_CLIENT_ID"]
CLIENT_SECRET = os.environ["ZOHO_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["ZOHO_REFRESH_TOKEN"]
ORG_ID = os.environ["ZOHO_ORG_ID"]
API_BASE = os.environ.get("ZOHO_API_BASE", "https://www.zohoapis.com")

_access_token = None


def _get_access_token():
    global _access_token
    resp = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"Zoho token refresh failed: {data}")
    _access_token = data["access_token"]
    return _access_token


def _headers():
    token = _access_token or _get_access_token()
    return {"Authorization": f"Zoho-oauthtoken {token}"}


def _get(path, params=None, retry=True):
    url = f"{API_BASE}/books/v3/{path}"
    params = {**(params or {}), "organization_id": ORG_ID}
    resp = requests.get(url, headers=_headers(), params=params)
    if resp.status_code == 401 and retry:
        _get_access_token()
        return _get(path, params, retry=False)
    resp.raise_for_status()
    return resp.json()


def get_invoices(status=None, date_start=None, date_end=None, page=1, per_page=200):
    params = {"page": page, "per_page": per_page, "sort_column": "date", "sort_order": "D"}
    if status:
        params["status"] = status
    if date_start:
        params["date_start"] = date_start
    if date_end:
        params["date_end"] = date_end
    data = _get("invoices", params)
    return data.get("invoices", [])


def get_invoice_detail(invoice_id):
    data = _get(f"invoices/{invoice_id}")
    return data.get("invoice", {})


def get_all_invoices(status=None, date_start=None, date_end=None):
    all_invoices = []
    page = 1
    while True:
        batch = get_invoices(status=status, date_start=date_start, date_end=date_end, page=page)
        all_invoices.extend(batch)
        if len(batch) < 200:
            break
        page += 1
    return all_invoices


def get_bill_detail(bill_id):
    data = _get(f"bills/{bill_id}")
    return data.get("bill", {})


def get_all_bills(status=None, date_start=None, date_end=None):
    all_bills = []
    page = 1
    while True:
        batch = get_bills(status=status, date_start=date_start, date_end=date_end, page=page)
        all_bills.extend(batch)
        if len(batch) < 200:
            break
        page += 1
    return all_bills


def download_invoice_pdf(invoice_id):
    """Télécharge le PDF de la facture (contenu binaire)."""
    url = f"{API_BASE}/books/v3/invoices/{invoice_id}"
    params = {"organization_id": ORG_ID, "accept": "pdf"}
    resp = requests.get(url, headers=_headers(), params=params)
    if resp.status_code == 401:
        _get_access_token()
        resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    return resp.content


def download_bill_attachment(bill_id):
    """Télécharge l'attachment uploadé sur la bill (PDF/image)."""
    url = f"{API_BASE}/books/v3/bills/{bill_id}/attachment"
    params = {"organization_id": ORG_ID}
    resp = requests.get(url, headers=_headers(), params=params)
    if resp.status_code == 401:
        _get_access_token()
        resp = requests.get(url, headers=_headers(), params=params)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.content


def get_bdouin_invoices(status=None, date_start=None, date_end=None):
    """Retourne uniquement les factures BDouin (filtre par whitelist clients)."""
    all_inv = get_all_invoices(status=status, date_start=date_start, date_end=date_end)
    return [i for i in all_inv if i.get("customer_id") in BDOUIN_CUSTOMER_IDS]


def get_bills(status=None, date_start=None, date_end=None, page=1, per_page=200):
    """Factures fournisseurs (achats/print)."""
    params = {"page": page, "per_page": per_page, "sort_column": "date", "sort_order": "D"}
    if status:
        params["status"] = status
    if date_start:
        params["date_start"] = date_start
    if date_end:
        params["date_end"] = date_end
    data = _get("bills", params)
    return data.get("bills", [])


def get_print_bills(date_start=None, date_end=None):
    """Retourne les factures d'impression BDouin (Sofiadis + IMAK)."""
    all_bills = []
    for vendor_id, vendor_name in PRINT_VENDOR_IDS.items():
        params = {"vendor_id": vendor_id, "per_page": 200, "sort_column": "date", "sort_order": "D"}
        if date_start:
            params["date_start"] = date_start
        if date_end:
            params["date_end"] = date_end
        data = _get("bills", params)
        bills = data.get("bills", [])
        for b in bills:
            b["_vendor_name"] = vendor_name
        all_bills.extend(bills)
    return sorted(all_bills, key=lambda x: x.get("date", ""), reverse=True)


def get_contacts(page=1, per_page=200):
    data = _get("contacts", {"page": page, "per_page": per_page})
    return data.get("contacts", [])

#!/usr/bin/env python3
"""
collect_drive.py — Collecte TOUS les fichiers BDouin sur Google Drive
Requiert : service account JSON avec accès Drive partagé

Usage:
    python3 collect_drive.py --creds path/to/service_account.json
    python3 collect_drive.py --creds path/to/service_account.json --query "bdouin"

Si tu n'as pas de service account, utilise l'OAuth flow :
    python3 collect_drive.py --oauth
    (nécessite credentials.json de Google Cloud Console)

Partager les dossiers Drive avec :
    bdouin-dashboard@heroic-footing-273510.iam.gserviceaccount.com
"""

import argparse
import json
import os
import sys
import tempfile
import psycopg2
import openpyxl

DB_URL = "postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway"

SEARCH_QUERIES = [
    "bdouin",
    "sofiadis",
    "imak",
    "sofiaco",
    "foulane",
    "awlad",
    "relevé",
    "logistique",
    "facture",
    "invoice",
    "impression",
]

PARSEABLE_MIMES = {
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
    'application/vnd.ms-excel': '.xls',
    'text/csv': '.csv',
    'application/vnd.google-apps.spreadsheet': 'sheets',  # Google Sheets → export as xlsx
}

def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn

def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drive_raw (
            id SERIAL PRIMARY KEY,
            file_id TEXT UNIQUE,
            name TEXT,
            mime_type TEXT,
            parent_folder TEXT,
            web_url TEXT,
            modified_at TIMESTAMP,
            content_json JSONB,
            collected_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS drive_raw_fileid ON drive_raw(file_id);
        CREATE INDEX IF NOT EXISTS drive_raw_name ON drive_raw(name);
    """)
    conn.commit()
    cur.close()

def read_xlsx_to_json(data: bytes):
    try:
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            f.write(data)
            tmp = f.name
        wb = openpyxl.load_workbook(tmp, data_only=True)
        sheets = []
        for ws in wb.worksheets:
            rows = []
            for row in ws.iter_rows(values_only=True):
                cleaned = []
                for cell in row:
                    if hasattr(cell, 'strftime'):
                        cleaned.append(str(cell.date()))
                    else:
                        cleaned.append(cell)
                rows.append(cleaned)
            sheets.append({"name": ws.title, "rows": rows})
        os.unlink(tmp)
        return sheets
    except Exception as e:
        return [{"error": str(e)}]

def safe_json(obj):
    s = json.dumps(obj, ensure_ascii=False, default=str)
    return s.replace('\x00', '')

def build_service(creds_path=None, use_oauth=False):
    from googleapiclient.discovery import build

    if use_oauth:
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        import pickle

        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
        token_path = os.path.expanduser('~/.bdouin_drive_token.pickle')

        creds = None
        if os.path.exists(token_path):
            with open(token_path, 'rb') as f:
                creds = pickle.load(f)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    creds_path or 'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_path, 'wb') as f:
                import pickle
                pickle.dump(creds, f)

        return build('drive', 'v3', credentials=creds)

    else:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)

def search_files(service, query_text):
    """Search Drive for files matching query_text"""
    files = []
    page_token = None
    q = f"fullText contains '{query_text}' and trashed=false"

    while True:
        resp = service.files().list(
            q=q,
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, parents, webViewLink, modifiedTime)',
            pageSize=100,
            pageToken=page_token
        ).execute()

        files.extend(resp.get('files', []))
        page_token = resp.get('nextPageToken')
        if not page_token:
            break

    return files

def download_file(service, file_id, mime_type):
    """Download a Drive file. Returns bytes."""
    if mime_type == 'application/vnd.google-apps.spreadsheet':
        # Export Google Sheet as xlsx
        req = service.files().export_media(
            fileId=file_id,
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        req = service.files().get_media(fileId=file_id)

    from googleapiclient.http import MediaIoBaseDownload
    import io
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()

def collect(creds_path=None, use_oauth=False, queries=None):
    print("Building Drive service...")
    service = build_service(creds_path, use_oauth)
    print("✓ Drive connected")

    conn = get_conn()
    ensure_table(conn)
    cur = conn.cursor()

    # Get already collected file IDs
    cur.execute("SELECT file_id FROM drive_raw")
    already = set(r[0] for r in cur.fetchall())

    queries = queries or SEARCH_QUERIES
    seen_ids = set()
    total_new = 0

    for q in queries:
        print(f"\nSearching: '{q}'")
        try:
            files = search_files(service, q)
        except Exception as e:
            print(f"  Error: {e}")
            continue

        print(f"  Found {len(files)} files")

        for f in files:
            fid = f['id']
            name = f.get('name', '')
            mime = f.get('mimeType', '')
            url  = f.get('webViewLink', '')
            mod  = f.get('modifiedTime', '')

            # Deduplicate across queries
            if fid in seen_ids or fid in already:
                continue
            seen_ids.add(fid)

            # Only parse parseable types
            if mime not in PARSEABLE_MIMES and not name.lower().endswith(('.xlsx', '.xls', '.csv')):
                # Store metadata only for non-parseable
                content = [{"mime": mime, "note": "not_parsed"}]
            else:
                try:
                    data = download_file(service, fid, mime)
                    if mime == 'application/vnd.google-apps.spreadsheet' or name.lower().endswith(('.xlsx', '.xls')):
                        content = read_xlsx_to_json(data)
                    elif name.lower().endswith('.csv'):
                        text = data.decode('utf-8', errors='replace').replace('\x00', '')
                        import csv, io
                        rows = list(csv.reader(io.StringIO(text)))[:5000]
                        content = [{"name": "csv", "rows": rows}]
                    else:
                        content = [{"mime": mime, "note": "not_parsed"}]
                    print(f"  ✓ {name}")
                except Exception as e:
                    content = [{"error": str(e)}]
                    print(f"  ✗ {name}: {e}")

            try:
                cur.execute("SAVEPOINT sp1")
                cur.execute("""
                    INSERT INTO drive_raw (file_id, name, mime_type, web_url, modified_at, content_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (file_id) DO NOTHING
                """, (fid, name, mime, url, mod, safe_json(content)))
                conn.commit()
                total_new += 1
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT sp1")
                conn.rollback()
                print(f"  DB error {name}: {e}")

    cur.execute("SELECT COUNT(*) FROM drive_raw")
    total = cur.fetchone()[0]
    conn.close()
    print(f"\nDone! New: {total_new} | Total drive_raw: {total}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--creds', help='Path to service_account.json or credentials.json')
    parser.add_argument('--oauth', action='store_true', help='Use OAuth 2.0 desktop flow')
    parser.add_argument('--queries', nargs='+', help='Search queries (default: bdouin sofiadis imak...)')
    args = parser.parse_args()

    if not args.creds and not args.oauth:
        print("ERROR: provide --creds path or --oauth")
        print()
        print("Pour OAuth:")
        print("  1. Aller sur https://console.cloud.google.com")
        print("  2. APIs & Services > Credentials > Create OAuth 2.0 Client ID")
        print("  3. Download as credentials.json")
        print("  4. pip3 install google-auth-oauthlib")
        print("  5. python3 collect_drive.py --creds credentials.json --oauth")
        print()
        print("Pour Service Account (si accès partagé avec bdouin-dashboard@heroic-footing-273510.iam.gserviceaccount.com):")
        print("  1. Récupérer le JSON key du service account depuis Google Cloud Console")
        print("  2. python3 collect_drive.py --creds service_account.json")
        sys.exit(1)

    collect(
        creds_path=args.creds,
        use_oauth=args.oauth,
        queries=args.queries,
    )

#!/usr/bin/env python3
"""
collect_drive_oauth.py — Aspire le Drive de Karim via OAuth utilisateur.

1ère exécution : ouvre le navigateur, demande l'autorisation
                  → sauvegarde le refresh_token dans /tmp/drive_token.json
Exécutions suivantes : utilise le refresh_token, totalement automatique.

Tables :
  drive_files    — métadonnées tous les fichiers Drive de Karim
                   (filtre BDouin sur la search ensuite via SQL)
"""

import os
import json
import time
import psycopg2
from psycopg2.extras import execute_values
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

DB_URL    = 'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway'
CLIENT    = '/tmp/oauth_client.json'
TOKEN     = '/tmp/drive_token.json'
SCOPES    = ['https://www.googleapis.com/auth/drive.readonly']

DDL = """
CREATE TABLE IF NOT EXISTS drive_files (
    id              TEXT PRIMARY KEY,
    title           TEXT,
    mime_type       TEXT,
    file_extension  TEXT,
    file_size       BIGINT,
    parent_id       TEXT,
    parents         JSONB,
    owner           TEXT,
    web_url         TEXT,
    created_time    TIMESTAMP,
    modified_time   TIMESTAMP,
    starred         BOOLEAN,
    shared          BOOLEAN,
    trashed         BOOLEAN,
    raw             JSONB,
    collected_at    TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_drive_modified ON drive_files(modified_time);
CREATE INDEX IF NOT EXISTS idx_drive_owner    ON drive_files(owner);
CREATE INDEX IF NOT EXISTS idx_drive_parent   ON drive_files(parent_id);
CREATE INDEX IF NOT EXISTS idx_drive_mime     ON drive_files(mime_type);
"""


def get_creds():
    creds = None
    if os.path.exists(TOKEN):
        creds = Credentials.from_authorized_user_file(TOKEN, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN, 'w') as f:
            f.write(creds.to_json())
    return creds


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(DDL); conn.commit()
    print('✓ Table drive_files ready')

    print('Authenticating...')
    creds = get_creds()
    svc = build('drive', 'v3', credentials=creds, cache_discovery=False)
    print('✓ Authenticated')

    # Get total count first
    print('\nEnumerating all Drive files (visible to Karim)...')

    fields = (
        'nextPageToken, files('
        'id,name,mimeType,fileExtension,size,parents,owners,'
        'webViewLink,createdTime,modifiedTime,starred,shared,trashed)'
    )

    page_token = None
    total = 0
    while True:
        try:
            resp = svc.files().list(
                q='trashed=false',
                pageSize=1000,
                fields=fields,
                pageToken=page_token,
                spaces='drive',
                corpora='user',  # only files Karim owns or has access to
                orderBy='modifiedTime desc',
            ).execute()
        except Exception as e:
            print(f'✗ {e}')
            break

        files = resp.get('files', [])
        if not files:
            break

        rows = []
        for f in files:
            owners = f.get('owners', [])
            owner_email = owners[0].get('emailAddress', '') if owners else ''
            parents = f.get('parents', [])
            parent_id = parents[0] if parents else None
            try:
                size = int(f.get('size', 0)) if f.get('size') else None
            except Exception:
                size = None
            rows.append((
                f['id'],
                (f.get('name') or '')[:500],
                f.get('mimeType'),
                f.get('fileExtension'),
                size,
                parent_id,
                json.dumps(parents),
                owner_email,
                f.get('webViewLink'),
                f.get('createdTime'),
                f.get('modifiedTime'),
                f.get('starred', False),
                f.get('shared', False),
                f.get('trashed', False),
                json.dumps(f, ensure_ascii=False, default=str),
            ))

        execute_values(cur, """
            INSERT INTO drive_files
              (id, title, mime_type, file_extension, file_size, parent_id, parents,
               owner, web_url, created_time, modified_time, starred, shared, trashed, raw)
            VALUES %s ON CONFLICT (id) DO UPDATE SET
              title=EXCLUDED.title, modified_time=EXCLUDED.modified_time,
              file_size=EXCLUDED.file_size, raw=EXCLUDED.raw,
              collected_at=NOW()
        """, rows)
        conn.commit()
        total += len(rows)
        print(f'  {total} files inserted...')

        page_token = resp.get('nextPageToken')
        if not page_token:
            break
        time.sleep(0.2)

    print(f'\n✓ Total files: {total}')

    # Stats
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT owner) FROM drive_files")
    n, owners = cur.fetchone()
    print(f'  {n} files | {owners} different owners')
    cur.execute("SELECT mime_type, COUNT(*) FROM drive_files GROUP BY mime_type ORDER BY 2 DESC LIMIT 10")
    print('\n  Top mime types:')
    for r in cur.fetchall():
        print(f'    {r[0]}: {r[1]}')
    conn.close()


if __name__ == '__main__':
    main()

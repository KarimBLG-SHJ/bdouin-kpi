/**
 * BDouin Gmail Collector — Apps Script
 * Collecte automatiquement les relevés Sofiadis B2B, logistique, et factures IMAK
 * depuis Gmail, parse les pièces jointes Excel + corps des emails, envoie vers Railway.
 *
 * Équipe BDouin impliquée dans les échanges :
 *   karim80080@gmail.com   — Karim Allam (perso)
 *   karim@nexv.co          — Karim Allam (pro / ancien)
 *   noredineallam@gmail.com — Norédine Allam (directeur artistique)
 *   rachid@nexv.co          — Rachid O. (associé)
 *
 * Setup :
 *   1. Coller ce script dans Google Apps Script (script.google.com)
 *   2. Activer les services : Drive API v2, Gmail (automatique)
 *   3. Exécuter setupLabels() une seule fois
 *   4. Exécuter collectAll() pour import initial (traite tout l'historique)
 *   5. Créer un trigger : Triggers → collectAll → Time-driven → Weekly (lundi 8h)
 */

const RAILWAY_URL = "https://web-production-b0b2d.up.railway.app";
const API_KEY     = "247fddb4931b7eb38bc54db3bb2aabd09213cc3206b65e225f4c1d6ab82603ef";

const LABEL_B2B  = "bdouin/sofiadis-b2b";
const LABEL_LOG  = "bdouin/sofiadis-logistics";
const LABEL_IMAK = "bdouin/imak-invoices";

// Toutes les adresses de l'équipe BDouin (pour les forwards/CC)
const TEAM_EMAILS = [
  "karim80080@gmail.com",     // Karim Allam — perso
  "karim@bdouin.com",         // Karim Allam — BDouin
  "karim@nexv.co",            // Karim Allam — pro/ancien
  "contact@bdouin.com",       // BDouin contact général
  "noredineallam@gmail.com",  // Norédine Allam — directeur artistique
  "rachid@nexv.co",           // Rachid O. — associé
  "rachidaikoufane@gmail.com" // Book designer (reçoit copies des relevés IMAK/Sofiadis)
];


// ─── POINT D'ENTRÉE PRINCIPAL ────────────────────────────────────────────────

function collectAll() {
  Logger.log("=== BDouin Gmail Collector START ===");
  collectSofiadisB2B();
  collectSofiadisLogistics();
  collectImak();
  Logger.log("=== BDouin Gmail Collector END ===");
}


// ─── SOFIADIS B2B — RELEVÉS DE VENTES ET RETOURS ─────────────────────────────

function collectSofiadisB2B() {
  const label = ensureLabel(LABEL_B2B);

  // Cherche aussi les forwards par l'équipe (Rachid, Norédine, karim@nexv.co)
  const queries = [
    `from:compta3@sofiadis.fr subject:"RELEVE DE VENTES" -label:${LABEL_B2B}`,
    `from:(${TEAM_EMAILS.join(" OR from:")}) subject:"RELEVE DE VENTES" has:attachment -label:${LABEL_B2B}`,
  ];

  let total = 0;
  for (const query of queries) {
    const threads = GmailApp.search(query);
    Logger.log(`[B2B] Query: "${query.substring(0,60)}..." → ${threads.length} threads`);

    for (const thread of threads) {
      for (const msg of thread.getMessages()) {
        const subject = msg.getSubject();
        const period  = extractPeriod(subject);
        const body    = msg.getPlainBody().substring(0, 2000); // corps de l'email

        for (const att of msg.getAttachments()) {
          const name = att.getName().toLowerCase();
          if (!name.match(/\.(xlsx|xls)$/)) continue;

          Logger.log(`[B2B] Parsing: ${name} — période: ${period}`);
          const sheets = parseExcel(att);
          if (!sheets || sheets.length === 0) continue;
          Logger.log(`[B2B] ${sheets.length} feuille(s): ${sheets.map(s=>s.name).join(', ')}`);

          const res = postToRailway("/api/sofiadis/b2b/ingest", {
            period,
            sheets,
            source:     subject,
            email_body: body,
            sender:     msg.getFrom()
          });
          Logger.log(`[B2B] → Railway: ${JSON.stringify(res)}`);
          total++;
        }
      }
      thread.addLabel(label);
    }
  }
  Logger.log(`[B2B] Total traités: ${total}`);
}


// ─── SOFIADIS LOGISTIQUE ──────────────────────────────────────────────────────

function collectSofiadisLogistics() {
  const label = ensureLabel(LABEL_LOG);

  const queries = [
    `from:mj@sofiaco.fr subject:logistique has:attachment -label:${LABEL_LOG}`,
    `from:sofiaco.fr subject:logistique has:attachment -label:${LABEL_LOG}`,
    `from:compta3@sofiadis.fr subject:logistique has:attachment -label:${LABEL_LOG}`,
    // Forwards de l'équipe avec pièces jointes logistique
    `from:(${TEAM_EMAILS.join(" OR from:")}) subject:logistique has:attachment -label:${LABEL_LOG}`,
  ];

  for (const query of queries) {
    const threads = GmailApp.search(query);
    Logger.log(`[LOG] Query: "${query.substring(0,60)}..." → ${threads.length} threads`);

    for (const thread of threads) {
      for (const msg of thread.getMessages()) {
        const subject = msg.getSubject();
        const period  = extractPeriod(subject);
        const body    = msg.getPlainBody().substring(0, 2000);

        for (const att of msg.getAttachments()) {
          const name    = att.getName().toLowerCase();
          const isExcel = name.match(/\.(xlsx|xls)$/);
          const isPdf   = name.match(/\.pdf$/);
          if (!isExcel && !isPdf) continue;

          Logger.log(`[LOG] Parsing: ${name} — période: ${period}`);
          let payload = { period, source: subject, email_body: body, sender: msg.getFrom() };

          if (isExcel) {
            const sheets = parseExcel(att);
            if (sheets && sheets.length > 0) payload.sheets = sheets;
          } else if (isPdf) {
            const text   = parsePdfText(att);
            const amount = extractAmountFromText(text) || extractAmountFromText(body);
            if (amount) payload.amount_ht = amount;
            payload.raw_text = text.substring(0, 500);
          }

          const res = postToRailway("/api/sofiadis/logistics/ingest", payload);
          Logger.log(`[LOG] → Railway: ${JSON.stringify(res)}`);
        }
      }
      thread.addLabel(label);
    }
  }
}


// ─── IMAK — FACTURES D'IMPRESSION ────────────────────────────────────────────

function collectImak() {
  const label = ensureLabel(LABEL_IMAK);

  const queries = [
    // Factures directes IMAK
    `from:imakofset.com.tr has:attachment (subject:invoice OR subject:Invoice OR subject:INVOICE) -label:${LABEL_IMAK}`,
    // Forwards par l'équipe
    `from:(${TEAM_EMAILS.join(" OR from:")}) from:imakofset.com.tr has:attachment -label:${LABEL_IMAK}`,
    // Fichiers Excel avec "invoice" dans le nom de pièce jointe
    `from:imakofset.com.tr has:attachment filename:invoice -label:${LABEL_IMAK}`,
    `from:imakofset.com.tr has:attachment filename:xlsx -label:${LABEL_IMAK}`,
  ];

  for (const query of queries) {
    const threads = GmailApp.search(query);
    Logger.log(`[IMAK] Query: "${query.substring(0,60)}..." → ${threads.length} threads`);

    for (const thread of threads) {
      for (const msg of thread.getMessages()) {
        const subject   = msg.getSubject();
        const emailDate = Utilities.formatDate(msg.getDate(), "UTC", "yyyy-MM-dd");
        const period    = emailDate.substring(0, 7);
        const body      = msg.getPlainBody().substring(0, 2000);

        for (const att of msg.getAttachments()) {
          const name    = att.getName().toLowerCase();
          const isExcel = name.match(/\.(xlsx|xls)$/);
          const isPdf   = name.match(/\.pdf$/);
          if (!isExcel && !isPdf) continue;

          Logger.log(`[IMAK] Parsing: ${name} — date: ${emailDate}`);
          let payload = {
            print_date:  emailDate,
            period:      period,
            source:      subject,
            invoice_ref: extractInvoiceRef(att.getName()),
            email_body:  body,
            sender:      msg.getFrom()
          };

          if (isExcel) {
            const sheets = parseExcel(att);
            if (sheets && sheets.length > 0) payload.sheets = sheets;
          } else if (isPdf) {
            const text = parsePdfText(att);
            payload.raw_text = text.substring(0, 1000);
            payload.rows     = extractRowsFromPdfText(text);
          }

          if ((!payload.sheets || payload.sheets.length === 0) && (!payload.rows || payload.rows.length === 0)) {
            Logger.log(`[IMAK] Aucune donnée extraite de ${name}`);
            continue;
          }

          const res = postToRailway("/api/imak/ingest", payload);
          Logger.log(`[IMAK] → Railway: ${JSON.stringify(res)}`);
        }
      }
      thread.addLabel(label);
    }
  }
}


// ─── UTILS — PARSING ─────────────────────────────────────────────────────────

function parseExcel(attachment) {
  // 1. Sauvegarde le blob xlsx via DriveApp
  // 2. Copie avec conversion via Drive REST v3 (UrlFetchApp + OAuth token)
  // 3. Lit TOUTES les feuilles avec SpreadsheetApp, supprime les deux fichiers temp
  let xlsId   = null;
  let sheetId = null;
  try {
    const blob = attachment.copyBlob();
    xlsId = DriveApp.createFile(blob).getId();
    Utilities.sleep(800); // évite rate limit Drive API

    const token = ScriptApp.getOAuthToken();
    const res = UrlFetchApp.fetch(
      `https://www.googleapis.com/drive/v3/files/${xlsId}/copy`,
      {
        method:             "POST",
        headers:            { "Authorization": "Bearer " + token, "Content-Type": "application/json" },
        payload:            JSON.stringify({ mimeType: "application/vnd.google-apps.spreadsheet" }),
        muteHttpExceptions: true
      }
    );
    const json = JSON.parse(res.getContentText());
    sheetId = json.id;
    if (!sheetId) throw new Error("copy failed: " + res.getContentText());

    const ss = SpreadsheetApp.openById(sheetId);
    // Retourne TOUTES les feuilles (Sofiadis a plusieurs onglets)
    return ss.getSheets().map(s => ({
      name: s.getName(),
      data: s.getDataRange().getValues()
    }));
  } catch (e) {
    Logger.log(`[parseExcel] Erreur: ${e}`);
    return null;
  } finally {
    if (xlsId)   try { DriveApp.getFileById(xlsId).setTrashed(true);   } catch(e) {}
    if (sheetId) try { DriveApp.getFileById(sheetId).setTrashed(true); } catch(e) {}
  }
}

function parsePdfText(attachment) {
  let pdfId  = null;
  let docId  = null;
  try {
    const blob = attachment.copyBlob().setContentType("application/pdf");
    pdfId = DriveApp.createFile(blob).getId();

    const token = ScriptApp.getOAuthToken();
    const res = UrlFetchApp.fetch(
      `https://www.googleapis.com/drive/v3/files/${pdfId}/copy`,
      {
        method:             "POST",
        headers:            { "Authorization": "Bearer " + token, "Content-Type": "application/json" },
        payload:            JSON.stringify({ mimeType: "application/vnd.google-apps.document" }),
        muteHttpExceptions: true
      }
    );
    const json = JSON.parse(res.getContentText());
    docId = json.id;
    if (!docId) throw new Error("pdf copy failed: " + res.getContentText());

    return DocumentApp.openById(docId).getBody().getText();
  } catch (e) {
    Logger.log(`[parsePdfText] Erreur: ${e}`);
    return "";
  } finally {
    if (pdfId) try { DriveApp.getFileById(pdfId).setTrashed(true); } catch(e) {}
    if (docId) try { DriveApp.getFileById(docId).setTrashed(true); } catch(e) {}
  }
}

function extractAmountFromText(text) {
  const patterns = [
    /total\s+h\.?t\.?\s*[:\-]?\s*([\d\s]+[,.][\d]{2})/i,
    /montant\s+total\s*[:\-]?\s*([\d\s]+[,.][\d]{2})/i,
    /total\s*[:\-]?\s*([\d\s]+[,.][\d]{2})\s*€/i,
    /net\s+à\s+payer\s*[:\-]?\s*([\d\s]+[,.][\d]{2})/i,
  ];
  for (const p of patterns) {
    const m = text.match(p);
    if (m) {
      const f = parseFloat(m[1].replace(/\s/g,'').replace(',','.'));
      if (!isNaN(f) && f > 10) return f;
    }
  }
  return null;
}

function extractRowsFromPdfText(text) {
  const lines = text.split('\n').map(l => l.trim()).filter(l => l.length > 0);
  const rows  = [["title", "qty", "unit_cost", "total"]];
  const numPat = /(\d[\d\s,.]*)/g;
  for (const line of lines) {
    const nums = [...line.matchAll(numPat)].map(m => m[1].replace(/\s/g,'').replace(',','.'));
    if (nums.length >= 2 && line.length > 10) {
      const title = line.replace(/[\d,.\s€]+/g, ' ').trim();
      if (title.length > 3) {
        rows.push([title, nums[0] || 0, nums[1] || 0, nums[nums.length - 1] || 0]);
      }
    }
  }
  return rows.length > 1 ? rows : [];
}

function extractPeriod(subject) {
  const months = {
    'janvier':'01','january':'01',
    'février':'02','fevrier':'02','february':'02',
    'mars':'03','march':'03',
    'avril':'04','april':'04',
    'mai':'05','may':'05',
    'juin':'06','june':'06',
    'juillet':'07','july':'07',
    'août':'08','aout':'08','august':'08',
    'septembre':'09','september':'09',
    'octobre':'10','october':'10',
    'novembre':'11','november':'11',
    'décembre':'12','decembre':'12','december':'12',
  };
  const s = subject.toLowerCase();
  const yearMatch = subject.match(/20\d{2}/);
  const year = yearMatch ? yearMatch[0] : new Date().getFullYear().toString();
  for (const [name, num] of Object.entries(months)) {
    if (s.includes(name)) return `${year}-${num}`;
  }
  const d = new Date();
  d.setMonth(d.getMonth() - 1);
  return Utilities.formatDate(d, "UTC", "yyyy-MM");
}

function extractInvoiceRef(filename) {
  const m = filename.match(/([A-Z0-9]{5,})/);
  return m ? m[1] : filename.replace(/\.(xlsx|xls|pdf)$/i, '');
}


// ─── UTILS — HTTP + LABELS ────────────────────────────────────────────────────

function postToRailway(path, payload) {
  try {
    const res = UrlFetchApp.fetch(RAILWAY_URL + path, {
      method:             "POST",
      contentType:        "application/json",
      payload:            JSON.stringify(payload),
      headers:            { "X-API-Key": API_KEY },
      muteHttpExceptions: true
    });
    const code = res.getResponseCode();
    const body = res.getContentText();
    if (code !== 200) Logger.log(`[postToRailway] ${path} HTTP ${code}: ${body}`);
    return JSON.parse(body);
  } catch (e) {
    Logger.log(`[postToRailway] Erreur: ${e}`);
    return { error: String(e) };
  }
}

function ensureLabel(name) {
  let label = GmailApp.getUserLabelByName(name);
  if (!label) label = GmailApp.createLabel(name);
  return label;
}

function setupLabels() {
  ensureLabel(LABEL_B2B);
  ensureLabel(LABEL_LOG);
  ensureLabel(LABEL_IMAK);
  Logger.log("Labels créés : " + [LABEL_B2B, LABEL_LOG, LABEL_IMAK].join(", "));
}

# Roadmap BDouin KPI

**Dernière MAJ** : 2026-04-22 · **Prod** : [web-production-b0b2d.up.railway.app](https://web-production-b0b2d.up.railway.app)

---

## ✅ Done

| Date | Tâche |
|---|---|
| 2026-04 | Auth serveur + cookie signé HMAC 30j |
| 2026-04 | Retrait complet IMAK du dashboard (hors scope) |
| 2026-04 | Message campagne email dynamique |
| 2026-04 | Endpoint `/api/summary` (KPI JSON unifié) |
| 2026-04 | Auth double : cookie OU X-API-Key pour server-to-server |
| 2026-04-22 | `SUMMARY_API_KEY` déployée Railway |
| 2026-04-22 | Blocage indexation (robots.txt + X-Robots-Tag + meta) |
| 2026-04-22 | Inventaire complet GA4 : 11 properties sur 6 comptes |
| 2026-04-22 | Scope GA4 validé : 4 properties cibles |
| 2026-04-22 | Service account `bdouin-analitycs@bdouin.iam.gserviceaccount.com` vérifié + clé JSON locale |
| 2026-04-22 | Accès Lecteur ajouté sur **BDouin Shop** et **HooPow** — API GA4 testée OK (Shop : 3471 users / 4620 sessions sur 7j) |
| 2026-04-22 | Endpoint `/api/ga4-multi` codé (parallèle multi-property) + push |
| 2026-04-22 | Roadmap tracker HTML live (cet écran) |

## 🔄 In Progress

- [ ] Ajouter env vars `GA4_CREDENTIALS_JSON` + `GA4_PROPERTIES` sur Railway pour activer `/api/ga4-multi`

## ⏳ Next (priorisé)

1. Onglet **"Apps & Sites"** dans le dashboard — consomme `/api/ga4-multi`
2. **Stores reviews** iOS + Android (scraping public, pas d'auth)
   - App Store RSS pour Awlad Quiz GO + Awlad School Mobile
   - Google Play scraping pour idem
3. **MailerLite — analyse segments** (top segments par engagement × LTV)
4. **Macro calendrier BDouin** (rentrée FR, Ramadan, Eid, Noël, BF)
5. **Google Search Console** sur bdouin.com

## 🚧 Blocked / Hors scope temporaire

- **GA4 Awlad Quiz GO + Awlad School Mobile** : Karim = Lecteur seul. Admins (Taha, Abdelhakim) non sollicités → on contourne via stores reviews + MailerLite tags
- **Coach IA (autre session)** : prompt agent_kpi à mettre à jour dans `engine.py` (appel `/api/summary`)

## 📦 Backlog (idées)

### Analyse avancée
- Cohort analysis : repeat buyers, LTV par cohorte
- Cross-sell paths ("après Foulane T1 → 62% T2-3...")
- Abandoned carts PrestaShop
- Sentiment reviews Apple/Play avec thèmes
- NPS survey via MailerLite

### Prédiction & reco
- Forecast ventes J+14/J+30 (Prophet/ARIMA + events macro)
- Prédiction tirage IMAK : vélocité × clicks emails × saves IG
- Moteur reco "customers also bought"
- Score "next best audience" pour chaque lancement

### Sources externes
- Meta Graph API (Instagram Business)
- Scraping concurrents (5-10)
- Google Trends signal demande
- Trustpilot / Google Reviews bdouin.com

### Monitoring
- Alerte review 1-2★ stores
- Alerte baisse note moyenne 7j
- Alerte pic drop-off app
- Alerte CA mois < 80% forecast à J-5

### UX
- Export PDF pour Fouad
- Vue mobile responsive
- Graphs drill-down
- Dark/light switcher

---

## 🏗 Architecture

- **Backend** : Flask `app.py` sur Railway (`sincere-reflection` / service `web`)
- **Frontend** : `static/index.html` (vanilla JS)
- **Auth** : cookie HMAC-SHA256 30j OU `X-API-Key` header
- **Sources** : PrestaShop, MailerLite, GA4 Data API, stores reviews (à venir)

## 🧠 Règles session

- **BDouin edition only** — pas HayHay, pas IMAK (sauf lecture pour affichage), pas ContextOS
- **Token economy** — relire ce roadmap avant historique, grep ciblé, pas de gros reads inutiles
- **Deploy discipline** — `py_compile` avant commit, vérif `/api/summary` live après push
- **Ne pas toucher** `coach-telegram-bot/engine.py` — géré par session Coach IA

# 🎯 BDouin Data Initiative

> Aller chercher toutes les données de BDouin edition pour comprendre **ce que le public veut**, **ce qui marche**, et **prédire / recommander**.

**Démarré** : 2026-04-22 · **Scope** : BDouin edition uniquement

---

## 🎯 Objectifs

| # | Objectif | Sources nécessaires |
|---|---|---|
| 1 | **Attente du public** | Search Console, Google Trends, IG DMs, paniers abandonnés, reviews |
| 2 | **Trucs qui marchent** | Shop PrestaShop, MailerLite tags, GA4, reviews stores |
| 3 | **Prédiction & recommandation** | Tout l'historique + events macro (calendrier BDouin) |
| 4 | Finance (plus tard) | — |

---

## 📊 État des sources de données

### ✅ Connectées

| Source | Type | Détail |
|---|---|---|
| **PrestaShop bdouin.com** | 🛒 Shop | Commandes, paniers, clients, produits, paiements (historique complet) |
| **MailerLite** | 📧 Email | ~300k abonnés, 39 segments, tags click/open par produit |
| **GA4 BDouin Shop** | 🌐 Web | 3 471 users / 4 620 sessions (7j) — via service account `bdouin-analitycs@bdouin.iam` |
| **GA4 HooPow** | 🌐 Web | 0 users (pas de traffic actuellement sur hoopow.com) |
| **App Store Reviews** | 📱 iOS | Awlad Quiz GO (110, 4.9★), Awlad School (250, 4.96★) — refresh auto 24h |
| **Google Play Reviews** | 📱 Android | Awlad Quiz GO (173, 4.87★), Awlad School (200, 4.87★) — refresh auto 24h |

### 🔄 En cours de branchement

| Source | Statut |
|---|---|
| Endpoint `/api/ga4-multi` | Code déployé — env vars Railway à setter |
| Endpoint `/api/reviews` | ✅ Live (cache 24h, refresh auto APScheduler) |
| Onglet "Voix public" dashboard | À coder (prochaine étape) |
| Onglet "Apps & Sites" dashboard | Dès que GA4 multi testé |

### 🗄 Stockage durable ✅

| Action | Statut |
|---|---|
| Railway Postgres provisionné | ✅ |
| Tables créées : `reviews`, `web_mentions`, `raw_sources` | ✅ |
| Ingestion /api/reviews → Postgres (INSERT ON CONFLICT) | ✅ |
| `/api/db/stats` (compteurs + avg rating par app/store) | ✅ |
| `/api/reviews/history` (requête full DB avec filtres app/store/rating/country/search/dates) | ✅ |

**État DB** : **15 333 reviews stockées** (Awlad School 13 307, Awlad Quiz GO 2 026)

### 🕵️ Chantier veille web

Tracker **toutes les mentions** de BDouin / Awlad School / Awlad Quiz / Foulane / Halua / Walad Binti sur le web.

| Source | Type | Effort |
|---|---|:-:|
| Google Alerts RSS | Mentions presse / web | 🟢 facile |
| Twitter/X API ou Nitter | Posts sociaux | 🟡 moyen |
| Reddit API (gratuite) | Threads communautaires | 🟢 facile |
| Google News API / scraping | Press coverage | 🟡 moyen |
| Forums islamiques FR | Recommandations produit | 🟠 scraping manuel |
| Mention.com / Brand24 | Tout-en-un | 🔴 payant |

Chaque mention → ligne dans `web_mentions` avec sentiment + contexte. Analyse NLP ultérieure.

### ⏳ Prochaines sources (priorisées)

| Priorité | Source | Valeur attendue | Effort |
|:-:|---|---|:-:|
| 1 | **App Store reviews iOS** (Awlad Quiz GO + Awlad School Mobile) | Voix du public, demandes features, notes | 🟢 facile (RSS public) |
| 2 | **Google Play reviews Android** (idem) | Idem | 🟢 facile (scraping public) |
| 3 | **MailerLite — analyse segments** | Top segments par LTV, prospects chauds app→shop | 🟡 moyen |
| 4 | **Macro calendrier BDouin** | Events qui drivent les ventes (rentrée FR, Ramadan, Eid, Noël, BF) | 🟡 moyen |
| 5 | **Google Search Console bdouin.com** | Requêtes Google qui amènent du trafic → signal demande brute | 🟡 moyen |
| 6 | **Instagram Business API** | Reach, saves, DMs, démo audience | 🟠 nécessite accès Meta Business |
| 7 | **Google Trends** | Intérêt marché ("livre enfant musulman" etc.) | 🟢 facile |
| 8 | **Paniers abandonnés PrestaShop** | Produits désirés mais pas achetés | 🟡 déjà dans DB, pas exploité |
| 9 | **Reviews shop** (Trustpilot, Google Reviews) | Sentiment client | 🟠 variable |

### 🚧 Bloqué

| Source | Pourquoi | Contournement |
|---|---|---|
| **GA4 Awlad Quiz GO** | Karim = Lecteur seul (admins : Taha, Abdelhakim) | On passe par App Store/Play reviews |
| **GA4 Awlad School Mobile** | Idem | Idem |
| **Firebase** (Crashlytics, Performance) | Pas activé dans les apps (peut-être) | Skip — GA4 suffit |

---

## 🧠 Ce qu'on a déjà appris (premières lectures)

### BDouin Shop (GA4 + PrestaShop)
- Avril 2026 (cours) : 41 commandes, 1 940€ — forecast mois 3 234€
- Mars 2026 : 492 commandes, 27 542€
- Top vente 7j : Guide illustré 100 Bonnes Manières (16 qty, 200€)
- Paiements : 15 card, 13 paypal, 11 link stripe, 2 free

### MailerLite (39 segments)
- Audience massive mais fragmentée :
  - AWLADSCHOOL FR NOV 2025 : **231 904**
  - AWLAD SCHOOL MARCH : 90 437
  - TAG Open Offres : 89 311
  - Bdouin.com clients : **5 057** (potentiel cross-sell direct)
- Tags click/open déjà par produit : Manga T1, Foulane, Packs Awlad School, Mini Guides, Offres

### Apps (GA4 vue de loin)
- Awlad Quiz GO : 882 active users / 7j, top pays France (7) + Côte d'Ivoire + Maroc
- Events RNSScreen/UIViewController confirment : apps React Native iOS/Android

### Voix du public (stores reviews — 15 333 reviews analysées)
- **Awlad School** : 4.91★ sur 13 307 reviews (iOS 4.93, Android 4.91) — trend stable
- **Awlad Quiz GO** : 4.88★ sur 2 026 reviews (iOS 4.9, Android 4.87)

### 🔥 Premier insight actionnable — Awlad Quiz GO chute 30j
Cause identifiée via DB : **bug feature "tournoi" sur v1.1.0**.

Reviews 1★ révélatrices :
- 2026-02-28 (MA, DZ, TN, FR — 4 reviews identiques Android) : "j'ai besoin de la mise à jour pour pouvoir participer au tournoi"
- 2026-03-15 (iOS FR) : "quand il y a un grand tournoi je ne peux pas gagner de points et monter dans le classement car ça ne fonctionne plus"

→ **Action équipe tech** : release correctif tournoi v1.1.0.

---

## 🚀 Prochaines actions concrètes

1. **Ajouter `GA4_CREDENTIALS_JSON` + `GA4_PROPERTIES` sur Railway** (Karim)
2. **Tester `/api/ga4-multi`** (Claude) → validation shop + hoopow
3. **Scraper App Store + Play reviews** (Claude) → endpoint `/api/reviews`
4. **Onglet "Voix public" dans dashboard** (Claude) → sentiment + demandes features
5. **Analyse MailerLite segments** (Claude) → matrice segment × engagement × conversion

---

## 🔁 MAJ du tracker

À chaque source branchée ou insight extrait, cette page est mise à jour.
Édition : `flask-app/static/roadmap.md` → push git → Railway redeploy auto (30s).

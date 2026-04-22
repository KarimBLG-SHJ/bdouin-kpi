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

### 🔄 En cours de branchement

| Source | Statut |
|---|---|
| Endpoint `/api/ga4-multi` | Code déployé — env vars Railway à setter |
| Onglet "Apps & Sites" dashboard | Prochaine étape dès que GA4 multi testé |

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

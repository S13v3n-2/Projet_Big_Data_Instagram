---
title: Instagram Engagement Optimizer
author:
  - Steven CARLOT
  - Haci YILMAZER
date: 2026-02-03
---
# Projet_Big_Data_Instagram

# Instagram Engagement Optimizer - Big Data Project

## 1. Problematique Metier

Dans un contexte de saturation des reseaux sociaux, l'optimisation de la retention repose sur la precision du ciblage de contenu. Ce projet repond a la problematique suivante :

**« Quel type de contenu (Reels, Stories, Posts, themes) maximise l’engagement pour chaque segment d’utilisateurs ? »**

L'objectif est de transformer des donnees comportementales et lifestyle en insights decisionnels via une segmentation client (Clustering) et une prediction du score d'engagement.

## 2. Architecture des Donnees (Medallion Architecture)

Le pipeline repose sur une architecture de donnees distribuee utilisant Apache Spark et le framework Delta Lake pour garantir l'integrite et la tracabilite des donnees.

![projet_bigdata.png](assets/projet_bigdata.png)

### Couche Bronze (Raw Data)

* **Sources :** Ingestion des logs d'utilisation (comportemental) et des fichiers CSV (profils utilisateurs).
* **Format :** Conversion en Parquet.
* **Optimisation :** Mise en place d'un partitionnement initial pour accelerer les traitements Spark.

### Couche Silver (Curated Data)

* **Nettoyage :** Suppression des doublons et gestion des valeurs manquantes (NULL).
* **Normalisation :** Typage des colonnes (age, temps d'utilisation, scores) et traitement des valeurs aberrantes (outliers).
* **Feature Engineering :** Calcul de variables derivees (ex: ratio likes/sessions, temps moyen par type de contenu).

#### **Table 1 : `silver.users_profiles`** (Données statiques)
Profils utilisateurs nettoyés issus du CSV Instagram Users Lifestyle.

| Colonnes clés | Description |
|---------------|-------------|
| `user_id` (PK) | Identifiant unique utilisateur |
| `age`, `gender`, `country` | Démographie |
| `content_type_preference` | Reels/Stories/Photos |
| `preferred_content_theme` | Fitness/Fashion/Food/Travel/Tech/Family |
| `perceived_stress_score` | Score stress 0-10 |
| `weekly_work_hours` | Heures travail/semaine |
| `exercise_hours_per_week` | Heures sport/semaine |
| `ingestion_date` | Date ingestion (partition) |

**Partitionnement** : `year/month/day/country/`

#### **Table 2 : `silver.users_usage`** (Données dynamiques)
Logs d'utilisation app nettoyés issus de la BDD.

| Colonnes clés | Description |
|---------------|-------------|
| `user_id` (PK) | Clé jointure |
| `daily_active_minutes_instagram` | Temps actif/jour |
| `user_engagement_score` | Score 0-100 (variable cible) |
| `notification_response_rate` | Taux réponse notifs (0-1) |
| `subscription_status` | active/inactive |
| `time_on_reels_per_day` | Minutes reels/jour |
| `time_on_stories_per_day` | Minutes stories/jour |
| `last_login_date` | Dernière connexion |
 
**Partitionnement** : `year/month/day/subscription_status/`

#### **Table 3 : `silver.users_enriched`** (Jointure + Features ML)
Table unifiée créée via jointure `users_profiles ⋈ users_usage` avec **features calculées**.

**Features dérivées ajoutées (8 nouvelles colonnes)** :
- `engagement_rate_per_minute` : engagement_score / daily_active_minutes
- `lifestyle_segment` : "Fit Relaxed" / "Workaholic" / "Sleep Deprived" / "Balanced"
- `content_affinity_score` : Matching content_type vs optimal
- `work_life_balance_index` : (168 - work_hours) / exercise_hours
- `digital_wellbeing_score` : Score sur-usage (0-100)
- `days_since_last_login` : DATEDIFF(current_date, last_login_date)
- `churn_risk_flag` : Boolean (True si inactif >90j)
- `engagement_by_content_type` : Score selon content_preference

## Couche Gold (Business Datamarts)

La couche Gold agrège la table Silver enrichie et exporte les résultats dans PostgreSQL pour l'API FastAPI et les dashboards Power BI. Tout est géré par `datamarts.py`.

- **Input Gold :** table Hive `silver.instagram_data_users_enriched`.  
- **Output Gold :** 3 tables PostgreSQL :
  - `gold_engagement_stats`
  - `gold_user_health`
  - `gold_top_recommendations`

La connexion PostgreSQL se fait via JDBC (`jdbc:postgresql://postgres-instagram:5432/instagram_db`, utilisateur `admin`).

---

### Datamart `gold_engagement_stats` (Engagement par segment et contenu)

Datamart agrégé pour analyser la performance d'engagement par segment et par préférences de contenu.

**Agrégation :**

```text
GROUP BY country, lifestyle_segment, content_type_preference, preferred_content_theme
- avg(user_engagement_score)       → avg_engagement
- avg(engagement_rate_per_minute)  → avg_efficiency
- count(user_id)                   → total_users
```

**Schéma logique :**

| Colonne                     | Description                                        |
|-----------------------------|----------------------------------------------------|
| `country`                   | Pays                                               |
| `lifestyle_segment`         | Segment de style de vie                            |
| `content_type_preference`   | Type de contenu préféré                            |
| `preferred_content_theme`   | Thème de contenu préféré                           |
| `avg_engagement`            | Score d'engagement moyen du segment               |
| `avg_efficiency`            | Engagement moyen par minute active                |
| `total_users`               | Nombre d'utilisateurs dans le segment             |

> Utilisé pour identifier le **contenu optimal par segment** (API + dashboards Power BI).

---

### Datamart `gold_user_health` (Churn & bien‑être numérique)

Datamart résumant l'état de bien‑être numérique et le risque de churn par segment de lifestyle.

**Agrégation :**

```text
GROUP BY lifestyle_segment
- avg(digital_wellbeing_score)  → avg_wellbeing_score
- avg(days_since_last_login)    → avg_days_inactive
- sum(churn_risk_flag)          → potential_churners
```

**Schéma logique :**

| Colonne                 | Description                                   |
|-------------------------|-----------------------------------------------|
| `lifestyle_segment`     | Segment de style de vie                       |
| `avg_wellbeing_score`   | Score moyen de bien‑être numérique           |
| `avg_days_inactive`     | Jours moyens depuis la dernière connexion    |
| `potential_churners`    | Nombre d'utilisateurs à risque de churn      |

> Utilisé pour les analyses **churn / santé numérique** et les alertes sur les segments à risque.

---

### Datamart `gold_top_recommendations` (Top users par engagement)

Datamart listant les utilisateurs les plus engagés, basé sur le rang calculé en Silver.

**Filtre et sélection :**

- Filtre : `engagement_rank <= 10` (top 10 par pays).
- Colonnes sélectionnées :
  - `user_id`, `country`, `lifestyle_segment`,
  - `content_type_preference`, `preferred_content_theme`,
  - `engagement_rank`.

**Schéma logique :**

| Colonne                     | Description                                   |
|-----------------------------|-----------------------------------------------|
| `user_id`                   | Identifiant utilisateur                       |
| `country`                   | Pays                                          |
| `lifestyle_segment`         | Segment de style de vie                       |
| `content_type_preference`   | Type de contenu recommandé/préféré           |
| `preferred_content_theme`   | Thème de contenu recommandé/préféré          |
| `engagement_rank`           | Rang d'engagement (top 10 par pays)          |

> Utilisé pour les **recommandations de contenus** (API) et les vues "Top N utilisateurs" dans Power BI.

---

## 3. Stack Technique

* **Traitement de donnees :** Apache Spark (PySpark)
* **Stockage :** Delta Lake, PostgreSQL
* **Machine Learning :** Scikit-Learn (K-Means), XGBoost
* **Visualisation :** Power BI
* **Interface de service :** FastAPI

## 4. Analyse et Visualisation (Power BI)

Le dashboard final offre une vision 360 des performances :

* **Heatmap :** Croisement entre le type de contenu (Content type) et le profil utilisateur (Lifestyle).
* **Performance Geographique :** Classement des meilleurs formats (Reels/Stories) par pays.
* **Suivi KPI :** Analyse du ROI par segment.
* **Table de Recommandation :** Visualisation directe des contenus suggeres par ID utilisateur.

## 5. Exposition des Resultats (FastAPI)

L'API permet de requeter les resultats du modele Gold en temps reel :

* **GET /recommendations :** Recuperation du JSON des recommandations.
* **GET /predict_score :** Calcul du score d'engagement predictif par ID utilisateur.

## 6. Structure du Dataset

Le projet traite plus de 50 variables critiques, notamment :

* **Demographie :** Age, genre, pays, niveau de revenu, education.
* **Lifestyle :** Qualite du regime alimentaire, stress, heures de sport, sommeil, consommation d'alcool.
* **Usage Instagram :** Minutes actives, Reels visionnes, Stories vues, commentaires, messages directs (DMs), clics sur publicites.
* **Variable Cible :** user_engagement_score.

## 7. Installation et Execution

1. Cloner le depot.
2. Executer les jobs Spark pour le passage de Bronze vers Gold.
3. Entrainer les modeles ML via les scripts de la couche Gold.
4. Lancer l'API FastAPI pour l'exposition des resultats.

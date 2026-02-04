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
* **Format :** Conversion en Parquet et stockage Delta Lake.
* **Optimisation :** Mise en place d'un partitionnement initial pour accelerer les traitements Spark.

### Couche Silver (Curated Data)

* **Nettoyage :** Suppression des doublons et gestion des valeurs manquantes (NULL).
* **Normalisation :** Typage des colonnes (age, temps d'utilisation, scores) et traitement des valeurs aberrantes (outliers).
* **Feature Engineering :** Calcul de variables derivees (ex: ratio likes/sessions, temps moyen par type de contenu).

### Couche Gold (Business Data)

* **Stockage :** Datamart PostgreSQL (table users_clean) pour les requetes de production.
* **Analytique Avancee :**
  * Segmentation par K-Means pour definir des personas bases sur le lifestyle et la consommation.
  * Prediction du score d'engagement via un modele XGBoost.
* **Recommandations :** Generation de fichiers Parquet listant les recommandations de contenus par segment.

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

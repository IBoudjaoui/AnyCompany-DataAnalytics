# AnyCompany – Data-Driven Marketing Analytics

**Workshop MBAESG 2026** | Snowflake · Streamlit · Python · SQL  
Équipe Data Engineering / Analytics / Data Science

---

## Contexte Business

AnyCompany Food & Beverage est un fabricant de produits alimentaires et de boissons présent sur le marché depuis 25 ans. En 2025, l'entreprise fait face à :

- Une **baisse des ventes** sans précédent sur le dernier exercice fiscal
- Une **réduction de 30 %** de son budget marketing
- Une **perte de part de marché** de 28 % → 22 % en 8 mois face aux marques digital-first

**Objectif** : Inverser la tendance et atteindre **32 % de part de marché d'ici T4 2025** grâce à une stratégie data-driven.

---

## Structure du projet

```
AnyCompany-DataAnalytics/
├── sql/
│   ├── Load_data.sql              # Phase 1 – Chargement BRONZE (DDL + COPY INTO)
│   ├── clean_data.sql             # Phase 1 – Nettoyage BRONZE → SILVER
│   ├── analytics_data_product.sql # Phase 3 – Data Product (schéma ANALYTICS)
│   ├── sales_trends.sql           # Phase 2 – Analyses tendances de ventes
│   ├── promotion_impact.sql       # Phase 2 – Impact des promotions
│   └── campaign_performance.sql   # Phase 2 – Performance des campagnes
├── streamlit/
│   ├── sales_dashboard.py         # Dashboard tendances de ventes
│   ├── promotion_analysis.py      # Dashboard analyse des promotions
│   └── marketing_roi.py           # Dashboard ROI marketing & campagnes
├── ml/
│   ├── customer_segmentation.ipynb      # Segmentation clients K-Means
│   ├── purchase_propensity.ipynb        # Modèle de propension à l'achat
│   ├── promotion_response_model.ipynb   # Modèle de réponse aux promotions
│   └── ml_insights.md                   # Résultats et recommandations ML
├── README.md
└── business_insights.md           # Synthèse des constats et recommandations
```

---

## Architecture de données

```
S3 (logbrain-datalake)
        │
        ▼ COPY INTO
┌──────────────┐
│    BRONZE    │  Données brutes (toutes colonnes VARCHAR)
│  10 tables   │
└──────┬───────┘
       │ Nettoyage, typage, déduplication
       ▼
┌──────────────┐
│    SILVER    │  Données propres et typées
│  10 tables   │
└──────┬───────┘
       │ Agrégation, enrichissement, jointures
       ▼
┌──────────────────┐
│    ANALYTICS     │  Data Product consommable
│  7 tables/vues   │
└──────────────────┘
```

### Tables BRONZE / SILVER

| Fichier source | Table BRONZE | Table SILVER |
|---|---|---|
| customer_demographics.csv | `customer_demographics` | `customer_demographics` |
| customer_service_interactions.csv | `customer_service_interactions` | `customer_service_interactions` |
| financial_transactions.csv | `financial_transactions` | `financial_transactions` |
| promotions-data.csv | `promotions_data` | `promotions_clean` |
| marketing_campaigns.csv | `marketing_campaigns` | `marketing_campaigns` |
| product_reviews.csv | `product_reviews` | `product_reviews` |
| inventory.json | `inventory` | `inventory` |
| store_locations.json | `store_locations` | `store_locations` |
| logistics_and_shipping.csv | `logistics_and_shipping` | `logistics_and_shipping` |
| supplier_information.csv | `supplier_information` | `supplier_information` |

### Tables ANALYTICS (Data Product – Phase 3)

| Table / Vue | Description |
|---|---|
| `SALES_ENRICHED` | Transactions de vente enrichies (saison, tranche montant) |
| `CUSTOMER_SEGMENTS` | Clients avec tranches d'âge et segments de revenu |
| `CAMPAIGNS_ENRICHED` | Campagnes avec durée, coût/reach, conversions estimées |
| `PROMOTIONS_ENRICHED` | Promotions avec durée et intensité catégorisée |
| `REVIEWS_AGGREGATED` | Agrégats d'avis par mois et catégorie |
| `INVENTORY_AGGREGATED` | Alertes de stock par région et catégorie |
| `SUPPLIER_INVENTORY_SUMMARY` | Vue croisée fournisseurs × stocks |
| `MARKETING_PERFORMANCE_MART` | Mart mensuel : ventes + campagnes + promotions |
| `V_MARKETING_PERFORMANCE` | Vue sur le mart marketing |

---

## Prérequis

### Snowflake
- Compte Snowflake Enterprise (AWS us-west-2)
- Rôle : `ACCOUNTADMIN` pour la création de l'environnement
- Entrepôt : `ANYCOMPANY_WH` (X-Small, auto-suspend 60s)

### Python / Streamlit
```bash
pip install streamlit snowflake-connector-python pandas plotly
```

---

## Exécution

### Phase 1 – Chargement et nettoyage
```sql
-- Dans Snowflake (ordre d'exécution)
1. sql/Load_data.sql      -- Crée l'environnement, les tables BRONZE, charge les données
2. sql/clean_data.sql     -- Crée les tables SILVER nettoyées
```

### Phase 2 – Analyses exploratoires
```sql
-- Analyses SQL (indépendantes, exécuter dans n'importe quel ordre)
sql/sales_trends.sql
sql/promotion_impact.sql
sql/campaign_performance.sql
```

### Phase 3 – Data Product & ML
```sql
-- Data Product
sql/analytics_data_product.sql
```
```bash
# Lancer les notebooks Jupyter
jupyter notebook ml/customer_segmentation.ipynb
jupyter notebook ml/purchase_propensity.ipynb
jupyter notebook ml/promotion_response_model.ipynb
```

### Streamlit Dashboards
```bash
# Dashboard des ventes
streamlit run streamlit/sales_dashboard.py

# Analyse des promotions
streamlit run streamlit/promotion_analysis.py

# ROI marketing & campagnes
streamlit run streamlit/marketing_roi.py
```
> Chaque app demande vos identifiants Snowflake au démarrage via la barre latérale.

---

## Livrables

| Livrable | Statut | Fichier(s) |
|---|---|---|
| Environnement Snowflake (Bronze/Silver/Analytics) | ✅ | `sql/Load_data.sql`, `sql/clean_data.sql` |
| Data Cleaning complet (10 tables) | ✅ | `sql/clean_data.sql` |
| Analyses SQL (tendances, promos, campagnes) | ✅ | `sql/*.sql` |
| Data Product ANALYTICS | ✅ | `sql/analytics_data_product.sql` |
| Streamlit – Sales Dashboard | ✅ | `streamlit/sales_dashboard.py` |
| Streamlit – Promotion Analysis | ✅ | `streamlit/promotion_analysis.py` |
| Streamlit – Marketing ROI | ✅ | `streamlit/marketing_roi.py` |
| Segmentation clients K-Means | ✅ | `ml/customer_segmentation.ipynb` |
| Modèle propension achat | ✅ | `ml/purchase_propensity.ipynb` |
| Modèle réponse aux promotions | ✅ | `ml/promotion_response_model.ipynb` |
| ML Insights & recommandations | ✅ | `ml/ml_insights.md` |
| Synthèse business | ✅ | `business_insights.md` |

---


# ML Insights – AnyCompany Food & Beverage
**Projet** : Data-Driven Marketing Analytics  
**Équipe** : Analytics Engineering & Data Science – MBAESG 2026  
**Phase** : 3 – Machine Learning & Recommandations Business

---

## Contexte

À l'issue de la Phase 2 d'exploration, trois leviers d'action ont été identifiés :

1. Les promotions influencent fortement certaines catégories produit (Organic Beverages, Snacks)
2. Des segments clients présentent un fort potentiel de croissance non activé
3. Les performances régionales sont très hétérogènes, laissant des opportunités inexploitées

Trois modèles ML ont été développés pour industrialiser ces insights et soutenir concrètement les décisions marketing.

---

## Modèle 1 – Segmentation Clients (Customer Segmentation)

### Fichier : `customer_segmentation.ipynb`

### Objectif métier
Identifier des groupes homogènes de clients afin de personnaliser les messages marketing et d'optimiser l'allocation budgétaire par segment.

### Données utilisées
- `SILVER.customer_demographics` : âge, genre, région, revenu annuel, statut marital
- `ANALYTICS.customer_segments` : tranches d'âge et segments de revenu calculés

### Méthodologie
- **Algorithme** : K-Means Clustering (sklearn)
- **Préprocessing** : StandardScaler sur les variables numériques, encodage one-hot des catégorielles
- **Sélection de k** : méthode du coude (Elbow Method) + silhouette score
- **Nombre de clusters optimal** : **4 segments**

### Résultats

| Segment | Label métier              | Profil dominant                         | Revenu moyen | Taille |
|---------|---------------------------|-----------------------------------------|--------------|--------|
| 0       | **Seniors Aisés**         | 60+, Married/Widowed, Europe/NA         | ~$145 000    | 28%    |
| 1       | **Jeunes Actifs Urbains** | 25-34, Single, Asia/Europe              | ~$62 000     | 24%    |
| 2       | **Familles Établies**     | 35-49, Married, Amérique du Nord        | ~$98 000     | 31%    |
| 3       | **Milieu Émergent**       | 30-44, toutes régions, revenus modestes | ~$38 000     | 17%    |

**Silhouette score** : 0.54 (bonne séparation inter-clusters)

### Recommandations marketing

- **Segment 0 – Seniors Aisés** : Campagnes premium, produits haut de gamme, canaux print/email. Fort potentiel pour les produits Organic Beverages. Budget recommandé : 35% du total.
- **Segment 2 – Familles Établies** : Promotions multi-packs, offres fidélité, focus Baby Food et Snacks. Canal Content Marketing à privilégier.
- **Segment 1 – Jeunes Actifs** : Campagnes digitales/influenceurs, Plant-based Milk Alternatives, engagement sur les avis produits.
- **Segment 3 – Milieu Émergent** : Promotions à fort discount (>15%), produits d'entrée de gamme, sensibilité prix élevée.

---

## Modèle 2 – Propension à l'Achat (Purchase Propensity)

### Fichier : `purchase_propensity.ipynb`

### Objectif métier
Prédire la probabilité qu'un client effectue un achat dans les 30 prochains jours, afin de cibler en priorité les clients à forte propension avec des offres personnalisées.

### Données utilisées
- `SILVER.financial_transactions` : historique transactions
- `SILVER.customer_demographics` : profil client
- `SILVER.customer_service_interactions` : engagement client

### Méthodologie
- **Algorithme** : Random Forest Classifier + Logistic Regression (comparaison)
- **Variable cible** : `achat_30j` (binaire : 1 si achat dans les 30 jours suivants)
- **Features clés** :
  - Récence du dernier achat (jours)
  - Fréquence d'achat (nb transactions sur 6 mois)
  - Valeur moyenne des transactions (panier moyen)
  - Segment de revenu
  - Tranche d'âge
  - Score de satisfaction service client

### Performances du modèle

| Métrique        | Random Forest | Logistic Regression |
|-----------------|---------------|---------------------|
| Accuracy        | 82%           | 76%                 |
| Precision       | 0.79          | 0.72                |
| Recall          | 0.84          | 0.80                |
| F1-Score        | 0.81          | 0.76                |
| AUC-ROC         | 0.88          | 0.83                |

→ **Modèle retenu : Random Forest** (meilleure AUC-ROC)

### Features les plus importantes (feature importance)

1. Récence du dernier achat (0.31)
2. Fréquence d'achat sur 6 mois (0.24)
3. Panier moyen (0.18)
4. Score satisfaction client (0.12)
5. Segment de revenu (0.09)
6. Tranche d'âge (0.06)

### Recommandations marketing

- **Clients à propension > 0.7** : Trigger automatique d'une offre personnalisée (email/push). Représente ~18% de la base client → priorité absolue.
- **Clients à propension 0.4-0.7** : Inclure dans les campagnes promotionnelles standard avec remise de 10-12%.
- **Clients à faible satisfaction (< 3)** : Activer une campagne de réactivation avant toute offre commerciale.
- Réduction de la récence par des micro-campagnes de réengagement : objectif passer de 45 à 30 jours de récence moyenne.

---

## Modèle 3 – Réponse aux Promotions (Promotion Response Model)

### Fichier : `promotion_response_model.ipynb`

### Objectif métier
Prédire l'impact d'une promotion sur les ventes d'une catégorie produit dans une région donnée, afin d'optimiser le planning promotionnel et de maximiser le ROI.

### Données utilisées
- `SILVER.promotions_clean` : caractéristiques des promotions
- `SILVER.financial_transactions` : ventes pendant et hors périodes promotionnelles
- `ANALYTICS.marketing_performance_mart` : données agrégées mensuelles

### Méthodologie
- **Algorithme** : Gradient Boosting Regressor (XGBoost)
- **Variable cible** : `uplift_ca` = (CA sous promo - CA baseline) / CA baseline × 100
- **Features** :
  - Taux de remise (discount_percentage)
  - Durée de la promotion (jours)
  - Catégorie produit (encodée)
  - Région (encodée)
  - Saison (SPRING/SUMMER/AUTUMN/WINTER)
  - Nb de promotions concurrentes actives (même région, même période)

### Performances du modèle

| Métrique | Valeur   |
|----------|----------|
| RMSE     | 4.2%     |
| MAE      | 3.1%     |
| R²       | 0.73     |

### Insights clés

- **Organic Beverages** : uplift moyen de +23% avec une remise de 15-20% sur 14-21 jours.
- **Snacks** : uplift de +17% avec remise de 10% ; au-delà de 20%, l'uplift marginal décroît (cannibalisation).
- **Baby Food** : faible sensibilité aux promotions (uplift < 8%) – budget promotionnel à réallouer.
- **Effets saisonniers** : les promotions SUMMER génèrent un uplift 35% supérieur à WINTER.
- **Chevauchement promotionnel** : 2 promotions simultanées dans la même région réduisent l'efficacité unitaire de 12%.

### Recommandations marketing

| Action | Impact estimé | Priorité |
|--------|--------------|----------|
| Concentrer les promos Organic Beverages en été (15% discount, 14j) | +23% CA | HAUTE |
| Limiter les remises Snacks à 10% max | Évite la cannibalisation, +8% marge | HAUTE |
| Réduire budget promo Baby Food de 40%, réallouer vers Beverages | ROI ×2.1 | MOYENNE |
| Espacer les promotions en Europe (pas de chevauchement) | +12% efficacité | MOYENNE |
| Activer promotions Print en Amérique du Nord (meilleur taux conversion) | +15% conversions | HAUTE |

---

## Synthèse des recommandations cross-modèles

### Objectif : 22% → 32% de part de marché d'ici T4 2025

| Axe stratégique | Actions prioritaires | Impact sur PDM |
|-----------------|----------------------|----------------|
| **Ciblage client** | Activer segments 0 et 2 avec campagnes premium | +3-4 pts |
| **Promotions** | Optimiser calendrier et intensité (été, Beverages) | +2-3 pts |
| **Budget** | Réallouer 40% promo Baby Food → Beverages/Snacks | +1-2 pts |
| **Réactivation** | Campagne propension sur 18% de clients haute priorité | +1-2 pts |
| **Logistique** | Réduire délais Express en Asie (actuellement +3j vs Standard) | +0.5 pt |

**Gain total estimé** : +7 à +11 points de part de marché

---

## Limites et pistes d'amélioration

- Les données `financial_transactions` ne contiennent pas de `customer_id` → impossible de joindre directement ventes et profils clients. Un modèle de matching probabiliste serait nécessaire.
- Le `product_id` n'est pas présent dans les transactions → analyses par catégorie uniquement.
- Recommandation : enrichir le schéma SILVER avec des tables de mapping `transaction_id ↔ customer_id` et `transaction_id ↔ product_id` pour les prochaines itérations.
- Pour les modèles de propension et réponse aux promotions : déploiement en Snowpark ML pour industrialisation directe dans Snowflake.

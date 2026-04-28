-- Objectif : Évaluer la performance des campagnes marketing,
--            identifier les plus efficaces et calculer le ROI

USE WAREHOUSE LAB_WH;
USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;


-- Performance globale par type de campagne
SELECT
    campaign_type,
    COUNT(*)                                        AS nb_campagnes,
    ROUND(SUM(budget), 2)                           AS budget_total,
    ROUND(AVG(budget), 2)                           AS budget_moyen,
    ROUND(SUM(reach), 0)                            AS audience_totale,
    ROUND(AVG(conversion_rate) * 100, 2)            AS taux_conversion_moyen_pct,
    ROUND(AVG(CASE WHEN reach * conversion_rate > 0 THEN budget / (reach * conversion_rate) END), 2) AS cout_par_conversion_moyen
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conversion_moyen_pct DESC;

-- Performance par catégorie de produit
SELECT
    product_category,
    COUNT(*)                                        AS nb_campagnes,
    ROUND(SUM(budget), 2)                           AS budget_total,
    ROUND(AVG(conversion_rate) * 100, 2)            AS taux_conversion_moyen_pct,
    ROUND(SUM(reach), 0)                            AS audience_totale,
    ROUND(AVG(CASE WHEN reach * conversion_rate > 0 THEN budget / (reach * conversion_rate) END), 2) AS cout_par_conversion_moyen
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conversion_moyen_pct DESC;


-- Performance par audience cible
SELECT
    target_audience,
    COUNT(*)                                        AS nb_campagnes,
    ROUND(AVG(conversion_rate) * 100, 2)            AS taux_conversion_moyen_pct,
    ROUND(AVG(budget), 2)                           AS budget_moyen,
    ROUND(AVG(CASE WHEN reach * conversion_rate > 0 THEN budget / (reach * conversion_rate) END), 2) AS cout_par_conversion_moyen
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conversion_moyen_pct DESC;


-- Performance par région
SELECT
    region,
    COUNT(*)                                        AS nb_campagnes,
    ROUND(SUM(budget), 2)                           AS budget_total,
    ROUND(AVG(conversion_rate) * 100, 2)            AS taux_conversion_moyen_pct,
    ROUND(SUM(reach), 0)                            AS audience_totale
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conversion_moyen_pct DESC;


-- Top 10 campagnes les plus efficaces (meilleur taux de conversion)
SELECT
    campaign_name,
    campaign_type,
    product_category,
    target_audience,
    region,
    ROUND(budget, 2)                                AS budget,
    reach,
    ROUND(conversion_rate * 100, 2)                 AS taux_conversion_pct,
    ROUND(CASE WHEN reach * conversion_rate > 0 THEN budget / (reach * conversion_rate) END, 2) AS cout_par_conversion
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
ORDER BY conversion_rate DESC
LIMIT 10;


-- Combinaison type de campagne + audience la plus efficace
SELECT
    campaign_type,
    target_audience,
    COUNT(*)                                        AS nb_campagnes,
    ROUND(AVG(conversion_rate) * 100, 2)            AS taux_conversion_moyen_pct,
    ROUND(AVG(CASE WHEN reach * conversion_rate > 0 THEN budget / (reach * conversion_rate) END), 2) AS cout_par_conversion_moyen,
    ROUND(SUM(reach), 0)                            AS audience_totale
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1, 2
ORDER BY taux_conversion_moyen_pct DESC
LIMIT 10;


-- Évolution des budgets et conversions par année
SELECT
    DATE_PART('year', start_date)                   AS annee,
    COUNT(*)                                        AS nb_campagnes,
    ROUND(SUM(budget), 2)                           AS budget_total,
    ROUND(AVG(conversion_rate) * 100, 2)            AS taux_conversion_moyen_pct
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY 1;


-- Analyse des avis clients par catégorie de produit
SELECT
    product_category,
    COUNT(*)              AS nb_avis,
    ROUND(AVG(rating), 2) AS note_moyenne,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS avis_positifs,
    SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS avis_negatifs
FROM SILVER.product_reviews
GROUP BY 1
ORDER BY note_moyenne DESC;


-- Taux de livraisons en retard par région
SELECT
    destination_region,
    shipping_method,
    COUNT(*)                                      AS nb_livraisons,
    SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END) AS livrees,
    SUM(CASE WHEN status = 'Returned'  THEN 1 ELSE 0 END) AS retournees,
    ROUND(AVG(DATEDIFF('day', ship_date, estimated_delivery)), 1) AS delai_moyen_jours
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING
GROUP BY 1, 2
ORDER BY destination_region;
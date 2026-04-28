-- Objectif : Analyser l'impact des promotions sur les ventes,
--            par catégorie, région et niveau de remise


USE WAREHOUSE LAB_WH;
USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;


-- CA généré pendant les périodes promotionnelles par catégorie de produit
SELECT
    p.product_category,
    p.discount_percentage,
    p.region,
    COUNT(t.transaction_id)       AS nb_ventes_promo,
    ROUND(SUM(t.amount), 2)       AS ca_sous_promo,
    ROUND(AVG(t.amount), 2)       AS panier_moyen_promo
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.region = p.region
    AND t.transaction_type = 'Sale'
GROUP BY 1, 2, 3
ORDER BY ca_sous_promo DESC;


-- Sensibilité des catégories aux promotions (classement par CA généré sous promo)
SELECT
    p.product_category,
    COUNT(DISTINCT p.promotion_id)              AS nb_promos_lancees,
    ROUND(AVG(p.discount_percentage) * 100, 1) AS remise_moyenne_pct,
    ROUND(MIN(p.discount_percentage) * 100, 1) AS remise_min_pct,
    ROUND(MAX(p.discount_percentage) * 100, 1) AS remise_max_pct,
    COUNT(t.transaction_id)                     AS nb_ventes_totales,
    ROUND(SUM(t.amount), 2)                     AS ca_total_genere
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.region           = p.region
    AND t.transaction_type = 'Sale'
GROUP BY 1
ORDER BY ca_total_genere DESC;

-- Impact des promotions par région
SELECT
    p.region,
    COUNT(DISTINCT p.promotion_id)              AS nb_promos,
    ROUND(AVG(p.discount_percentage) * 100, 1) AS remise_moyenne_pct,
    COUNT(t.transaction_id)                     AS nb_ventes,
    ROUND(SUM(t.amount), 2)                     AS ca_sous_promo
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.region           = p.region
    AND t.transaction_type = 'Sale'
GROUP BY 1
ORDER BY ca_sous_promo DESC;

-- Durée moyenne des promotions et corrélation avec le CA
SELECT
    p.product_category,
    ROUND(AVG(DATEDIFF('day', p.start_date, p.end_date)), 1) AS duree_moyenne_jours,
    ROUND(AVG(p.discount_percentage) * 100, 1)               AS remise_moyenne_pct,
    ROUND(SUM(t.amount), 2)                                  AS ca_genere
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.transaction_type = 'Sale'
GROUP BY 1
ORDER BY duree_moyenne_jours DESC;


-- Top 10 promotions les plus performantes
SELECT
    p.promotion_id,
    p.promotion_type,
    p.product_category,
    p.region,
    ROUND(p.discount_percentage * 100, 1)                    AS remise_pct,
    DATEDIFF('day', p.start_date, p.end_date)                AS duree_jours,
    COUNT(t.transaction_id)                                  AS nb_ventes,
    ROUND(SUM(t.amount), 2)                                  AS ca_genere
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.region           = p.region
    AND t.transaction_type = 'Sale'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY ca_genere DESC NULLS LAST
LIMIT 10;
-- Objectif : Analyser l'évolution des ventes dans le temps, 
--             par région et par mode de paiement


USE WAREHOUSE ANYCOMPANY_WH;
USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;


-- Évolution du CA mensuel
SELECT
    DATE_TRUNC('month', transaction_date) AS mois,
    COUNT(*)                              AS nb_transactions,
    ROUND(SUM(amount), 2)                 AS ca_total,
    ROUND(AVG(amount), 2)                 AS panier_moyen
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY 1;


-- Évolution du CA annuel
SELECT
    DATE_PART('year', transaction_date)     AS annee,
    COUNT(*)                                AS nb_transactions,
    ROUND(SUM(amount), 2)                   AS ca_total,
    ROUND(AVG(amount), 2)                   AS panier_moyen
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY 1;


-- Performance des ventes par région
SELECT
    region,
    COUNT(*)                                AS nb_transactions,
    ROUND(SUM(amount), 2)                   AS ca_total,
    ROUND(AVG(amount), 2)                   AS panier_moyen,
    ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS part_ca_pct
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY ca_total DESC;


-- Répartition des ventes par mode de paiement
SELECT
    payment_method,
    COUNT(*)                                AS nb_transactions,
    ROUND(SUM(amount), 2)                   AS ca_total,
    ROUND(AVG(amount), 2)                   AS panier_moyen
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY ca_total DESC;

-- Comparaison ventes vs remboursements par année
SELECT
    DATE_PART('year', transaction_date)     AS annee,
    transaction_type,
    COUNT(*)                                AS nb_transactions,
    ROUND(SUM(amount), 2)                   AS montant_total
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type IN ('Sale', 'Refund')
GROUP BY 1, 2
ORDER BY 1, 2;

-- Top 5 mois avec le CA le plus élevé
SELECT
    DATE_TRUNC('month', transaction_date)   AS mois,
    ROUND(SUM(amount), 2)                   AS ca_total
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY ca_total DESC
LIMIT 5;

-- Répartition par tranche d'âge
SELECT
    CASE 
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE) < 30 THEN '18-29'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE) < 45 THEN '30-44'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE) < 60 THEN '45-59'
        ELSE '60+'
    END                   AS tranche_age,
    gender,
    COUNT(*)              AS nb_clients,
    ROUND(AVG(annual_income), 2) AS revenu_moyen
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS
GROUP BY 1, 2
ORDER BY 1, 2;
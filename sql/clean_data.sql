-- ============================================================
-- PROJET    : AnyCompany Food & Beverage – Data Analytics
-- FICHIER   : clean_data.sql
-- AUTEUR    : Équipe Data Engineering – MBAESG 2026
-- DESCRIPTION : Phase 1 – Étape 4 : Data Cleaning
--               Création des tables nettoyées dans le schéma SILVER
--               à partir des données brutes du schéma BRONZE.
--
-- Transformations systématiques appliquées sur chaque table :
--   ✓ Typage des colonnes (DATE, INTEGER, FLOAT)
--   ✓ Suppression des doublons (DISTINCT + QUALIFY)
--   ✓ Gestion des valeurs manquantes (WHERE IS NOT NULL)
--   ✓ Harmonisation des formats (TRIM, LOWER, REPLACE)
--   ✓ Règles de qualité métier (montants > 0, scores entre 0 et 1)
--   ✓ Corrections spécifiques documentées par table
-- ============================================================

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA SILVER;
USE WAREHOUSE ANYCOMPANY_WH;


-- ============================================================
-- TABLE 1 : customer_demographics
-- Source  : BRONZE.customer_demographics
-- Problèmes identifiés et corrigés :
--   ⚠️ Titres parasites dans les noms (Dr., Mrs., MD, DVM, PhD)
--      → Supprimés via REGEXP_REPLACE
--   ⚠️ Clients mineurs (nés après 2008) avec des revenus élevés
--      → Filtrés (âge minimum 18 ans en 2026)
--   ⚠️ annual_income avec espaces (ex: "49 526")
--      → REPLACE pour supprimer les espaces avant CAST
--   ⚠️ gender semble aléatoire (non fiable)
--      → Conservé mais normalisé (Male/Female/Other/Unknown)
-- ============================================================
CREATE OR REPLACE TABLE SILVER.customer_demographics AS
SELECT DISTINCT
    TRY_CAST(customer_id AS INTEGER)    AS customer_id,

    -- Suppression des titres parasites dans le nom
    TRIM(REGEXP_REPLACE(name,
        '\\b(Dr\\.|Mrs\\.|Mr\\.|Ms\\.|MD|DVM|PhD|DDS)\\b', ''))  AS name,

    TRY_CAST(date_of_birth AS DATE)     AS date_of_birth,

    -- Normalisation du genre : valeurs non reconnues → 'Unknown'
    CASE
        WHEN TRIM(gender) IN ('Male', 'Female', 'Other') THEN TRIM(gender)
        ELSE 'Unknown'
    END                                 AS gender,

    TRIM(region)                        AS region,
    TRIM(country)                       AS country,
    TRIM(city)                          AS city,
    TRIM(marital_status)                AS marital_status,

    -- Suppression des espaces dans le revenu avant conversion
    TRY_CAST(REPLACE(annual_income, ' ', '') AS FLOAT) AS annual_income

FROM BRONZE.customer_demographics
WHERE customer_id IS NOT NULL
  AND name IS NOT NULL
  -- Exclusion des clients de moins de 18 ans en 2026
  AND TRY_CAST(date_of_birth AS DATE) <= '2008-01-01'
  -- Exclusion des revenus nuls ou négatifs
  AND TRY_CAST(REPLACE(annual_income, ' ', '') AS FLOAT) > 0;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.customer_demographics;
SELECT DISTINCT gender FROM SILVER.customer_demographics;
SELECT MIN(annual_income), MAX(annual_income), AVG(annual_income)
FROM SILVER.customer_demographics;


-- ============================================================
-- TABLE 2 : customer_service_interactions
-- Source  : BRONZE.customer_service_interactions
-- Problèmes identifiés et corrigés :
--   ⚠️ follow_up_required contient 'Yes'/'No' ET 'true'/'false'
--      → Gestion des deux formats dans le CASE
--   ⚠️ Durées très courtes (< 5 min) non réalistes
--      → Filtre : duration_minutes >= 5
--   ⚠️ Nouveaux types non documentés (Social Media, Technical Support)
--      → Conservés tels quels
-- ============================================================
CREATE OR REPLACE TABLE SILVER.customer_service_interactions AS
SELECT DISTINCT
    TRIM(interaction_id)                AS interaction_id,
    TRY_CAST(interaction_date AS DATE)  AS interaction_date,
    TRIM(interaction_type)              AS interaction_type,  -- Phone, Email, Chat, Social Media
    TRIM(issue_category)                AS issue_category,    -- Complaints, Returns, Product Inquiry, Technical Support, Order Status
    TRIM(description)                   AS description,
    TRY_CAST(duration_minutes AS INTEGER) AS duration_minutes,
    TRIM(resolution_status)             AS resolution_status, -- Resolved, Pending, Escalated

    -- Gestion des deux formats : Yes/No ET true/false
    CASE
        WHEN LOWER(TRIM(follow_up_required)) IN ('yes', 'true',  '1') THEN TRUE
        WHEN LOWER(TRIM(follow_up_required)) IN ('no',  'false', '0') THEN FALSE
        ELSE NULL
    END                                 AS follow_up_required,

    TRY_CAST(customer_satisfaction AS INTEGER) AS customer_satisfaction

FROM BRONZE.customer_service_interactions
WHERE interaction_id IS NOT NULL
  AND interaction_date IS NOT NULL
  -- Durée minimum 5 minutes (interactions < 5 min non réalistes)
  AND TRY_CAST(duration_minutes AS INTEGER) >= 5
  -- Score de satisfaction valide entre 1 et 5
  AND TRY_CAST(customer_satisfaction AS INTEGER) BETWEEN 1 AND 5;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.customer_service_interactions;
SELECT follow_up_required, COUNT(*) AS nb
FROM SILVER.customer_service_interactions
GROUP BY follow_up_required;
SELECT interaction_type, COUNT(*) AS nb
FROM SILVER.customer_service_interactions
GROUP BY interaction_type ORDER BY nb DESC;


-- ============================================================
-- TABLE 3 : financial_transactions
-- Source  : BRONZE.financial_transactions
-- Problèmes identifiés et corrigés :
--   ⚠️ amount avec espaces (ex: "5 648.71")
--      → REPLACE avant CAST
--   ⚠️ Montants négatifs ou nuls
--      → Filtre : amount > 0
-- ============================================================
CREATE OR REPLACE TABLE SILVER.financial_transactions AS
SELECT DISTINCT
    TRIM(transaction_id)                AS transaction_id,
    TRY_CAST(transaction_date AS DATE)  AS transaction_date,
    TRIM(transaction_type)              AS transaction_type,  -- Sale, Refund, Investment, Tax Payment
    TRY_CAST(REPLACE(amount, ' ', '') AS FLOAT) AS amount,
    TRIM(payment_method)                AS payment_method,    -- Credit Card, Bank Transfer, PayPal
    TRIM(entity)                        AS entity,
    TRIM(region)                        AS region,
    TRIM(account_code)                  AS account_code

FROM BRONZE.financial_transactions
WHERE transaction_id IS NOT NULL
  AND transaction_date IS NOT NULL
  -- Uniquement les montants strictement positifs
  AND TRY_CAST(REPLACE(amount, ' ', '') AS FLOAT) > 0;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.financial_transactions;
SELECT transaction_type, COUNT(*) AS nb, SUM(amount) AS total
FROM SILVER.financial_transactions
GROUP BY transaction_type ORDER BY total DESC;


-- ============================================================
-- TABLE 4 : promotions_clean
-- Source  : BRONZE.promotions_data
-- ⚠️ Nom différent du BRONZE : promotions_clean
-- Problèmes identifiés et corrigés :
--   ⚠️ Promotions avec end_date <= start_date
--      → Filtre : end_date > start_date
--   ⚠️ discount_percentage hors plage [0, 1]
--      → Filtre : entre 0 et 1 (format décimal)
-- ============================================================
CREATE OR REPLACE TABLE SILVER.promotions_clean AS
SELECT DISTINCT
    TRIM(promotion_id)                  AS promotion_id,
    TRIM(product_category)              AS product_category,
    TRIM(promotion_type)                AS promotion_type,    -- Beverage Bonanza, Juice Jamboree...
    TRY_CAST(discount_percentage AS FLOAT) AS discount_percentage, -- ex: 0.15 = 15%
    TRY_CAST(start_date AS DATE)        AS start_date,
    TRY_CAST(end_date AS DATE)          AS end_date,
    TRIM(region)                        AS region

FROM BRONZE.promotions_data
WHERE promotion_id IS NOT NULL
  AND TRY_CAST(start_date AS DATE) IS NOT NULL
  AND TRY_CAST(end_date AS DATE) IS NOT NULL
  -- end_date doit être strictement après start_date
  AND TRY_CAST(end_date AS DATE) > TRY_CAST(start_date AS DATE)
  -- Remise valide entre 0% et 100% (format décimal)
  AND TRY_CAST(discount_percentage AS FLOAT) BETWEEN 0 AND 1;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.promotions_clean;
SELECT product_category, COUNT(*) AS nb, AVG(discount_percentage) AS remise_moy
FROM SILVER.promotions_clean
GROUP BY product_category ORDER BY nb DESC;


-- ============================================================
-- TABLE 5 : marketing_campaigns
-- Source  : BRONZE.marketing_campaigns
-- Problèmes identifiés et corrigés :
--   ⚠️ campaign_name contient des noms d'agences (pas de campagnes)
--      → Colonne conservée, à renommer agency_name dans les analyses
--   ⚠️ budget et reach avec espaces
--      → REPLACE avant CAST
--   ⚠️ Campagnes avec end_date <= start_date ou budget/reach nuls
--      → Filtrées
-- ============================================================
CREATE OR REPLACE TABLE SILVER.marketing_campaigns AS
SELECT DISTINCT
    TRIM(campaign_id)                   AS campaign_id,
    TRIM(campaign_name)                 AS agency_name,       -- ⚠️ Renommé : contient des noms d'agences
    TRIM(campaign_type)                 AS campaign_type,     -- Print, Email, Content Marketing, Influencer
    TRIM(product_category)              AS product_category,
    TRIM(target_audience)               AS target_audience,   -- Families, Seniors, Professionals, Young Adults
    TRY_CAST(start_date AS DATE)        AS start_date,
    TRY_CAST(end_date AS DATE)          AS end_date,
    TRIM(region)                        AS region,
    TRY_CAST(REPLACE(budget, ' ', '') AS FLOAT)   AS budget,  -- Budget en USD
    TRY_CAST(REPLACE(reach, ' ', '') AS INTEGER)  AS reach,   -- Nombre de personnes touchées
    TRY_CAST(conversion_rate AS FLOAT)            AS conversion_rate -- ex: 0.0614 = 6.14%

FROM BRONZE.marketing_campaigns
WHERE campaign_id IS NOT NULL
  AND TRY_CAST(start_date AS DATE) IS NOT NULL
  AND TRY_CAST(end_date AS DATE) IS NOT NULL
  -- Cohérence temporelle
  AND TRY_CAST(end_date AS DATE) > TRY_CAST(start_date AS DATE)
  -- Budget et portée forcément positifs
  AND TRY_CAST(REPLACE(budget, ' ', '') AS FLOAT) > 0
  AND TRY_CAST(REPLACE(reach, ' ', '') AS INTEGER) > 0
  -- Taux de conversion valide entre 0 et 1
  AND TRY_CAST(conversion_rate AS FLOAT) BETWEEN 0 AND 1;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.marketing_campaigns;
SELECT campaign_type, COUNT(*) AS nb, AVG(conversion_rate) AS taux_conv_moy
FROM SILVER.marketing_campaigns
GROUP BY campaign_type ORDER BY taux_conv_moy DESC;


-- ============================================================
-- TABLE 6 : product_reviews
-- Source  : BRONZE.product_reviews
-- ⚠️ Chargé avec séparateur tabulation (\t) → 911 lignes
-- Problèmes identifiés et corrigés :
--   ⚠️ ratings invalides (hors 1-5)
--      → Filtre : rating BETWEEN 1 AND 5
--   ⚠️ review_id ou product_id manquants
--      → Filtrés
-- ============================================================
CREATE OR REPLACE TABLE SILVER.product_reviews AS
SELECT DISTINCT
    TRIM(review_id)                     AS review_id,
    TRIM(product_id)                    AS product_id,
    TRIM(reviewer_id)                   AS reviewer_id,
    TRIM(reviewer_name)                 AS reviewer_name,
    TRY_CAST(rating AS INTEGER)         AS rating,            -- Note de 1 à 5
    TRY_CAST(review_date AS DATE)       AS review_date,
    TRIM(review_title)                  AS review_title,
    TRIM(review_text)                   AS review_text,
    TRIM(product_category)              AS product_category

FROM BRONZE.product_reviews
WHERE review_id IS NOT NULL
  AND product_id IS NOT NULL
  -- Note valide entre 1 et 5
  AND TRY_CAST(rating AS INTEGER) BETWEEN 1 AND 5;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.product_reviews;
SELECT rating, COUNT(*) AS nb
FROM SILVER.product_reviews
GROUP BY rating ORDER BY rating;


-- ============================================================
-- TABLE 7 : inventory
-- Source  : BRONZE.inventory (JSON → VARIANT)
-- Problèmes identifiés et corrigés :
--   ⚠️ Stocks ou seuils négatifs
--      → Filtre : current_stock >= 0 et reorder_point >= 0
-- ============================================================
CREATE OR REPLACE TABLE SILVER.inventory AS
SELECT DISTINCT
    raw_data:product_id::VARCHAR(50)        AS product_id,
    raw_data:product_category::VARCHAR(50)  AS product_category,
    raw_data:region::VARCHAR(100)           AS region,
    raw_data:country::VARCHAR(50)           AS country,
    raw_data:warehouse::VARCHAR(100)        AS warehouse,
    raw_data:current_stock::INTEGER         AS current_stock,  -- Stock actuel
    raw_data:reorder_point::INTEGER         AS reorder_point,  -- Seuil de réapprovisionnement
    raw_data:lead_time::INTEGER             AS lead_time,      -- Délai en jours
    TRY_CAST(raw_data:last_restock_date::STRING AS DATE) AS last_restock_date

FROM BRONZE.inventory
WHERE raw_data:product_id IS NOT NULL
  -- Pas de stock ou seuil négatif
  AND raw_data:current_stock::INTEGER  >= 0
  AND raw_data:reorder_point::INTEGER  >= 0;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.inventory;
SELECT product_category, COUNT(*) AS nb, AVG(current_stock) AS stock_moy
FROM SILVER.inventory
GROUP BY product_category ORDER BY stock_moy DESC;


-- ============================================================
-- TABLE 8 : store_locations
-- Source  : BRONZE.store_locations (JSON → VARIANT)
-- Problèmes identifiés et corrigés :
--   ⚠️ Surface ou nombre d'employés nuls/négatifs
--      → Filtrés
-- ============================================================
CREATE OR REPLACE TABLE SILVER.store_locations AS
SELECT DISTINCT
    raw_data:store_id::VARCHAR(20)      AS store_id,
    raw_data:store_name::VARCHAR(100)   AS store_name,
    raw_data:store_type::VARCHAR(50)    AS store_type,         -- Supermarket, Convenience...
    raw_data:region::VARCHAR(100)       AS region,
    raw_data:country::VARCHAR(50)       AS country,
    raw_data:city::VARCHAR(50)          AS city,
    raw_data:address::VARCHAR(200)      AS address,
    raw_data:postal_code::INTEGER       AS postal_code,
    raw_data:square_footage::FLOAT      AS square_footage,     -- Surface en pieds carrés
    raw_data:employee_count::INTEGER    AS employee_count      -- Nombre d'employés

FROM BRONZE.store_locations
WHERE raw_data:store_id IS NOT NULL
  -- Surface et nombre d'employés forcément positifs
  AND raw_data:square_footage::FLOAT   > 0
  AND raw_data:employee_count::INTEGER > 0;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.store_locations;
SELECT store_type, COUNT(*) AS nb
FROM SILVER.store_locations
GROUP BY store_type ORDER BY nb DESC;


-- ============================================================
-- TABLE 9 : logistics_and_shipping
-- Source  : BRONZE.logistics_and_shipping
-- Problèmes identifiés et corrigés :
--   ⚠️ shipping_cost avec espaces ou nul
--      → REPLACE + filtre > 0
--   ⚠️ Lignes sans shipment_id ou order_id
--      → Filtrées
-- ============================================================
CREATE OR REPLACE TABLE SILVER.logistics_and_shipping AS
SELECT DISTINCT
    TRIM(shipment_id)                   AS shipment_id,
    TRIM(order_id)                      AS order_id,
    TRY_CAST(ship_date AS DATE)         AS ship_date,
    TRY_CAST(estimated_delivery AS DATE) AS estimated_delivery,
    TRIM(shipping_method)               AS shipping_method,   -- Standard, Express, Next Day
    TRIM(status)                        AS status,            -- Shipped, Delivered, Returned, In Transit
    TRY_CAST(REPLACE(shipping_cost, ' ', '') AS FLOAT) AS shipping_cost, -- Coût en USD
    TRIM(destination_region)            AS destination_region,
    TRIM(destination_country)           AS destination_country,
    TRIM(carrier)                       AS carrier

FROM BRONZE.logistics_and_shipping
WHERE shipment_id IS NOT NULL
  AND order_id IS NOT NULL
  AND TRY_CAST(ship_date AS DATE) IS NOT NULL
  -- Coût d'expédition forcément positif
  AND TRY_CAST(REPLACE(shipping_cost, ' ', '') AS FLOAT) > 0;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.logistics_and_shipping;
SELECT status, COUNT(*) AS nb
FROM SILVER.logistics_and_shipping
GROUP BY status ORDER BY nb DESC;


-- ============================================================
-- TABLE 10 : supplier_information
-- Source  : BRONZE.supplier_information
-- Problèmes identifiés et corrigés :
--   ⚠️ lead_time nul ou négatif → filtre > 0
--   ⚠️ reliability_score hors [0, 1] → filtre BETWEEN
-- ============================================================
CREATE OR REPLACE TABLE SILVER.supplier_information AS
SELECT DISTINCT
    TRIM(supplier_id)                   AS supplier_id,
    TRIM(supplier_name)                 AS supplier_name,
    TRIM(product_category)              AS product_category,
    TRIM(region)                        AS region,
    TRIM(country)                       AS country,
    TRIM(city)                          AS city,
    TRY_CAST(lead_time AS INTEGER)      AS lead_time,          -- Délai de livraison en jours
    TRY_CAST(reliability_score AS FLOAT) AS reliability_score, -- Score entre 0 et 1
    TRIM(quality_rating)                AS quality_rating      -- A, B ou C

FROM BRONZE.supplier_information
WHERE supplier_id IS NOT NULL
  -- Délai de livraison forcément positif
  AND TRY_CAST(lead_time AS INTEGER) > 0
  -- Score de fiabilité valide entre 0 et 1
  AND TRY_CAST(reliability_score AS FLOAT) BETWEEN 0 AND 1;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.supplier_information;
SELECT quality_rating, COUNT(*) AS nb, AVG(reliability_score) AS fiabilite_moy
FROM SILVER.supplier_information
GROUP BY quality_rating ORDER BY quality_rating;


-- ============================================================
-- TABLE 11 : employee_records
-- Source  : BRONZE.employee_records
-- Problèmes identifiés et corrigés :
--   ⚠️ Titres parasites dans les noms (Dr., MD, DDS...)
--      → Supprimés via REGEXP_REPLACE
--   ⚠️ Employés embauchés AVANT leur naissance
--      → Filtre : hire_date > date_of_birth
--   ⚠️ Employés embauchés avant 16 ans
--      → Filtre : DATEDIFF >= 16 ans
--   ⚠️ Doublons sur employee_id (même ID, noms différents)
--      → QUALIFY ROW_NUMBER() : on garde le plus récemment embauché
--   ⚠️ Region/Country incohérents (Colombia en Africa...)
--      → Non corrigeable sans référentiel externe, conservé tel quel
--   ⚠️ Job title ne correspond pas au département
--      → Non corrigeable sans règle métier, conservé tel quel
-- ============================================================
CREATE OR REPLACE TABLE SILVER.employee_records AS
SELECT DISTINCT
    TRIM(employee_id)                   AS employee_id,

    -- Suppression des titres parasites dans le nom
    TRIM(REGEXP_REPLACE(name,
        '\\b(Dr\\.|Mrs\\.|Mr\\.|Ms\\.|MD|DDS|DVM|PhD)\\b', '')) AS name,

    TRY_CAST(date_of_birth AS DATE)     AS date_of_birth,
    TRY_CAST(hire_date AS DATE)         AS hire_date,
    TRIM(department)                    AS department,  -- Sales, Finance, Marketing, IT, HR, Operations, Customer Service
    TRIM(job_title)                     AS job_title,
    TRY_CAST(REPLACE(salary, ' ', '') AS FLOAT) AS salary, -- Salaire en USD
    TRIM(region)                        AS region,
    TRIM(country)                       AS country,
    LOWER(TRIM(email))                  AS email        -- Harmonisation en minuscules

FROM BRONZE.employee_records
WHERE employee_id IS NOT NULL
  AND name IS NOT NULL
  -- Exclusion des cas impossibles : embauche avant la naissance
  AND TRY_CAST(hire_date AS DATE) > TRY_CAST(date_of_birth AS DATE)
  -- Exclusion des employés embauchés avant 16 ans
  AND DATEDIFF('year',
        TRY_CAST(date_of_birth AS DATE),
        TRY_CAST(hire_date AS DATE)) >= 16
  AND TRY_CAST(REPLACE(salary, ' ', '') AS FLOAT) > 0

-- Gestion des doublons sur employee_id : on garde le plus récemment embauché
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY employee_id
    ORDER BY TRY_CAST(hire_date AS DATE) DESC
) = 1;

-- Vérification
SELECT COUNT(*) AS nb_lignes FROM SILVER.employee_records;

-- Vérifier l'absence de doublons sur employee_id
SELECT employee_id, COUNT(*) AS nb
FROM SILVER.employee_records
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- Vérifier les âges minimum à l'embauche
SELECT name, date_of_birth, hire_date,
    DATEDIFF('year', date_of_birth, hire_date) AS age_a_lembauche
FROM SILVER.employee_records
ORDER BY age_a_lembauche ASC
LIMIT 10;


-- ============================================================
-- BILAN FINAL : Nombre de lignes par table SILVER
-- ============================================================
SELECT 'customer_demographics'        AS table_name, COUNT(*) AS nb_lignes FROM SILVER.customer_demographics        UNION ALL
SELECT 'customer_service_interactions',               COUNT(*)              FROM SILVER.customer_service_interactions UNION ALL
SELECT 'financial_transactions',                      COUNT(*)              FROM SILVER.financial_transactions        UNION ALL
SELECT 'promotions_clean',                            COUNT(*)              FROM SILVER.promotions_clean              UNION ALL
SELECT 'marketing_campaigns',                         COUNT(*)              FROM SILVER.marketing_campaigns           UNION ALL
SELECT 'product_reviews',                             COUNT(*)              FROM SILVER.product_reviews               UNION ALL
SELECT 'inventory',                                   COUNT(*)              FROM SILVER.inventory                     UNION ALL
SELECT 'store_locations',                             COUNT(*)              FROM SILVER.store_locations               UNION ALL
SELECT 'logistics_and_shipping',                      COUNT(*)              FROM SILVER.logistics_and_shipping        UNION ALL
SELECT 'supplier_information',                        COUNT(*)              FROM SILVER.supplier_information          UNION ALL
SELECT 'employee_records',                            COUNT(*)              FROM SILVER.employee_records;
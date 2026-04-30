-- ============================================================
-- PROJET    : AnyCompany Food & Beverage – Data Analytics
-- FICHIER   : Load_data.sql
-- AUTEUR    : Équipe Data Engineering – MBAESG 2026
-- DESCRIPTION : Phase 1 – Étapes 1 à 3
--               - Création de l'environnement Snowflake
--                 (warehouse, base de données, schémas)
--               - Création des tables BRONZE (données brutes)
--               - Création du stage S3
--               - Chargement des fichiers CSV et JSON
--               - Vérifications post-chargement
-- SOURCE    : s3://logbrain-datalake/datasets/food-beverage/
-- ============================================================


-- ============================================================
-- ÉTAPE 1 : Création de l'environnement Snowflake
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Création du Virtual Warehouse dédié au lab
-- AUTO_SUSPEND = 60s pour économiser les crédits
CREATE WAREHOUSE IF NOT EXISTS ANYCOMPANY_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Warehouse dédié au lab AnyCompany Food & Beverage';

-- Création de la base de données principale
CREATE DATABASE IF NOT EXISTS ANYCOMPANY_LAB
    COMMENT = 'Base de données du lab Data-Driven Marketing Analytics';

-- Création du schéma BRONZE : données brutes chargées depuis S3
CREATE SCHEMA IF NOT EXISTS ANYCOMPANY_LAB.BRONZE
    COMMENT = 'Données brutes chargées depuis S3 sans transformation';

-- Création du schéma SILVER : données nettoyées et exploitables
CREATE SCHEMA IF NOT EXISTS ANYCOMPANY_LAB.SILVER
    COMMENT = 'Données nettoyées, typées et exploitables pour les analyses';


-- ============================================================
-- ÉTAPE 2 : Création des tables BRONZE
-- Toutes les colonnes sont en VARCHAR pour éviter les erreurs
-- de typage lors du chargement brut depuis S3.
-- Le typage sera effectué dans le schéma SILVER (clean_data.sql)
-- ============================================================

USE DATABASE ANYCOMPANY_LAB;
USE SCHEMA BRONZE;
USE WAREHOUSE ANYCOMPANY_WH;

-- ------------------------------------------------------------
-- TABLE 1 : customer_demographics
-- Données démographiques des clients
-- (identité, localisation, revenu annuel)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.customer_demographics (
    customer_id       VARCHAR(20),
    name              VARCHAR(100),
    date_of_birth     VARCHAR(20),
    gender            VARCHAR(20),
    region            VARCHAR(50),
    country           VARCHAR(50),
    city              VARCHAR(50),
    marital_status    VARCHAR(20),
    annual_income     VARCHAR(20)    -- Peut contenir des espaces ex: "49 526"
);

-- ------------------------------------------------------------
-- TABLE 2 : customer_service_interactions
-- Interactions des clients avec le service client
-- (type, catégorie du problème, durée, satisfaction)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.customer_service_interactions (
    interaction_id        VARCHAR(20),
    interaction_date      VARCHAR(20),
    interaction_type      VARCHAR(20),   -- Phone, Email, Chat, Social Media
    issue_category        VARCHAR(50),   -- Complaints, Returns, Product Inquiry, Technical Support, Order Status
    description           VARCHAR(500),
    duration_minutes      VARCHAR(10),
    resolution_status     VARCHAR(20),   -- Resolved, Pending, Escalated
    follow_up_required    VARCHAR(10),   -- Yes/No ou true/false selon les lignes
    customer_satisfaction VARCHAR(5)     -- Score de 1 à 5
);

-- ------------------------------------------------------------
-- TABLE 3 : financial_transactions
-- Transactions financières de l'entreprise
-- (ventes, remboursements, investissements, taxes)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.financial_transactions (
    transaction_id    VARCHAR(20),
    transaction_date  VARCHAR(20),
    transaction_type  VARCHAR(30),   -- Sale, Refund, Investment, Tax Payment
    amount            VARCHAR(20),   -- Peut contenir des espaces ex: "5 648.71"
    payment_method    VARCHAR(30),   -- Credit Card, Bank Transfer, PayPal
    entity            VARCHAR(100),
    region            VARCHAR(50),
    account_code      VARCHAR(20)
);

-- ------------------------------------------------------------
-- TABLE 4 : promotions_data
-- Données des promotions commerciales
-- (type, remise en décimal, période, région ciblée)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.promotions_data (
    promotion_id         VARCHAR(20),
    product_category     VARCHAR(50),
    promotion_type       VARCHAR(50),   -- ex: Beverage Bonanza, Juice Jamboree, Spring Refresh
    discount_percentage  VARCHAR(10),   -- Format décimal : 0.15 = 15%
    start_date           VARCHAR(20),
    end_date             VARCHAR(20),
    region               VARCHAR(100)
);

-- ------------------------------------------------------------
-- TABLE 5 : marketing_campaigns
-- Campagnes marketing de l'entreprise
-- (type, audience, budget, portée, taux de conversion)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.marketing_campaigns (
    campaign_id       VARCHAR(20),
    campaign_name     VARCHAR(100),   -- ⚠️ Contient des noms d'agences, pas des noms de campagnes
    campaign_type     VARCHAR(50),    -- Print, Email, Content Marketing, Influencer
    product_category  VARCHAR(50),
    target_audience   VARCHAR(50),    -- Families, Seniors, Professionals, Young Adults
    start_date        VARCHAR(20),
    end_date          VARCHAR(20),
    region            VARCHAR(100),
    budget            VARCHAR(20),    -- Peut contenir des espaces ex: "106 162.48"
    reach             VARCHAR(20),    -- Nombre de personnes touchées
    conversion_rate   VARCHAR(10)     -- Format décimal : 0.0614 = 6.14%
);

-- ------------------------------------------------------------
-- TABLE 6 : product_reviews
-- Avis clients sur les produits
-- ⚠️ Fichier avec séparateur tabulation (\t)
--    Textes libres pouvant contenir virgules et retours à la ligne
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.product_reviews (
    review_id         VARCHAR(20),
    product_id        VARCHAR(20),
    reviewer_id       VARCHAR(50),
    reviewer_name     VARCHAR(100),
    rating            VARCHAR(5),      -- Note de 1 à 5
    review_date       VARCHAR(30),
    review_title      VARCHAR(500),    -- Taille large pour les titres longs
    review_text       VARCHAR(10000),  -- Taille large pour les textes libres
    product_category  VARCHAR(100)
);

-- ------------------------------------------------------------
-- TABLE 7 : inventory
-- Niveaux de stock par produit et entrepôt
-- Fichier source JSON → stocké en colonne VARIANT
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.inventory (
    raw_data VARIANT   -- JSON brut, extraction dans SILVER
);

-- ------------------------------------------------------------
-- TABLE 8 : store_locations
-- Informations géographiques des magasins
-- Fichier source JSON → stocké en colonne VARIANT
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.store_locations (
    raw_data VARIANT   -- JSON brut, extraction dans SILVER
);

-- ------------------------------------------------------------
-- TABLE 9 : logistics_and_shipping
-- Données logistiques et d'expédition
-- (méthode d'envoi, statut, coût, transporteur)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.logistics_and_shipping (
    shipment_id          VARCHAR(20),
    order_id             VARCHAR(20),
    ship_date            VARCHAR(20),
    estimated_delivery   VARCHAR(20),
    shipping_method      VARCHAR(20),   -- Standard, Express, Next Day
    status               VARCHAR(20),   -- Shipped, Delivered, Returned, In Transit
    shipping_cost        VARCHAR(20),
    destination_region   VARCHAR(100),
    destination_country  VARCHAR(50),
    carrier              VARCHAR(100)
);

-- ------------------------------------------------------------
-- TABLE 10 : supplier_information
-- Informations sur les fournisseurs
-- (délai livraison, score fiabilité, note qualité)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.supplier_information (
    supplier_id        VARCHAR(20),
    supplier_name      VARCHAR(100),
    product_category   VARCHAR(50),
    region             VARCHAR(50),
    country            VARCHAR(50),
    city               VARCHAR(50),
    lead_time          VARCHAR(10),   -- Délai en jours
    reliability_score  VARCHAR(10),   -- Score entre 0 et 1
    quality_rating     VARCHAR(5)     -- A, B ou C
);

-- ------------------------------------------------------------
-- TABLE 11 : employee_records
-- Données organisationnelles des employés
-- (département, poste, salaire, localisation)
-- ⚠️ Problèmes qualité : titres dans les noms, region/country
--    incohérents, employés embauchés avant leur naissance
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.employee_records (
    employee_id     VARCHAR(20),
    name            VARCHAR(100),
    date_of_birth   VARCHAR(20),
    hire_date       VARCHAR(20),
    department      VARCHAR(50),    -- Sales, Finance, Marketing, IT, HR, Operations, Customer Service
    job_title       VARCHAR(100),
    salary          VARCHAR(20),    -- Peut contenir des espaces
    region          VARCHAR(50),
    country         VARCHAR(50),
    email           VARCHAR(100)
);


-- ============================================================
-- ÉTAPE 3 : Création du Stage S3 et chargement des données
-- Le stage pointe directement vers le bucket S3 public
-- contenant tous les fichiers sources du projet
-- ============================================================

-- Création du stage externe S3
CREATE OR REPLACE STAGE ANYCOMPANY_STAGE
    URL         = 's3://logbrain-datalake/datasets/food-beverage/'
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Vérification que le stage est accessible
LIST @ANYCOMPANY_STAGE;


-- ============================================================
-- Chargement des fichiers CSV (tables 1 à 6, 9, 10, 11)
-- ON_ERROR = 'CONTINUE' : charge les lignes valides
-- même si certaines contiennent des erreurs de parsing
-- ============================================================

-- TABLE 1 : customer_demographics
COPY INTO BRONZE.customer_demographics
FROM @ANYCOMPANY_STAGE/customer_demographics.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TABLE 2 : customer_service_interactions
COPY INTO BRONZE.customer_service_interactions
FROM @ANYCOMPANY_STAGE/customer_service_interactions.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TABLE 3 : financial_transactions
COPY INTO BRONZE.financial_transactions
FROM @ANYCOMPANY_STAGE/financial_transactions.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TABLE 4 : promotions_data
-- ⚠️ Nom du fichier avec un tiret : promotions-data.csv
COPY INTO BRONZE.promotions_data
FROM @ANYCOMPANY_STAGE/promotions-data.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TABLE 5 : marketing_campaigns
COPY INTO BRONZE.marketing_campaigns
FROM @ANYCOMPANY_STAGE/marketing_campaigns.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TABLE 6 : product_reviews
-- ⚠️ Séparateur tabulation (\t) au lieu de la virgule
-- ⚠️ FORCE = TRUE car Snowflake mémorise les fichiers déjà chargés
COPY INTO BRONZE.product_reviews
FROM @ANYCOMPANY_STAGE/product_reviews.csv
FILE_FORMAT = (
    TYPE                          = 'CSV'
    FIELD_DELIMITER               = '\t'    -- Séparateur tabulation
    SKIP_HEADER                   = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TRIM_SPACE                    = TRUE
    EMPTY_FIELD_AS_NULL           = TRUE
)
ON_ERROR = 'CONTINUE'
FORCE    = TRUE;

-- TABLE 9 : logistics_and_shipping
COPY INTO BRONZE.logistics_and_shipping
FROM @ANYCOMPANY_STAGE/logistics_and_shipping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TABLE 10 : supplier_information
COPY INTO BRONZE.supplier_information
FROM @ANYCOMPANY_STAGE/supplier_information.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TABLE 11 : employee_records
COPY INTO BRONZE.employee_records
FROM @ANYCOMPANY_STAGE/employee_records.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';


-- ============================================================
-- Chargement des fichiers JSON (tables 7 et 8)
-- STRIP_OUTER_ARRAY = TRUE : le fichier JSON est un tableau
-- d'objets → chaque objet devient une ligne dans la table
-- ============================================================

-- TABLE 7 : inventory
COPY INTO BRONZE.inventory
FROM @ANYCOMPANY_STAGE/inventory.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE';

-- TABLE 8 : store_locations
COPY INTO BRONZE.store_locations
FROM @ANYCOMPANY_STAGE/store_locations.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE';


-- ============================================================
-- VÉRIFICATION GLOBALE : Nombre de lignes par table BRONZE
-- Résultats attendus :
--   - 10 tables à 5000 lignes
--   - product_reviews à ~911 lignes (textes libres irréguliers)
-- ============================================================
SELECT 'customer_demographics'        AS table_name, COUNT(*) AS nb_lignes FROM BRONZE.customer_demographics        UNION ALL
SELECT 'customer_service_interactions',               COUNT(*)              FROM BRONZE.customer_service_interactions UNION ALL
SELECT 'financial_transactions',                      COUNT(*)              FROM BRONZE.financial_transactions        UNION ALL
SELECT 'promotions_data',                             COUNT(*)              FROM BRONZE.promotions_data               UNION ALL
SELECT 'marketing_campaigns',                         COUNT(*)              FROM BRONZE.marketing_campaigns           UNION ALL
SELECT 'product_reviews',                             COUNT(*)              FROM BRONZE.product_reviews               UNION ALL
SELECT 'logistics_and_shipping',                      COUNT(*)              FROM BRONZE.logistics_and_shipping        UNION ALL
SELECT 'supplier_information',                        COUNT(*)              FROM BRONZE.supplier_information          UNION ALL
SELECT 'employee_records',                            COUNT(*)              FROM BRONZE.employee_records              UNION ALL
SELECT 'inventory',                                   COUNT(*)              FROM BRONZE.inventory                     UNION ALL
SELECT 'store_locations',                             COUNT(*)              FROM BRONZE.store_locations;


-- ============================================================
-- ATTRIBUTION DES DROITS AUX UTILISATEURS
-- À exécuter après la création des tables et le chargement
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Création du rôle dédié au lab
CREATE ROLE IF NOT EXISTS ANYCOMPANY_ROLE;

-- Droits sur le warehouse
GRANT USAGE ON WAREHOUSE ANYCOMPANY_WH TO ROLE ANYCOMPANY_ROLE;

-- Droits sur la base de données et les schémas
GRANT USAGE ON DATABASE ANYCOMPANY_LAB              TO ROLE ANYCOMPANY_ROLE;
GRANT USAGE ON SCHEMA ANYCOMPANY_LAB.BRONZE         TO ROLE ANYCOMPANY_ROLE;
GRANT USAGE ON SCHEMA ANYCOMPANY_LAB.SILVER         TO ROLE ANYCOMPANY_ROLE;

-- Droits sur toutes les tables existantes
GRANT SELECT ON ALL TABLES IN SCHEMA ANYCOMPANY_LAB.BRONZE TO ROLE ANYCOMPANY_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ANYCOMPANY_LAB.SILVER TO ROLE ANYCOMPANY_ROLE;

-- Droits sur les tables futures
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANYCOMPANY_LAB.BRONZE TO ROLE ANYCOMPANY_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANYCOMPANY_LAB.SILVER TO ROLE ANYCOMPANY_ROLE;

-- Création des utilisateurs
CREATE USER IF NOT EXISTS SYFAX
    PASSWORD          = 'AnyCompany2025!'
    DEFAULT_ROLE      = ANYCOMPANY_ROLE
    DEFAULT_WAREHOUSE = ANYCOMPANY_WH
    DEFAULT_NAMESPACE = 'ANYCOMPANY_LAB.BRONZE'
    MUST_CHANGE_PASSWORD = TRUE;

CREATE USER IF NOT EXISTS MIKHAILKOSAREV
    PASSWORD          = 'AnyCompany2025!'
    DEFAULT_ROLE      = ANYCOMPANY_ROLE
    DEFAULT_WAREHOUSE = ANYCOMPANY_WH
    DEFAULT_NAMESPACE = 'ANYCOMPANY_LAB.BRONZE'
    MUST_CHANGE_PASSWORD = TRUE;

-- Attribution du rôle aux utilisateurs
GRANT ROLE ANYCOMPANY_ROLE TO USER SYFAX;
GRANT ROLE ANYCOMPANY_ROLE TO USER MIKHAILKOSAREV;
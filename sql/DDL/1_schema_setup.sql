-- =============================================
-- SCHEMA AND TABLE SETUP SCRIPT
-- Combines all DDL operations into a single file
-- =============================================

-- =============================================
-- 1. SCHEMA CREATION
-- =============================================
-- Create the main schemas
CREATE SCHEMA IF NOT EXISTS DATA_PIPELINE;
CREATE SCHEMA IF NOT EXISTS STAGING;

-- Grant permissions to appropriate roles
GRANT USAGE ON SCHEMA DATA_PIPELINE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA STAGING TO ROLE SYSADMIN;

-- =============================================
-- 2. TABLE CREATION
-- =============================================
-- Set the context to DATA_PIPELINE schema
USE SCHEMA DATA_PIPELINE;

-- Customers table
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID INT PRIMARY KEY,
    CUSTOMER_NAME STRING NOT NULL,
    EMAIL STRING UNIQUE,
    PHONE STRING,
    ADDRESS STRING,
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

-- Products table
CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID INT PRIMARY KEY,
    PRODUCT_NAME STRING NOT NULL,
    DESCRIPTION STRING,
    PRICE NUMBER(10, 2) NOT NULL,
    CATEGORY STRING,
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================
-- 3. STAGING TABLES
-- =============================================
-- Switch to STAGING schema
USE SCHEMA STAGING;

-- Staging table for customer data loads
CREATE OR REPLACE TABLE STG_CUSTOMERS (
    CUSTOMER_ID INT,
    CUSTOMER_NAME STRING,
    EMAIL STRING,
    PHONE STRING,
    ADDRESS STRING,
    LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID STRING
);

-- Staging table for product data loads
CREATE OR REPLACE TABLE STG_PRODUCTS (
    PRODUCT_ID INT,
    PRODUCT_NAME STRING,
    DESCRIPTION STRING,
    PRICE NUMBER(10, 2),
    CATEGORY STRING,
    LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID STRING
);

-- =============================================
-- 4. VIEWS
-- =============================================
-- Switch back to DATA_PIPELINE schema
USE SCHEMA DATA_PIPELINE;

-- View for active customers
CREATE OR REPLACE VIEW VW_ACTIVE_CUSTOMERS AS
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    PHONE,
    CREATED_AT
FROM 
    CUSTOMERS
WHERE 
    UPDATED_AT >= DATEADD(month, -6, CURRENT_TIMESTAMP());

-- View for product categories
CREATE OR REPLACE VIEW VW_PRODUCT_CATEGORIES AS
SELECT 
    CATEGORY,
    COUNT(*) AS PRODUCT_COUNT,
    AVG(PRICE) AS AVG_PRICE,
    MIN(PRICE) AS MIN_PRICE,
    MAX(PRICE) AS MAX_PRICE
FROM 
    PRODUCTS
GROUP BY 
    CATEGORY
ORDER BY 
    PRODUCT_COUNT DESC;

-- =============================================
-- 5. FINAL VALIDATION
-- =============================================
-- Verify all objects were created
SELECT 'Schema Creation Complete' AS STATUS,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'DATA_PIPELINE') AS DATA_PIPELINE_TABLES,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS 
        WHERE TABLE_SCHEMA = 'DATA_PIPELINE') AS DATA_PIPELINE_VIEWS,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'STAGING') AS STAGING_TABLES;

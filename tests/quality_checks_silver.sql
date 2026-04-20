/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/



-- Checking data quality in silver layer

-- ========================================================
-- Checking 'silver.crm_cust_info'
-- ========================================================
-- checking for nulls and duplicates in cst_id
-- Expectation: No result
SELECT
cst_id,
count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL; 

-- checking for unwanted spaces in cst_key
-- Expectation: No result
SELECT
cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- checking for unwanted spaces in cst_firstname
-- Expectation: No result
SELECT
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- checking data standardizaton and consistency
SELECT DISTINCT
cst_gndr
FROM silver.crm_cust_info;

-- ========================================================
-- Checking 'silver.crm_prd_info'
-- ========================================================

-- checking for nulls and duplicates in prd_id
-- Expectation: No result
SELECT
prd_id,
count(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id =0 OR prd_id IS NULL;

-- checking for unwanted spaces in prd_num
-- Expectation: No result
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- checking data standardizaton and consistency
SELECT DISTINCT
prd_line
FROM silver.crm_prd_info;

-- checking null or negative values in cost
-- Expectation: No result 
SELECT 
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <= 0 OR NULL;

-- checking Invalid data order
-- Expectation: No result
SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ========================================================
-- Checking 'silver.crm_sales_details'
-- ========================================================

-- checking Invalid dates
-- Expectation: No results 
SELECT
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 
OR length(sls_order_dt) != 8;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT
    sls_sales, sls_quantity, sls_price
FROM
    silver.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
        OR sls_sales IS NULL
        OR sls_quantity IS NULL
        OR sls_price IS NULL
        OR sls_sales <= 0
        OR sls_quantity <= 0
        OR sls_price <= 0
ORDER BY sls_sales , sls_quantity , sls_price;


-- ========================================================
-- Checking 'silver.erp_CUST_AZ12'
-- ========================================================

-- Checking furure dates
-- Expectation: no result
SELECT DISTINCT
BDATE
FROM silver.erp_CUST_AZ12
WHERE BDATE > CURDATE();

-- Checking standardization and consistency
-- Expectation: no result
SELECT DISTINCT
GEN
FROM silver.erp_CUST_AZ12;


-- ====================================================================
-- Checking 'silver.erp_loc_A101'
-- ====================================================================

-- Checking standardization and consistency

SELECT DISTINCT
CNTRY
FROM silver.erp_LOC_A101;


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Checking data standardization and consistency
SELECT DISTINCT
MAINTENANCE
FROM silver.erp_PX_CAT_G1V2;

-- Checkfor unwanted space
-- Expectation: no result
SELECT 
*
FROM silver.erp_PX_CAT_G1V2
WHERE ID != TRIM(ID)
OR   CAT != TRIM(CAT)
OR SUBCAT!= TRIM(SUBCAT)
OR MAINTENANCE != TRIM(MAINTENANCE)

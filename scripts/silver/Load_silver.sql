/*
=============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
===============================================================================
*/



-- Loading clean and transform data in silver layer
-- ==================START BATCH==================

SET @batch_start_time = NOW();

SELECT'Loading silver layer' AS Message;

SELECT'Loading CRM TABLES' AS Message;

-- loading silver.crm_cust_info

SET @start_time = NOW();
TRUNCATE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname, -- remove unwanted spaces
TRIM(cst_lastname) AS cst_lastname,   -- remove unwanted spaces
CASE
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'n/a'
END AS cst_marital_status, -- normalize marital status to readable form

CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'n/a'
END AS cst_gndr, -- normalize gender values to readable form
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
	FROM bronze.crm_cust_info) t where rn = 1 AND cst_id != 0 AND cst_id IS NOT NULL; -- select recent record
SET @end_time = NOW();

SELECT CONCAT('silver.crm_cust_info loading duration: ',
TIMESTAMPDIFF(second,@start_time,@end_time), ' seconds') AS Duration;

-- loading silver.crm_prd_info
SET @start_time = NOW();
TRUNCATE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Exract cataegory ID
SUBSTRING(prd_key,7,length(prd_key)) AS prd_key,   -- Extract product key
prd_nm,
NULLIF(prd_cost,0) AS prd_cost,
CASE
	WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line, -- Map product line to descriptive name
prd_start_dt,
(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY) AS prd_end_date
FROM bronze.crm_prd_info; -- calculate end date as one day before start date
SET @end_time = NOW();

SELECT CONCAT('silver.crm_prd_info loading duration: ',
TIMESTAMPDIFF(second,@start_time,@end_time), ' seconds') AS Duration;

-- loading silver.crm_sales_details
SET @start_time = NOW();
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details(

sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
		ELSE CAST(sls_order_dt AS DATE)
	END AS sls_order_dt,

	CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(sls_ship_dt AS DATE)
	END AS sls_ship_dt,

	CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
		ELSE CAST(sls_due_dt AS DATE)
	END AS sls_due_dt,

		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price 
	END)
			THEN CAST(sls_quantity * ABS(
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price 
	END) AS SIGNED)
			ELSE CAST(sls_sales AS SIGNED) 
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
	sls_quantity,
	CAST(
	CASE
		WHEN sls_price IS NULL OR sls_price <=0 
		THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price 
	END AS SIGNED
    ) sls_price  -- Calculate price if original value is invalid
	FROM bronze.crm_sales_details;
SET @end_time = NOW();

SELECT CONCAT('silver.crm_sales_details loading duration: ',
TIMESTAMPDIFF(second,@start_time,@end_time), ' seconds') AS Duration;

SELECT'Loading ERP TABLES' AS Message;

-- loading silver.erp_CUST_AZ12
SET @start_time = NOW();
TRUNCATE silver.erp_CUST_AZ12;
INSERT INTO silver.erp_CUST_AZ12(

	CID,
	BDATE,
	GEN
)
SELECT
CASE 
	WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID)) -- remove NAS prefix if present
    ELSE CID
END AS CID,

CASE 
	WHEN BDATE > CURDATE() THEN NULL
    ELSE BDATE
END AS BDATE, -- set future dates to nulls

CASE 
	WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(GEN)) IN ('M','MALE')   THEN 'Male'
    ELSE 'n/a'
END AS GEN -- Normalize gender values and handle unknown nulls
FROM bronze.erp_CUST_AZ12;
SET @end_time = NOW();

SELECT CONCAT('silver.erp_CUST_AZ12 loading duration: ',
TIMESTAMPDIFF(second,@start_time,@end_time), ' seconds') AS Duration;

-- loading silver.erp_LOC_A101
SET @start_time = NOW(); 
TRUNCATE silver.erp_LOC_A101;
INSERT INTO silver.erp_LOC_A101(
CID,
CNTRY
) 

SELECT
REPLACE(CID,'-','') AS CID,
CASE
	WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
    WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United states'
    WHEN TRIM(CNTRY) = '' OR  TRIM(CNTRY) IS NULL THEN 'n/a'
    ELSE TRIM(CNTRY)
END AS CNTRY -- normalize and handle missing or blank country code 
FROM bronze.erp_LOC_A101;
SET @end_time = NOW();

SELECT CONCAT('silver.erp_LOC_A101 loading duration: ',
TIMESTAMPDIFF(second,@start_time,@end_time), ' seconds') AS Duration;

-- loading silver.erp_PX_CAT_G1V2
SET @start_time = NOW();
TRUNCATE silver.erp_PX_CAT_G1V2;
INSERT INTO silver.erp_PX_CAT_G1V2(
ID,
CAT,
SUBCAT,
MAINTENANCE
)

SELECT
ID,
CAT,
SUBCAT,
MAINTENANCE
FROM bronze.erp_PX_CAT_G1V2;
SET @end_time = NOW();

SELECT CONCAT('silver.erp_PX_CAT_G1V2 loading duration: ',
TIMESTAMPDIFF(second,@start_time,@end_time), ' seconds') AS Duration;

-- ==================END BATCH==================

SET @batch_end_time = NOW();

SELECT CONCAT('Total loading duration: ',



TIMESTAMPDIFF(second,@batch_start_time,@batch_end_time), ' seconds') AS Duration;

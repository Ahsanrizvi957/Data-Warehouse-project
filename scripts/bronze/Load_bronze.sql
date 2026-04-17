/*
===============================================================================
 Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading fresh data.
    - Uses the `Load data infile` command to load data from csv Files to bronze tables.

===============================================================================
*/

-- ==============================START BATCH==============================

SET @batch_start_time = NOW();

SELECT'=================================================================' AS msg;
SELECT'Loading bronze layer' AS msg;
SELECT'=================================================================' AS msg;

-- =======================CRM CUSTOMER=======================
SET @start_time = NOW();

TRUNCATE TABLE bronze.crm_cust_info;

LOAD DATA LOCAL INFILE '/home/ali/Downloads/DATA_SOURCE/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET @end_time = NOW();

SELECT CONCAT('crm_cust_info loading durarion: ', TIMESTAMPDIFF(second,@start_time,@end_time), ' seconds') AS Duration;

-- =======================CRM PRODUCTS=======================
SET @start_time = NOW();

TRUNCATE bronze.crm_prd_info;

LOAD DATA LOCAL INFILE  '/home/ali/Downloads/DATA_SOURCE/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

SET @end_time = NOW();

SELECT CONCAT('crm_prd_info loading duration: ',TIMESTAMPDIFF(second,@start_time,@end_time),' seconds') AS Duration;

-- =======================CRM SALES=======================
SET @start_time = NOW();

TRUNCATE bronze.crm_sales_details;

LOAD DATA LOCAL INFILE '/home/ali/Downloads/DATA_SOURCE/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET @end_time = NOW();

SELECT CONCAT('crm_sales_details loading duration: ',TIMESTAMPDIFF(second,@start_time,@end_time),' seconds') AS Duration;

-- =======================ERP CUST=======================
SET @start_time = NOW();

TRUNCATE bronze.erp_CUST_AZ12;

LOAD DATA LOCAL INFILE '/home/ali/Downloads/DATA_SOURCE/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_CUST_AZ12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET @end_time = NOW();

SELECT CONCAT('erp_CUST_AZ12 loading duration: ',TIMESTAMPDIFF(second,@start_time,@end_time),' seconds') AS Duration;


-- =======================ERP LOC=======================
SET @start_time = NOW();

TRUNCATE bronze.erp_LOC_A101;

LOAD DATA LOCAL INFILE '/home/ali/Downloads/DATA_SOURCE/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_LOC_A101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET @end_time = NOW();

SELECT CONCAT('erp_LOC_A101 loading duration: ',TIMESTAMPDIFF(second,@start_time,@end_time),' seconds') AS Duration;

-- =======================ERP PRODUCT CATEGORY=======================
SET @start_time = NOW();

TRUNCATE bronze.erp_PX_CAT_G1V2;

LOAD DATA LOCAL INFILE '/home/ali/Downloads/DATA_SOURCE/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_PX_CAT_G1V2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET @end_time = NOW();

SELECT CONCAT('erp_PX_CAT_G1V2 loading duration: ',TIMESTAMPDIFF(second,@start_time,@end_time),' seconds') AS Duration;

-- ==============================START BATCH==============================
SET @batch_end_time = NOW();

SELECT'=================================================================' AS msg;
SELECT CONCAT('Total loading duration: ', TIMESTAMPDIFF(second,@batch_start_time,@batch_end_time),' seconds') AS Duration;
SELECT'=================================================================' AS msg;


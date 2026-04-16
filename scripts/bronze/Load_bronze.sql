/*
===============================================================================
 Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `Load data infile` command to load data from csv Files to bronze tables.

===============================================================================
*/




-- It First empty data in table and then Load data in bronze.crm_cust_info
TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA INFILE '/var/lib/mysql-files/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- It First empty data in table and then Load data in bronze.crm_prd_info
TRUNCATE bronze.crm_prd_info;
LOAD DATA INFILE '/var/lib/mysql-files/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

-- It First empty data in table and then Load data in bronze.crm_sales_details
TRUNCATE bronze.crm_sales_details;
LOAD DATA INFILE '/var/lib/mysql-files/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- It First empty data in table and then Load data in bronze.erp_CUST_AZ12
TRUNCATE bronze.erp_CUST_AZ12;
LOAD DATA INFILE '/var/lib/mysql-files/CUST_AZ12.csv'
INTO TABLE bronze.erp_CUST_AZ12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- It First empty data in table and then Load data in bronze.erp_LOC_A101
TRUNCATE bronze.erp_LOC_A101;
LOAD DATA INFILE '/var/lib/mysql-files/LOC_A101.csv'
INTO TABLE bronze.erp_LOC_A101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- It First empty data in table and then Load data in bronze.erp_PX_CAT_G1V2
TRUNCATE bronze.erp_PX_CAT_G1V2;
LOAD DATA INFILE '/var/lib/mysql-files/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_PX_CAT_G1V2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

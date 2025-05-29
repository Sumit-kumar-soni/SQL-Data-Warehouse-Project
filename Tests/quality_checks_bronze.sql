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
    - Run these checks after data loading Bronze Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ===================================================================================================
-- Bronze CRM Customer Info
-- ===================================================================================================

-- Check For NULL or Duplicates in Primary Key
-- Expectation: No Result

SELECT
	cst_id,
	COUNT(*)
FROM Bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Result

SELECT cst_firstname
FROM Bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_Lastname
FROM Bronze.crm_cust_info
WHERE cst_Lastname != TRIM(cst_Lastname);

-- Data Standardization & Consistency
-- Expectation: No Result

SELECT
	cst_marital_status,
	cst_gndr
FROM Bronze.crm_cust_info;

-- CLEANED DATASET

INSERT INTO Silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,    -- Remove Unwanted Spaces
	TRIM(cst_lastname) AS cst_lastname,      -- Remove Unwanted Spaces
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status,                      -- Normalize marital status values to readable format
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gndr,                                -- Normalize gender values to readable format
		cst_create_date
	FROM(
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS Flag_Last
			FROM Bronze.crm_cust_info
		)t
	WHERE Flag_Last = 1;                       -- Select the most recent record per customer

-- ===================================================================================================
-- Bronze CRM Product Info
-- ===================================================================================================

-- Check For NULL or Duplicates in Primary Key
-- Expectation: No Result

SELECT
	prd_id,
	COUNT(*)
FROM Bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Result

SELECT prd_nm
FROM Bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Result

SELECT
	prd_cost
FROM Bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
-- Expectation: No Result

SELECT prd_line
FROM Bronze.crm_prd_info;

-- Check for Invaild Date Orders
-- Expectation: No Result

SELECT * FROM Bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- CLEANED DATASET

INSERT INTO Silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,                  -- Extract category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,                         -- Extract product key
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,                                       -- Handling NULLs values
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,                                     -- Map product line codes to descriptive values
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1
		AS DATE)                   -- Calculate end day as one day before the next start date
	AS prd_end_dt
FROM Bronze.crm_prd_info;

-- ===================================================================================================
-- Bronze CRM Sales Details Info
-- ===================================================================================================

-- Check for Invaild Date
-- Expectation: No Result

SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM Bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20250601
OR sls_order_dt < 19000101;

SELECT 
NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM Bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20250601
OR sls_ship_dt < 19000101;

SELECT 
NULLIF(sls_due_dt,0) AS sls_ship_dt
FROM Bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20250601
OR sls_due_dt < 19000101;

-- Check for Invaild Date Orders
-- Expectation: No Result

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantity, and Price 
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero or negative.
-- Expectation: No Result

SELECT
	sls_sales AS OLD_SALES,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
	sls_quantity,
	sls_price AS OLD_PRICE,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM Bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY 	sls_sales, sls_quantity, sls_price;

-- CLEANED DATASET

INSERT INTO Silver.crm_sales_details(
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
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL  -- Handling invalid data
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)        -- Changing it to more Correct Data type
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)        -- Changing it to more Correct Data type
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL  -- Handling invalid data
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,              -- Recalculate sales if original value is missing or incorrect
	sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price               -- Derive price if original value is invalid
FROM Bronze.crm_sales_details;

-- ===================================================================================================
-- Bronze ERP Customer AZ12
-- ===================================================================================================

-- Identify Out-of-Range Dates
-- Expectation: No Result

SELECT
	bdate,
CASE WHEN bdate > GETDATE() THEN NULL 
	 ELSE bdate
END AS bdate
FROM Bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
-- Expectation: No Result

SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM Bronze.erp_cust_az12

-- Cleaned DataSet

INSERT INTO Silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))         -- Remove 'NAS' prefix if present 
	 ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL 
	 ELSE bdate
END AS bdate,                                     -- set future birthdates to NULL
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	 ELSE 'n/a'
END AS gen                                        -- Normalize gender values and handle unknown cases
FROM Bronze.erp_cust_az12

-- ===================================================================================================
-- Bronze ERP location
-- ===================================================================================================

-- Data Standardization & Consistency
-- Expectation: No Result

SELECT DISTINCT cntry AS old_cntry,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM Bronze.erp_loc_a101;

-- Cleaned Dataset

INSERT INTO Silver.erp_loc_a101(
	cid,
	cntry
)

SELECT
REPLACE(cid, '-', '') AS cid,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry           -- Normalize and Handle missing or blank country codes
FROM Bronze.erp_loc_a101;

-- ===================================================================================================
-- Bronze ERP category product
-- ===================================================================================================

-- Check for unwanted Spaces
-- Expectation: No Result

SELECT * FROM Bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
-- Expectation: No Result

SELECT DISTINCT
cat
FROM Bronze.erp_px_cat_g1v2;

SELECT DISTINCT
subcat
FROM Bronze.erp_px_cat_g1v2;

SELECT DISTINCT
maintenance
FROM Bronze.erp_px_cat_g1v2;

-- Cleaned Dataset

INSERT INTO Silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
)
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM Bronze.erp_px_cat_g1v2;

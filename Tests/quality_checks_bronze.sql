-- Check For NULL or Duplicates in Primary Key
-- Expectation: No Result

SELECT
	cst_id,
	COUNT(*)
FROM Bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted Spaces
SELECT cst_firstname
FROM Bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_Lastname
FROM Bronze.crm_cust_info
WHERE cst_Lastname != TRIM(cst_Lastname);

-- Data Standardization & Consistency

SELECT cst_Lastname
FROM Bronze.crm_cust_info;

-- CLEANED DATABASE
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
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
	cst_create_date
FROM(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS Flag_Last
	FROM Bronze.crm_cust_info
)t WHERE Flag_Last = 1;
SELECT * FROM Silver.crm_cust_info

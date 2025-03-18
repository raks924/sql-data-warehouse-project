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

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
/* 
--- check for non duplicate values
*/
select cst_id,count(*) as num_of_rec
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null


select 
* 
from (
select 
*,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as ord
from silver.crm_cust_info)t where ord = 1

-------CRM PROD INFO
--- Check for nulls or duplicates in primary key 
select prd_id , count(*) 
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

select * from silver.crm_prd_info
--- Check for unwanted spaces
select prd_nm 
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

--- Check for Nulls Or Negatives 
select prd_cost 
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

--- Data Standardization
select distinct prd_line
from bronze.crm_prd_info

--- Check for Invalid Date Orders
select * 
from  bronze.crm_prd_info
where prd_start_dt > prd_end_dt

--------CRM SALES DETAILS
elect * from bronze.crm_sales_details

---- check for unwanted spaces
select * 
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

--- check for invalid dates
select nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
or len(sls_order_dt) != 8
or sls_order_dt > 20500101 or sls_order_dt < 19000101

select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

--- check for data consistency 
select sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <0 or sls_quantity < 0 or sls_price < 0 
or sls_sales = 0 or sls_quantity = 0 or sls_price = 0 
order by sls_sales, sls_quantity, sls_price

----------ERP CUST AZ12
--- Identify Out of range dates
select distinct 
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > '2050-01-01'

--- identify low cardinality column
select distinct gen from bronze.erp_cust_az12

---- DATA STANDARDIZATION
select distinct
gen,
CASE WHEN UPPER(TRIM(gen)) LIKE '%F%'  THEN 'Female'
	 WHEN UPPER(TRIM(gen)) LIKE '%M%' THEN 'Male'
	 ELSE 'N/A'
END AS gen
from bronze.erp_cust_az12

----------ERP LOC A101
select cid
from silver.erp_loc_a101
where cid != trim(cid)
---Data Standardization 
select distinct cntry as old___,
case when UPPER(LTRIM(RTRIM(cntry))) like 'DE%' then 'Germany'
	 WHEN UPPER(LTRIM(RTRIM(cntry))) like 'US%' THEN 'United States'
	 when ltrim(rtrim(cntry)) like '_' or cntry is null then 'n/a'
	 else TRIM(cntry)
end as cntry 
from bronze.erp_loc_a101

-----------ERP PX CAT G1V2
---Data Standardization 
select distinct cntry as old___,
case when UPPER(LTRIM(RTRIM(cntry))) like 'DE%' then 'Germany'
	 WHEN UPPER(LTRIM(RTRIM(cntry))) like 'US%' THEN 'United States'
	 when ltrim(rtrim(cntry)) like '_' or cntry is null then 'n/a'
	 else TRIM(cntry)
end as cntry 
from bronze.erp_loc_a101





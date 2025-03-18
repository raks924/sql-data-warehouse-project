/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	declare @start_time Datetime , @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime
	begin try
		set @batch_start_time  = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver tables data'
		
		print 'Loading CRM Tables-------------------------------'
		print '>> Truncating table : silver.crm_cust_info';
		set @start_time  = GETDATE();
		truncate table silver.crm_cust_info
		print '>> Inserting data into : silver.crm_cust_info'
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname, ---removing unwanted spaces
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			 when upper(trim(cst_marital_status)) = 'M' then 'Married' 
			 else 'n/a' --- Handling missing values
		end cst_marital_status, ---Normalize marital status values to readable format 
		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr)) = 'M' then 'Male' 
			 else 'n/a'
		end cst_gndr,
		cst_create_date
		from 
		(
			select 
				*,
				ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as ord
			from bronze.crm_cust_info
			where cst_id is not Null --- Handling duplicates
		)t where ord=1 --- Select the most recent record per customer 
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		print '>> Truncating table : silver.crm_prd_info';
		set @start_time  = GETDATE();
		truncate table silver.crm_prd_info
		print '>> Inserting data into : silver.crm_prd_info'
		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
		prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id , --- derive cat_id to reference another table
		substring(prd_key,7,len(prd_key)) as prd_key, --- derive prd_key to reference another table
		prd_nm,
		isnull(prd_cost,0) as prd_cost, --- handling null values
		case upper(trim(prd_line))
			 when 'M' then 'Mountain' 
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt asc)-1 as date) as prd_end_dt --- data enrichment
		from bronze.crm_prd_info
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		print '>> Truncating table : silver.crm_sales_details';
		set @start_time  = GETDATE();
		truncate table silver.crm_sales_details
		print '>> Inserting data into : silver.crm_sales_details'
		insert into silver.crm_sales_details(
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

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt < 0 or len(sls_order_dt) != 8 then null
			 else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt < 0 or len(sls_ship_dt) != 8 then null
			 else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt < 0 or len(sls_due_dt) != 8 then null
			 else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case when sls_sales is null then sls_quantity*sls_price
			 when sls_sales <= 0 or sls_sales != sls_quantity*abs(sls_price) then  sls_quantity*sls_price ---handling invalid and missing data
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null then sls_sales/nullif(sls_quantity,0)
			 when sls_price <= 0 then sls_sales/nullif(sls_quantity,0)
			 else sls_price
		end as sls_price
		from bronze.crm_sales_details
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		print 'Loading ERP Tables------------------'
		print '>> Truncating table : silver.erp_cust_az12';
		set @start_time  = GETDATE();
		truncate table silver.erp_cust_az12
		print '>> Inserting data into : silver.erp_cust_az12'
		insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		select 
			case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) --- Handle invalid values
				 else cid
			end as cid,
			case when bdate > getdate() then null --- handling future values as null
				else bdate
			end as bdate,
			CASE WHEN UPPER(TRIM(gen)) LIKE '%F%'  THEN 'Female'
				 WHEN UPPER(TRIM(gen)) LIKE '%M%' THEN 'Male'
				 ELSE 'N/A'
			END AS gen --- Normalize gender values and handle unknown cases 
		from bronze.erp_cust_az12
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		print '>> Truncating table : silver.erp_loc_a101';
		set @start_time  = GETDATE();
		truncate table silver.erp_loc_a101
		print '>> Inserting data into : silver.erp_loc_a101'
		insert into silver.erp_loc_a101 (
			cid,
			cntry
		)
		select 
		REPLACE(cid,'-','') cid,
		case when UPPER(LTRIM(RTRIM(cntry))) like 'DE%' then 'Germany'
			 WHEN UPPER(LTRIM(RTRIM(cntry))) like 'US%' THEN 'United States'
			 when ltrim(rtrim(cntry)) like '_' or cntry is null then 'n/a'
			 else TRIM(cntry)
		end as cntry
		from bronze.erp_loc_a101
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		print '>> Truncating table : silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2
		print '>> Inserting data into : silver.erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
		print 'Load duration for silver layer: ' + cast(datediff(second,@batch_start_time,@batch_end_time) AS NVARCHAR)
	end try
	begin catch 
		print '----------Error Occured-------------' + ERROR_MESSAGE()
		print 'Error----->' + cast(error_number() as nvarchar)
		print 'Error state------>' + cast(error_state() as Nvarchar)
	end catch
END

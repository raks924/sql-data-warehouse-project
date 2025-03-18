/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
create or alter procedure bronze.load_bronze as 
begin
	declare @start_time Datetime , @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime
	begin try
		set @batch_start_time  = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze tables data'

		print '--------------------------------------'
		print 'Loading source system crm data'
		set @start_time  = GETDATE();
		truncate table bronze.crm_cust_info
		bulk insert bronze.crm_cust_info
		from '/data/source_crm/cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ','
		);
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		set @start_time  = GETDATE();
		truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from '/data/source_crm/prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ','
		);
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		set @start_time  = GETDATE();
		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from '/data/source_crm/sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ','
		);
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

	
		print '--------------------------------------'
		print 'Loading source system erp data'
		set @start_time  = GETDATE();
		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from '/data/source_erp/cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ','
		);
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		set @start_time  = GETDATE();
		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from '/data/source_erp/loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ','
		);
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'

		set @start_time  = GETDATE();
		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from '/data/source_erp/px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ','
		);
		set @end_time  = GETDATE();
		print 'load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
		print '-------------------------------------------------'
		set @batch_end_time  = GETDATE();
		print 'load duration for entire batch : ' + cast(datediff(second, @batch_start_time,@batch_end_time) as nvarchar) + 'seconds'
	end try
	begin catch 
		print '==============================='
		print 'error occured during loading data in bronze layer'
		print 'error number' + cast(error_number() as nvarchar)
		print 'error state' + cast(error_state() as nvarchar)
		print '==============================='
	end catch
end

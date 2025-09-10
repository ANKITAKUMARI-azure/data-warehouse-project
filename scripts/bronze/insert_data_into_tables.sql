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

Create OR Alter Procedure bronze.load_bronze As
Begin
	Declare @startTime DateTime, @endTime DateTime
	Begin Try

		PRINT	'===================================================================='
		PRINT	'Bronze Layer Data Load'
		PRINT	'===================================================================='
		
		PRINT	'===================================================================='
		PRINT	'Source: CRM'
		PRINT	'===================================================================='
		
		PRINT	'--------------------------------------------------------------------'
		PRINT	'Table: bronze.crm_cust_info'
		PRINT	'--------------------------------------------------------------------'

		set @startTime = GETDATE()
		PRINT	'>> Truncating bronze.crm_cust_info table'
		Truncate table bronze.crm_cust_info;
		PRINT	'>> Bulk Inserting data into bronze.crm_cust_info table'
		Bulk Insert bronze.crm_cust_info 
		From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endTime = GETDATE()
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds'
		
		
		PRINT	'--------------------------------------------------------------------'
		PRINT	'Table: bronze.crm_prd_info'
		PRINT	'--------------------------------------------------------------------'

		set @startTime = GETDATE()
		PRINT	'>> Truncating bronze.crm_prd_info table'
		Truncate table bronze.crm_prd_info;
		PRINT	'>> Bulk Inserting data into bronze.crm_prd_info table'
		Bulk Insert bronze.crm_prd_info
		From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endTime = GETDATE()
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds'

		
		PRINT	'--------------------------------------------------------------------'
		PRINT	'Table: bronze.crm_sales_details'
		PRINT	'--------------------------------------------------------------------'

		set @startTime = GETDATE()
		PRINT	'>> Truncating bronze.crm_sales_details table'
		Truncate table bronze.crm_sales_details;
		PRINT	'>> Bulk Inserting data into bronze.crm_sales_details table'
		Bulk Insert bronze.crm_sales_details
		From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endTime = GETDATE()
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds'


		PRINT	'===================================================================='
		PRINT	'Source: ERP'
		PRINT	'===================================================================='
		
		PRINT	'--------------------------------------------------------------------'
		PRINT	'Table: bronze.erp_cust_az12'
		PRINT	'--------------------------------------------------------------------'

		set @startTime = GETDATE()
		PRINT	'>> Truncating bronze.erp_cust_az12 table'
		Truncate table bronze.erp_cust_az12;
		PRINT	'>> Bulk Inserting data into bronze.erp_cust_az12 table'
		Bulk Insert bronze.erp_cust_az12
		From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		With (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endTime = GETDATE()
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds'

		
		PRINT	'--------------------------------------------------------------------'
		PRINT	'Table: bronze.erp_px_cat_g1v2'
		PRINT	'--------------------------------------------------------------------'

		set @startTime = GETDATE()
		PRINT	'>> Truncating bronze.erp_px_cat_g1v2 table'
		Truncate table bronze.erp_px_cat_g1v2;
		PRINT	'>> Bulk Inserting data into bronze.crm_cust_info table'
		Bulk Insert bronze.erp_px_cat_g1v2
		From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		With (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endTime = GETDATE()
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds'

		
		PRINT	'--------------------------------------------------------------------'
		PRINT	'Table: bronze.erp_loc_a101'
		PRINT	'--------------------------------------------------------------------'

		set @startTime = GETDATE()
		PRINT	'>> Truncating bronze.erp_loc_a101 table'
		Truncate table bronze.erp_loc_a101;
		PRINT	'>> Bulk Inserting data into bronze.erp_loc_a101'
		Bulk Insert bronze.erp_loc_a101
		From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		With (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @endTime = GETDATE()
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds'

	End Try

	Begin Catch
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	End Catch

End;

Exec bronze.load_bronze;

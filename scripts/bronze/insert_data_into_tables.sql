/*
===============================================================================
Bulk Insert Data Into Table: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
===============================================================================
*/

Truncate table bronze.crm_cust_info;
Go

Bulk Insert bronze.crm_cust_info 
From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
With (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
Go

select * from bronze.crm_cust_info;
Go
--------------------------------------------------------------------

Truncate table bronze.crm_prd_info;
Go

Bulk Insert bronze.crm_prd_info
From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
With (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
Go

select * from bronze.crm_prd_info;
Go
--------------------------------------------------------------------

Truncate table bronze.crm_sales_details;
Go

Bulk Insert bronze.crm_sales_details
From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
With (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
Go

select * from bronze.crm_sales_details;
Go
--------------------------------------------------------------------

Truncate table bronze.erp_cust_az12;
Go

Bulk Insert bronze.erp_cust_az12
From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
With (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
Go

select * from bronze.erp_cust_az12;
Go
--------------------------------------------------------------------

Truncate table bronze.erp_px_cat_g1v2;
Go

Bulk Insert bronze.erp_px_cat_g1v2
From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
With (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
Go

select * from bronze.erp_px_cat_g1v2;
Go
--------------------------------------------------------------------

Truncate table bronze.erp_loc_a101;
Go

Bulk Insert bronze.erp_loc_a101
From 'C:\Users\Nikhil Kumar\OneDrive\Desktop\MasterClass\Azure\DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
With (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
Go

select * from bronze.erp_loc_a101;
Go

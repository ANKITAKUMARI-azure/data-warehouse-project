/*
===============================================================================
Stored Procedure: Load Silver Layer (Source -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from bronze table after transforming the data. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `INSERT INTO` command to load data from bronze tables to silver table after transforming the data.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

create or alter Procedure silver.load_silver as 
begin
	Declare @startTime datetime, @endTime datetime, @batchStartTime datetime, @batchEndTime datetime
	Begin try
		set @batchStartTime = GETDATE();

		PRINT '========================================================';
		PRINT 'Loading Silver layer';
		PRINT '========================================================';
		PRINT '========================================================';
		PRINT 'Loading transformed data into silver.crm_cust_info';
		PRINT '========================================================';

		set @startTime = GETDATE();
		TRUNCATE TABLE [silver].[crm_cust_info];

		INSERT INTO [silver].[crm_cust_info]
           ([cst_id]
           ,[cst_key]
           ,[cst_firstname]
           ,[cst_lastname]
           ,[cst_marital_status]
           ,[cst_gndr]
           ,[cst_create_date]
           ,[dwh_created_date])
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'N/A'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'N/A'
			END AS cst_gndr,
			cst_create_date,
			GETDATE()
		FROM (
			SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_flag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) AS sub
		WHERE latest_flag = 1;
		set @endTime = GETDATE();
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds';

		set @startTime = GETDATE();
		PRINT '========================================================';
		PRINT 'Loading transformed data into silver.crm_prd_info';
		PRINT '========================================================';

		TRUNCATE TABLE [silver].[crm_prd_info];

		INSERT INTO [silver].[crm_prd_info]
           ([prd_id]
           ,[cat_id]
           ,[prd_key]
           ,[prd_nm]
           ,[prd_cost]
           ,[prd_line]
           ,[prd_start_dt]
           ,[prd_end_dt]
		   ,dw_created_date)
		   
		SELECT prd_id
			  ,replace(left(prd_key,5),'-','_') as cat_id
			  ,substring(prd_key,7,LEN(prd_key)) as prd_key
			  ,trim(prd_nm) as prd_nm
			  ,isnull(prd_cost,0) as prd_cost
			  ,case upper(trim(prd_line))
					when 'M' then 'Mountain'
					when 'R' then 'Road'
					when 'S' then 'Other Sales'
					when 'T' then 'Touring'
					else 'N/A'
				end as prd_line
			  ,cast (prd_start_dt as DATE)
			  ,cast (lead(prd_start_dt) over(partition by prd_key order by prd_start_dt asc) - 1 as DATE) as prd_end_dt
			  ,getdate()
		FROM bronze.crm_prd_info;

		set @endTime = GETDATE();
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds';

		set @startTime = GETDATE();
		PRINT '========================================================';
		PRINT 'Loading transformed data into silver.crm_sales_details';
		PRINT '========================================================';

		TRUNCATE TABLE [silver].[crm_sales_details];
		
		INSERT INTO [silver].[crm_sales_details]
           ([sls_ord_num]
           ,[sls_prd_key]
           ,[sls_cust_id]
           ,[sls_order_dt]
           ,[sls_ship_dt]
           ,[sls_due_dt]
           ,[sls_sales]
           ,[sls_quantity]
           ,[sls_price]
           ,[dw_created_date])
		SELECT sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id
			  ,case when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
					else cast(cast(sls_order_dt as varchar) as Date) 
					end as sls_order_dt
			  ,case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
					else cast(cast(sls_ship_dt as varchar) as Date) 
					end as sls_ship_dt
			  ,case when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
					else cast(cast(sls_due_dt as varchar) as Date) 
					end as sls_due_dt
			  ,sls_quantity
			  ,case when sls_price <0 or sls_price is null then  (sls_sales/nullif(sls_quantity,0))
					else sls_price
				end as sls_price
			  ,case when sls_sales != (abs(sls_price)*sls_quantity) or sls_sales is null or sls_sales <=0 then (abs(sls_price)*sls_quantity)
					else sls_sales
				end as sls_sales
			,GETDATE() 
		  FROM bronze.crm_sales_details;

		set @endTime = GETDATE();
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds';

		set @startTime= GETDATE();
		PRINT '========================================================';
		PRINT 'Loading transformed data into silver.erp_cust_az12';
		PRINT '========================================================';

		TRUNCATE TABLE [silver].[erp_cust_az12];

		INSERT INTO [silver].[erp_cust_az12]
           ([cid]
           ,[bdate]
           ,[gen]
           ,[dwh_created_date])

		select 
			replace(trim(cid),'NAS','') as cid
			,cast(trim(cast(bdate as varchar)) as DATE) as bdate
			,case	when upper(trim(gen))='M' or upper(trim(gen))='MALE' then 'Male'
					when upper(trim(gen))='F' or upper(trim(gen))='FEMALE' then 'Female'
					else 'N/A'
			end as gen
			,GETDATE()
		from bronze.erp_cust_az12;

		set @endTime = GETDATE();
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds';

		set @startTime = GETDATE();

		PRINT '========================================================';
		PRINT 'Loading transformed data into silver.erp_loc_a101';
		PRINT '========================================================';

		TRUNCATE TABLE [silver].[erp_loc_a101];

		INSERT INTO [silver].[erp_loc_a101]
           ([cid]
           ,[cntry]
           ,[dwh_created_date])
		select 
				replace((trim(cid)),'-','') as cid,
				case when upper(trim(cntry))='US' or upper(trim(cntry))='USA' or upper(trim(cntry))='UNITED STATES' then 'United States'
					when upper(trim(cntry))='DE' or upper(trim(cntry))='GERMANY' then 'Germany'
					when cntry is NULL or upper(trim(cntry))='  ' then 'N/A'
					else cntry
				end as cntry 
				,GETDATE()
		from bronze.erp_loc_a101;

		set @endTime = GETDATE();
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds';

		set @startTime = GETDATE();
		PRINT '========================================================';
		PRINT 'Loading transformed data into silver.erp_px_cat_g1v2';
		PRINT '========================================================';

		TRUNCATE TABLE [silver].[erp_px_cat_g1v2];

		INSERT INTO [silver].[erp_px_cat_g1v2]
           ([id]
           ,[cat]
           ,[subcat]
           ,[maintenance]
           ,[dwh_created_date])
		select 
			id,
			trim(cat) as cat,
			trim(subcat) as subcat,
			trim(maintenance) as maintenance,
			GETDATE()
		from bronze.[erp_px_cat_g1v2];

		set @endTime = GETDATE();
		PRINT	'>> Load Duration: '+ Cast(datediff(second,@startTime,@endTime) AS NVARCHAR) + ' seconds';

		set @batchEndTime = GETDATE();
		PRINT '========================================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT	'>> Complete Batch Load Duration: '+ Cast(datediff(second,@batchStartTime,@batchEndTime) AS NVARCHAR) + ' seconds';
		PRINT '========================================================';

	End Try
	Begin Catch
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	End Catch

end

EXEC silver.load_silver;

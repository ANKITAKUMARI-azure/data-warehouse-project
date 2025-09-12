select * from bronze.crm_sales_details;

------------------------------------------------------------------------------------------------------
--checking the null and duplicate values in primary key
------------------------------------------------------------------------------------------------------

select sls_ord_num from bronze.crm_sales_details where sls_ord_num is null;
select sls_prd_key from bronze.crm_sales_details where sls_prd_key is null;

------------------------------------------------------------------------------------------------------
--checking all the date columns and making it as a date data type instead of integer
------------------------------------------------------------------------------------------------------

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
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM bronze.crm_sales_details

------------------------------------------------------------------------------------------------------
--checking if sales as per business rule is equal to price*quantity. ALso it should not be -ve, 0 or null
------------------------------------------------------------------------------------------------------

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
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM bronze.crm_sales_details
  WHERE sls_sales is null or sls_quantity is null or sls_price is null
  or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
  or sls_sales != (sls_price*sls_quantity) 


---Tranforming the data 

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
  FROM bronze.crm_sales_details

------------------------------------------------------------------------------------------------------
--to insert the correct data ainto silver table after transforming and cleaning
------------------------------------------------------------------------------------------------------

Drop table silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
	[sls_ord_num] [nvarchar](50) NULL,
	[sls_prd_key] [nvarchar](50) NULL,
	[sls_cust_id] [int] NULL,
	[sls_order_dt] date NULL,
	[sls_ship_dt] date NULL,
	[sls_due_dt] date NULL,
	[sls_sales] [int] NULL,
	[sls_quantity] [int] NULL,
	[sls_price] [int] NULL,
	dw_created_date datetime
) ON [PRIMARY]

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

  ------------------------------------------------------------------------------------------------------
--Final validation for the silver layer table
------------------------------------------------------------------------------------------------------

select * from silver.crm_sales_details;

select * from silver.crm_sales_details where sls_ord_num is null or sls_prd_key is null or sls_cust_id is null;




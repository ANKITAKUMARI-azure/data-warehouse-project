select * from bronze.crm_prd_info;

------------------------------------------------------------------------------------------------------
--checking the null and duplicate values in primary key
------------------------------------------------------------------------------------------------------

select prd_id, count(*) from bronze.crm_prd_info group by prd_id having count(*)>1;

------------------------------------------------------------------------------------------------------
--Dividing the product key into 2 parts: cat_id[4] and product_key
------------------------------------------------------------------------------------------------------

SELECT prd_id
      ,replace(left(prd_key,5),'-','_') as cat_id
	  ,substring(prd_key,7,LEN(prd_key)) as prd_key
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
  FROM bronze.crm_prd_info;

------------------------------------------------------------------------------------------------------
--checking if prd_nm doesn't have spaces at start and end
------------------------------------------------------------------------------------------------------

SELECT prd_id
      ,replace(left(prd_key,5),'-','_') as cat_id
	  ,substring(prd_key,7,LEN(prd_key)) as prd_key
      ,trim(prd_nm) as prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
FROM bronze.crm_prd_info;

------------------------------------------------------------------------------------------------------
--checking if prd_cost have any invalid values [0 or null]
------------------------------------------------------------------------------------------------------

select * from bronze.crm_prd_info where prd_cost < 0 or prd_cost is null

-- To fix the issue we will use below command

SELECT prd_id
      ,replace(left(prd_key,5),'-','_') as cat_id
	  ,substring(prd_key,7,LEN(prd_key)) as prd_key
      ,trim(prd_nm) as prd_nm
      ,isnull(prd_cost,0) as prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
FROM bronze.crm_prd_info;

------------------------------------------------------------------------------------------------------
--checking the cardinaity of prd_line
------------------------------------------------------------------------------------------------------

select distinct prd_line from bronze.crm_prd_info;

-- To fix the issue we will use below command

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
      ,prd_start_dt
      ,prd_end_dt
FROM bronze.crm_prd_info;

------------------------------------------------------------------------------------------------------
--checking the invalid dates in prd_start_dt and prd_end_dt
------------------------------------------------------------------------------------------------------

select * from bronze.crm_prd_info where prd_end_dt < prd_start_dt

------------------------------------------------------------------------------------------------------
--fixing the date issues.
------------------------------------------------------------------------------------------------------

with cte as (
	select * ,
	lead(prd_start_dt) over(partition by prd_key order by prd_start_dt asc) - 1 as expected_end_date
	from bronze.crm_prd_info
)
select * from cte

--fixing the date issues.

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
FROM bronze.crm_prd_info;

------------------------------------------------------------------------------------------------------
--to insert the correct data ainto silver table after transforming and cleaning
------------------------------------------------------------------------------------------------------
DROP TABLE [silver].[crm_prd_info];

CREATE TABLE silver.[crm_prd_info](
	[prd_id] [int] NULL,
	[cat_id] [nvarchar](50) NULL,
	[prd_key] [nvarchar](50) NULL,
	[prd_nm] [nvarchar](50) NULL,
	[prd_cost] [int] NULL,
	[prd_line] [nvarchar](50) NULL,
	[prd_start_dt] [date] NULL,
	[prd_end_dt] [date] NULL,
	dw_created_date [datetime] NULL
) ON [PRIMARY]

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

------------------------------------------------------------------------------------------------------
--Final validation for the silver layer table
------------------------------------------------------------------------------------------------------

select * from silver.crm_prd_info;

select prd_id,count(*) from silver.crm_prd_info group by prd_id having count(*)>1 or prd_id is null;

select distinct prd_line from silver.crm_prd_info;

select * from silver.crm_prd_info where prd_start_dt > prd_end_dt;


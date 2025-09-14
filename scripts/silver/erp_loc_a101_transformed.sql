SELECT  distinct [cid]
      ,[cntry]
  FROM [bronze].[erp_loc_a101];

------------------------------------------------------------------------------------------------------
--checking the null and duplicate values in primary key
------------------------------------------------------------------------------------------------------

select * from [bronze].[erp_loc_a101] where cid is null;

-----Tranforming the data 

select 
replace((trim(cid)),'-','') as cid,
cntry 
from [bronze].[erp_loc_a101];

------------------------------------------------------------------------------------------------------
--checking the values in country column
------------------------------------------------------------------------------------------------------

SELECT  distinct
cntry 
FROM [bronze].[erp_loc_a101];

-----Tranforming the data 

select 
replace((trim(cid)),'-','') as cid,
case when upper(trim(cntry))='US' or upper(trim(cntry))='USA' or upper(trim(cntry))='UNITED STATES' then 'United States'
	when upper(trim(cntry))='DE' or upper(trim(cntry))='GERMANY' then 'Germany'
	when cntry is NULL or upper(trim(cntry))='  ' then 'N/A'
	else cntry
end as cntry 
from [bronze].[erp_loc_a101];

------------------------------------------------------------------------------------------------------
--to insert the correct data ainto silver table after transforming and cleaning
------------------------------------------------------------------------------------------------------

Drop table silver.[erp_loc_a101];

CREATE TABLE [silver].[erp_loc_a101](
	[cid] [nvarchar](50) NULL,
	[cntry] [nvarchar](50) NULL,
	[dwh_created_date] [datetime] NULL
) ON [PRIMARY];

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

------------------------------------------------------------------------------------------------------
--Final validation for the silver layer table
------------------------------------------------------------------------------------------------------

select * from silver.erp_loc_a101;

select distinct cntry from silver.erp_loc_a101;

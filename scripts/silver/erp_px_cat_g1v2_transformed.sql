SELECT [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
  FROM [bronze].[erp_px_cat_g1v2];

select * from silver.crm_prd_info;

------------------------------------------------------------------------------------------------------
--checking the null and duplicate values in primary key
------------------------------------------------------------------------------------------------------

select * from [bronze].[erp_px_cat_g1v2] where id is null;

------------------------------------------------------------------------------------------------------
--checking the category, subcategory values
------------------------------------------------------------------------------------------------------

select distinct cat from [bronze].[erp_px_cat_g1v2];
select distinct subcat from [bronze].[erp_px_cat_g1v2];
select distinct maintenance from [bronze].[erp_px_cat_g1v2];

-----Tranforming the data 

select id,
trim(cat) as cat,
trim(subcat) as subcat,
trim(maintenance) as maintenance
from [bronze].[erp_px_cat_g1v2] where id not in (select cat_id from silver.crm_prd_info);

------------------------------------------------------------------------------------------------------
--to insert the correct data ainto silver table after transforming and cleaning
------------------------------------------------------------------------------------------------------

Drop table silver.[erp_px_cat_g1v2];

CREATE TABLE [silver].[erp_px_cat_g1v2](
	[id] [nvarchar](50) NULL,
	[cat] [nvarchar](50) NULL,
	[subcat] [nvarchar](50) NULL,
	[maintenance] [nvarchar](50) NULL,
	[dwh_created_date] [datetime] NULL
) ON [PRIMARY];

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

------------------------------------------------------------------------------------------------------
--Final validation for the silver layer table
------------------------------------------------------------------------------------------------------

select * from silver.[erp_px_cat_g1v2];

select * from silver.[erp_px_cat_g1v2] where id is null;

select distinct cat from silver.[erp_px_cat_g1v2];

select distinct subcat from silver.[erp_px_cat_g1v2];

select distinct maintenance from silver.[erp_px_cat_g1v2];

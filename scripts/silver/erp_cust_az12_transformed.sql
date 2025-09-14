select * from bronze.erp_cust_az12;

select * from silver.crm_cust_info;
------------------------------------------------------------------------------------------------------
--checking the null and duplicate values in primary key
------------------------------------------------------------------------------------------------------

select * from [bronze].[erp_cust_az12] where cid is null;
select cid,count(*) from [bronze].[erp_cust_az12] group by cid having count(*)>1;

------------------------------------------------------------------------------------------------------
--checking the cid and fixing the issue
------------------------------------------------------------------------------------------------------

select 
	replace(trim(cid),'NAS','') as cid
	,[bdate]
	,gen
from bronze.erp_cust_az12;

------------------------------------------------------------------------------------------------------
--checking the distinct value in gender
------------------------------------------------------------------------------------------------------

select distinct gen from bronze.erp_cust_az12;

---Tranforming the data 

select 
	replace(trim(cid),'NAS','') as cid
	,[bdate]
	,case	when upper(trim(gen))='M' or upper(trim(gen))='MALE' then 'Male'
			when upper(trim(gen))='F' or upper(trim(gen))='FEMALE' then 'Female'
			else 'N/A'
	end as gen
from bronze.erp_cust_az12;

------------------------------------------------------------------------------------------------------
--checking the date fields
------------------------------------------------------------------------------------------------------
select bdate from bronze.erp_cust_az12 where bdate is null or (YEAR(bdate)<='1924' and YEAR(bdate)>= YEAR(GETDATE()));

---Tranforming the data 

select 
	replace(trim(cid),'NAS','') as cid
	,cast(trim(cast(bdate as varchar)) as DATE) as bdate
	,case	when upper(trim(gen))='M' or upper(trim(gen))='MALE' then 'Male'
			when upper(trim(gen))='F' or upper(trim(gen))='FEMALE' then 'Female'
			else 'N/A'
	end as gen
from bronze.erp_cust_az12;

------------------------------------------------------------------------------------------------------
--to insert the correct data ainto silver table after transforming and cleaning
------------------------------------------------------------------------------------------------------

Drop table silver.[erp_cust_az12];

CREATE TABLE [silver].[erp_cust_az12](
	[cid] [nvarchar](50) NULL,
	[bdate] [date] NULL,
	[gen] [nvarchar](50) NULL,
	[dwh_created_date] [datetime] NULL
) ON [PRIMARY];


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

------------------------------------------------------------------------------------------------------
--Final validation for the silver layer table
------------------------------------------------------------------------------------------------------

select * from silver.erp_cust_az12;

select * from silver.erp_cust_az12 where cid is null or bdate is null or gen is null;

select distinct gen from silver.erp_cust_az12;

select * from bronze.crm_cust_info

------------------------------------------------------------------------------------------------------
--checking the null and duplicate values in primary key
------------------------------------------------------------------------------------------------------

select cst_id,count(*) from bronze.crm_cust_info group by cst_id having count(*)>1 or cst_id is null;

-- To transform the issue we will use below command

with cte as (
	select 
	*,
	row_number() over(partition by cst_id order by cst_create_date desc) as latest_flag
	from bronze.crm_cust_info where cst_id is not null 
)
select * from cte where latest_flag = 1;

------------------------------------------------------------------------------------------------------
--checking the unwanted spaces in columns 
------------------------------------------------------------------------------------------------------

select cst_firstname, cst_lastname from bronze.crm_cust_info 
where cst_firstname != trim(cst_firstname) or cst_lastname != trim(cst_lastname) ;

-- To transform the issue we will use below command

with cte as (
	select 
	*,
	row_number() over(partition by cst_id order by cst_create_date desc) as latest_flag
	from bronze.crm_cust_info where cst_id is not null 
)
SELECT 
	cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    trim(cst_marital_status) as cst_marital_status,
    trim(cst_gndr) as cst_gndr,
	cst_create_date
FROM cte WHERE latest_flag = 1;

------------------------------------------------------------------------------------------------------
--checking the data consistency 
------------------------------------------------------------------------------------------------------

select distinct cst_gndr from bronze.crm_cust_info ;
select distinct cst_marital_status from bronze.crm_cust_info;


-- To transform the issue we will use below command

with cte as (
	select 
	*,
	row_number() over(partition by cst_id order by cst_create_date desc) as latest_flag
	from bronze.crm_cust_info where cst_id is not null 
)

SELECT 
	cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case	when upper(trim(cst_marital_status)) = 'S' then 'Single'
			when upper(trim(cst_marital_status)) = 'M' then 'Married'
			else 'NA'
	end as cst_marital_status,
    case	when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'NA'
	end as cst_gndr,
	cst_create_date
FROM cte WHERE latest_flag = 1;

------------------------------------------------------------------------------------------------------
--checking the data in date field is valid or not
------------------------------------------------------------------------------------------------------

select 
	cst_create_date,
	case  WHEN cst_create_date IS NULL THEN 'Invalid'
		 else 'Valid'
	end as flag
from bronze.crm_cust_info;

------------------------------------------------------------------------------------------------------
--to insert the correct data ainto silver table after transforming and cleaning
------------------------------------------------------------------------------------------------------

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
        ELSE 'NA'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'NA'
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

select * from silver.[crm_cust_info];

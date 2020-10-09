set @ttm=(SELECT trailing_months FROM {{ params.processing_db_name }}.fc_config_fact);
#select @ttm;

set @analysis_end_date=(SELECT ifnull(end_date,current_date-1) FROM {{ params.processing_db_name }}.fc_config_fact);
#select @analysis_end_date;

set @start_date=(SELECT start_date FROM {{ params.processing_db_name }}.fc_config_fact);
#select @start_date;


set @analysis_month=(SELECT trailing_months_period FROM {{ params.processing_db_name }}.fc_config_fact);
#select @analysis_month;


set @ttm_analysis_start_date=(select date_format((select date_sub(@analysis_end_date,interval @analysis_month-1 month)),'%Y-%m-01'));
#select @ttm_analysis_start_date;

set @analysis_start_date=(select case when @ttm='yes' then @ttm_analysis_start_date else @start_date end analysis_start_date);
#select @analysis_start_date;

truncate table  {{ params.master_db_name }}.fc_data_analysis_present;
insert into {{ params.master_db_name }}.fc_data_analysis_present(description,label,value)
select "Run Date" as description,null as value, current_timestamp() as value
union all

-- Number of Accounts in Account Master
select "Number of Accounts in Account Master" as description,null as label,
count(distinct account_number) as value 
from {{ params.raw_db_name }}.fc_account_master

union all
-- Number of Accounts in Transaction Base
select "Number of Accounts in Transaction Base" as description,null as label,
count(distinct account_number) as value 
from {{ params.raw_db_name }}.fc_transaction_base



union all
-- Number of Accounts in Balance Summary
select "Number of Accounts in Balance Summary" as description,null as label,
count(distinct account_number) as value 
from {{ params.raw_db_name }}.fc_balance_summary



union all
-- Number of Customers having Loan
select "Number of Customers having Loan" as description,null as label,
count(distinct customer_code) as value 
from {{ params.raw_db_name }}.fc_loan_facts


union all
-- Number of Active Accounts in Account Master 
select "Number of Active Accounts in Account Master" as description,null as label,
count(distinct account_number) as value 
from {{ params.raw_db_name }}.fc_account_master where active_flag=1

union all
-- Number of Salary Accounts Identified 
select "Number of Salary Accounts Identified" as description, null as label,
count(1) from {{ params.processing_db_name }}.fc_credit_score_fact_metrics



union all
-- Number of Salary Accounts having Loan
select "Number of Salary Accounts having Loan" as description,null as label,
 count(distinct a.account_number) FROM 
{{ params.raw_db_name }}.fc_transaction_base a
inner join {{ params.raw_db_name }}.fc_account_master b
on a.account_number=b.account_number
inner join {{ params.raw_db_name }}.fc_loan_facts c
on b.customer_code=c.customer_code where is_salary=1

union all
-- analysis date range
select "Analysis date range" as description,null as label,
concat(date(@analysis_start_date),"-to-",date(@analysis_end_date)) as value

union all
-- Number of Transactions considered for analysis (rows) 
select "Number of Transactions considered for analysis" as description, null as label,
count(1) as value from {{ params.raw_db_name }}.fc_transaction_base 
where tran_date>=@analysis_start_date and tran_date<=@analysis_end_date
union all

-- Number of Balance History considered for analysis (rows) 
select "Number of Balance History considered for analysis" as description, null as label,
count(1) as value from {{ params.raw_db_name }}.fc_balance_summary 
where tran_date>=@analysis_start_date and tran_date<=@analysis_end_date


union all
-- Number of Eligible Accounts for Analysis  
select "Number of Eligible Accounts for Analysis" as description, null as label,
count(1) from {{ params.processing_db_name }}.fc_credit_score_fact


union all

-- Number of Eligible Accounts by scheme type for Analysis
select "Number of Eligible Accounts by scheme type for Analysis" as description, product as label,
count(1) as value from {{ params.processing_db_name }}.fc_credit_score_fact
group by product
union all

-- Number of Eligible Accounts for Fone Loan
select "Number of Eligible Accounts for Fone Loan" as description, null as label,
count(1) from {{ params.processing_db_name }}.fc_credit_score
union all

select "Number of Eligible Accounts for Fone Loan (CP>20)" as description, null as label,
count(1) as value from {{ params.processing_db_name }}.fc_credit_score where confidence_percentage>20
union all

select "Number of Eligible Accounts for Fone Loan (CP>30)" as description, null as label,
count(1) as value from {{ params.processing_db_name }}.fc_credit_score where confidence_percentage>30
union all

select "Number of Eligible Accounts for Fone Loan (CP>40)" as description, null as label,
count(1) as value from {{ params.processing_db_name }}.fc_credit_score where confidence_percentage>40
union all

select "Number of Eligible Accounts for Fone Loan (CP>50)" as description, null as label,
count(1) as value from {{ params.processing_db_name }}.fc_credit_score where confidence_percentage>50
union all
select "Maximum Potential Fone Loan Disbursement" as description,null as label,
 sum(max_fone_loan) as value from 
(
select *,
case
when average_salary>100000 then 100000 else average_salary end as max_fone_loan from {{ params.processing_db_name }}.fc_credit_score
)m
union all
select "Maximum Potential Fone Loan Disbursement (CP>20)" as description,null as label,
 sum(max_fone_loan) as value from 
(
select *,
case
when average_salary>100000 then 100000 else average_salary end as max_fone_loan from {{ params.processing_db_name }}.fc_credit_score
where confidence_percentage>20
)m
union all
select "Maximum Potential Fone Loan Disbursement (CP>30)" as description,null as label,
 sum(max_fone_loan) as value from 
(
select *,
case
when average_salary>100000 then 100000 else average_salary end as max_fone_loan from {{ params.processing_db_name }}.fc_credit_score
where confidence_percentage>30
)m
union all
select "Maximum Potential Fone Loan Disbursement (CP>40)" as description,null as label,
 sum(max_fone_loan) as value from 
(
select *,
case
when average_salary>100000 then 100000 else average_salary end as max_fone_loan from {{ params.processing_db_name }}.fc_credit_score
where confidence_percentage>40
)m
union all
select "Maximum Potential Fone Loan Disbursement (CP>50)" as description,null as label,
 sum(max_fone_loan) as value from 
(
select *,
case
when average_salary>100000 then 100000 else average_salary end as max_fone_loan from {{ params.processing_db_name }}.fc_credit_score
where confidence_percentage>50
)m;



DROP TEMPORARY TABLE IF EXISTS  {{ params.master_db_name }}.fc_data_analysis_joined;

create temporary table {{ params.master_db_name }}.fc_data_analysis_joined as 
SELECT a.description,a.label FROM 
{{ params.master_db_name }}.fc_data_analysis a
inner join {{ params.master_db_name }}.fc_data_analysis_present b
on a.description=b.description
and ifnull(a.label,0)=ifnull(b.label,0);

SET SQL_SAFE_UPDATES=0;
update {{ params.master_db_name }}.fc_data_analysis set last2_run=last1_run;
update {{ params.master_db_name }}.fc_data_analysis set last1_run=null;
update {{ params.master_db_name }}.fc_data_analysis set last1_run=this_run;
update {{ params.master_db_name }}.fc_data_analysis set this_run=null;


update {{ params.master_db_name }}.fc_data_analysis   a
inner join {{ params.master_db_name }}.fc_data_analysis_present b
on a.description=b.description
and IFNULL(a.label,0)=IFNULL(b.label,0)
set a.this_run= b.value;


insert into {{ params.master_db_name }}.fc_data_analysis(description,label,this_run)
select a.description,a.label,a.value from  {{ params.master_db_name }}.fc_data_analysis_present a
left join {{ params.master_db_name }}.fc_data_analysis_joined b
on a.description=b.description
and IFNULL(a.label,0)=IFNULL(b.label,0)
where b.description is null
;


delete from {{ params.master_db_name }}.fc_data_analysis
where last2_run is null
and last1_run is null
and this_run is null;

update {{ params.master_db_name }}.fc_data_analysis set diff_this_last1=this_run-last1_run
where description not in ('Run Date','Analysis date range');


update {{ params.master_db_name }}.fc_data_analysis set diff_this_last1=datediff(date(this_run),date(last1_run))
where description in ('Run Date');


update {{ params.master_db_name }}.fc_data_analysis set diff_last1_last2=last1_run-last2_run
where description not in ('Run Date','Analysis date range');

update {{ params.master_db_name }}.fc_data_analysis set diff_last1_last2=datediff(date(last1_run),date(last2_run))
where description in ('Run Date');

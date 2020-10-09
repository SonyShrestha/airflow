set @today_date=current_timestamp();
set sql_safe_updates=0;
drop temporary table if exists {{ params.master_db_name }}.fc_account_score_limit;

create temporary table {{ params.master_db_name }}.fc_account_score_limit as 
select account_number,average_salary,confidence_percentage,
case when average_salary>100000 then 100000 else average_salary end as max_fone_loan_limit
 FROM {{ params.app_db_name }}.fc_credit_score;


DROP TEMPORARY TABLE IF EXISTS  {{ params.master_db_name }}.fc_account_joined;

create temporary table {{ params.master_db_name }}.fc_account_joined as 
SELECT a.account_number FROM 
{{ params.master_db_name }}.fc_account_score_limit a
inner join {{ params.master_db_name }}.fc_cp_past_5_runs b
on a.account_number=b.account_number;


update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_last4=cp_last3;
update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_last3=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_last3=cp_last2;
update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_last2=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_last2=cp_last1;
update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_last1=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_last1=cp_this;
update {{ params.master_db_name }}.fc_cp_past_5_runs set cp_this=null;

update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_last4=limit_last3;
update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_last3=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_last3=limit_last2;
update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_last2=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_last2=limit_last1;
update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_last1=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_last1=limit_this;
update {{ params.master_db_name }}.fc_cp_past_5_runs set limit_this=null;

update {{ params.master_db_name }}.fc_cp_past_5_runs set last4_run_date=last3_run_date;
update {{ params.master_db_name }}.fc_cp_past_5_runs set last3_run_date=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set last3_run_date=last2_run_date;
update {{ params.master_db_name }}.fc_cp_past_5_runs set last2_run_date=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set last2_run_date=last1_run_date;
update {{ params.master_db_name }}.fc_cp_past_5_runs set last1_run_date=null;
update {{ params.master_db_name }}.fc_cp_past_5_runs set last1_run_date=this_run_date;
update {{ params.master_db_name }}.fc_cp_past_5_runs set this_run_date=null;


set sql_safe_updates=0;

update {{ params.master_db_name }}.fc_cp_past_5_runs   a
inner join {{ params.master_db_name }}.fc_account_score_limit b
on a.account_number=b.account_number
set cp_this= b.confidence_percentage;

update {{ params.master_db_name }}.fc_cp_past_5_runs   a
inner join {{ params.master_db_name }}.fc_account_score_limit b
on a.account_number=b.account_number
set  limit_this=b.max_fone_loan_limit;

update {{ params.master_db_name }}.fc_cp_past_5_runs   a
inner join {{ params.master_db_name }}.fc_account_score_limit b
on a.account_number=b.account_number
set this_run_date= @today_date;

insert into {{ params.master_db_name }}.fc_cp_past_5_runs(account_number,cp_this,limit_this,this_run_date)
select account_number,confidence_percentage,max_fone_loan_limit,@today_date from  {{ params.master_db_name }}.fc_account_score_limit
where account_number not in (select distinct account_number from {{ params.master_db_name }}.fc_account_joined);

drop TEMPORARY TABLE if exists {{ params.master_db_name }}.check_num_of_runs;
CREATE TEMPORARY TABLE {{ params.master_db_name }}.check_num_of_runs as 
select account_number,is_run1+is_run2+is_run3+is_run4+is_run5 as num_of_runs 
from (select account_number,case when cp_this is null then 0 else 1 end as is_run1,
case when cp_last1 is null then 0 else 1 end as is_run2,
case when cp_last2 is null then 0 else 1 end as is_run3,
case when cp_last3 is null then 0 else 1 end as is_run4,
case when cp_last4 is null then 0 else 1 end as is_run5
from {{ params.master_db_name }}.fc_cp_past_5_runs)m1;



delete from {{ params.master_db_name }}.fc_cp_past_5_runs 
where cp_this is null 
and cp_last1 is null
and cp_last2 is null
and cp_last3 is null 
and cp_last4 is null
and limit_this is null
and limit_last1 is null
and limit_last2 is null
and limit_last3 is null
and limit_last4 is null;


update {{ params.master_db_name }}.fc_cp_past_5_runs a
inner join {{ params.master_db_name }}.check_num_of_runs b
on a.account_number=b.account_number
set avg_cp=(ifnull(cp_this,0)+ifnull(cp_last1,0)+ifnull(cp_last2,0)+ifnull(cp_last3,0)+ifnull(cp_last4,0))/num_of_runs;


update {{ params.master_db_name }}.fc_cp_past_5_runs 
a
inner join {{ params.master_db_name }}.check_num_of_runs b
on a.account_number=b.account_number
set avg_limit=(ifnull(limit_this,0)+ifnull(limit_last1,0)+ifnull(limit_last2,0)+ifnull(limit_last3,0)+ifnull(limit_last4,0))/num_of_runs;



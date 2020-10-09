
drop table if exists ANOMALY_ESEWA.am_capture_20200824;
create table ANMALY_ESEWA.am_capture_20200824 as
select * from ANOMALY_ESEWA.am_capture_log;

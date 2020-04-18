
--mySQL DDL:

-- Create User and database
create user virus@'%' identified by 'corona';
create database coronavirus;
use coronavirus;
grant all privileges on coronavirus.* to virus;

-- Tables
drop table d_county;
drop table d_state;
drop table d_country;
drop table d_datetime;

drop table cur_rate;
drop table f_rate;
drop table ts_rate;

drop table f_rate2;
drop table ts_rate2;

drop table cur_rate3;
drop table f_rate3;
drop table ts_rate3;

drop table f_rate4;
drop table ts_rate4;

-- Shared tables
create table d_country (d_country_id integer unsigned not null auto_increment, name varchar(8000) not null, lat double null, long_ double null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_d_country primary key (d_country_id));
create table d_state (d_state_id integer unsigned not null auto_increment, name varchar (8000) not null, lat double null, long_ double null, d_country_id integer unsigned not null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_d_state primary key (d_state_id), constraint fk_d_state1 foreign key (d_country_id) references d_country (d_country_id) on delete cascade);
create table d_county (d_county_id integer unsigned not null auto_increment, name varchar (8000) not null, lat double null, long_ double null, d_country_id integer unsigned not null, d_state_id integer unsigned not null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_d_county primary key (d_county_id), constraint fk_d_county1 foreign key (d_country_id) references d_country (d_country_id) on delete cascade, constraint fk_d_county2 foreign key (d_state_id) references d_state (d_state_id) on delete cascade);
create table d_datetime (d_datetime_id integer unsigned not null auto_increment, update_dt datetime not null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_d_datetime primary key (d_datetime_id));

-- Used by corona.py
create table cur_rate (cur_rate_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_cur_rate primary key (cur_rate_id));
create table f_rate (f_rate_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned not null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_f_rate primary key (f_rate_id));
create table ts_rate (ts_rate_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned not null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_ts_rate primary key (ts_rate_id));

-- Used by import_data.py
create table f_rate2 (f_rate2_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned not null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_f_rate2 primary key (f_rate2_id));
create table ts_rate2 (ts_rate2_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned not null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_ts_rate2 primary key (ts_rate2_id));

-- Used by load_hist.py and load_web.py
create table cur_rate3 (cur_rate3_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned null, d_county_id integer unsigned null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, active_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_cur_rate3 primary key (cur_rate3_id));
create table f_rate3 (f_rate3_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned null, d_county_id integer unsigned null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, active_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_f_rate3 primary key (f_rate3_id));
create table ts_rate3 (ts_rate3_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned null, d_county_id integer unsigned null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, active_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_ts_rate3 primary key (ts_rate3_id));

-- Used by import_us_data.py
create table f_rate4 (f_rate4_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned null, d_county_id integer unsigned null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_f_rate4 primary key (f_rate4_id));
create table ts_rate4 (ts_rate4_id integer unsigned not null auto_increment, d_country_id integer unsigned not null, d_state_id integer unsigned null, d_county_id integer unsigned null, d_datetime_id integer unsigned not null, confirmed_cases integer null, deaths integer null, recovered_cases integer null, created_at_ts timestamp default CURRENT_TIMESTAMP, updated_at_ts timestamp null default NULL ON UPDATE CURRENT_TIMESTAMP, constraint pk_ts_rate4 primary key (ts_rate4_id));

-- Indexes
CREATE INDEX i_cur_rate_01 ON cur_rate (d_country_id, d_state_id);
CREATE INDEX i_ts_rate_01 ON ts_rate (d_country_id, d_state_id, d_datetime_id);
CREATE INDEX i_f_rate_01 ON f_rate (d_country_id, d_state_id, d_datetime_id);
CREATE INDEX i_ts_rate2_01 ON ts_rate2 (d_country_id, d_state_id, d_datetime_id);
CREATE INDEX i_f_rate2_01 ON f_rate2 (d_country_id, d_state_id, d_datetime_id);
CREATE INDEX i_cur_rate3_01 ON cur_rate3 (d_country_id, d_state_id, d_county_id);
CREATE INDEX i_ts_rate3_01 ON ts_rate3 (d_country_id, d_state_id, d_county_id, d_datetime_id);
CREATE INDEX i_f_rate3_01 ON f_rate3 (d_country_id, d_state_id, d_county_id, d_datetime_id);
CREATE INDEX i_ts_rate4_01 ON ts_rate4 (d_country_id, d_state_id, d_county_id, d_datetime_id);
CREATE INDEX i_f_rate4_01 ON f_rate4 (d_country_id, d_state_id, d_county_id, d_datetime_id);

truncate d_county
truncate d_state;
truncate d_country;
truncate d_datetime;
truncate cur_rate;
truncate f_rate;
truncate ts_rate;

-- Statistics
select TABLE_CATALOG, TABLE_NAME, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH, DATA_FREE, CREATE_TIME, UPDATE_TIME from INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = 'coronavirus';

SELECT 'cur_rate' as 'Table', count(1) as 'Rows' from cur_rate
union all
SELECT 'cur_rate3', count(1) from cur_rate3
union all
SELECT 'd_country', count(1) from d_country
union all
SELECT 'd_county', count(1) from d_county
union all
SELECT 'd_datetime', count(1) from d_datetime
union all
SELECT 'd_state', count(1) from d_state
union all
SELECT 'f_rate', count(1) from f_rate
union all
SELECT 'f_rate2', count(1) from f_rate2
union all
SELECT 'f_rate3', count(1) from f_rate3
union all
SELECT 'f_rate4', count(1) from f_rate4
union all
SELECT 'ts_rate', count(1) from ts_rate
union all
SELECT 'ts_rate2', count(1) from ts_rate2
union all
SELECT 'ts_rate3', count(1) from ts_rate3
union all
SELECT 'ts_rate4', count(1) from ts_rate4;


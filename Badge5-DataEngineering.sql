use role accountadmin;

select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 

-------------------------------------------------------------------
------- Lesson 2: Project Kick-Off and Database Set Up ---------
-------------------------------------------------------------------
use role sysadmin;
create database if not exists ags_game_audience;
drop schema public;
create schema if not exists raw;


create or replace table game_logs (
    raw_log variant

    );
create or replace file format ff_json_logs
    type = 'JSON'
    compression = 'AUTO' 
    enable_octal = FALSE
    allow_duplicate = FALSE 
    strip_outer_array = TRUE
    strip_null_values = FALSE
    ignore_utf8_errors = FALSE; 

show stages;
list @uni_kishore;

select $1
from @uni_kishore/kickoff
(file_format => ff_json_logs);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/kickoff
file_format = (format_name = ff_json_logs);


select
    raw_log:agent::text as agent,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    raw_log
from game_logs;

create or replace view logs as
select
    raw_log:agent::text as agent,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    raw_log
from game_logs;

select *
from logs;


---------- DNGW01 -------
use database util_db;
use schema public;

-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DNGW01' as step
  ,(
      select count(*)  
      from ags_game_audience.raw.logs
      where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE 
   ) as actual
, 250 as expected
, 'Project DB and Log File Set Up Correctly' as description
); 

-------------------------------------------------------------------
------- Lesson 3: Time Zones, Dates and Timestamps ---------
-------------------------------------------------------------------

select current_timestamp();

--what time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';

select *
from AGS_GAME_AUDIENCE.RAW.LOGS;

list @uni_kishore;

select $1,
from @uni_kishore/updated_feed
(file_format => ff_json_logs);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/updated_feed
file_format = (format_name = ff_json_logs);

select * 
from ags_game_audience.raw.game_logs;

select *
from logs
where agent is null;

create or replace view logs as
select
    raw_log:ip_address::text as ip_address,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    raw_log
from game_logs
where raw_log:agent::text is null;

select *
from logs
WHERE USER_LOGIN ilike '%prajina';

-->> UTC timezone--

use database util_db;
use schema public;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
   'DNGW02' as step
   ,( select sum(tally) from(
        select (count(*) * -1) as tally
        from ags_game_audience.raw.logs 
        union all
        select count(*) as tally
        from ags_game_audience.raw.game_logs)     
     ) as actual
   ,250 as expected
   ,'View is filtered' as description
); 

-------------------------------------------------------------------
------- Lesson 4: Extracting, Transforming, and Loading ---------
-------------------------------------------------------------------

select parse_ip('100.41.16.160','inet');

select parse_ip('1100.41.16.160','inet'):host;

--Or this:

select parse_ip('100.41.16.160','inet'):family;
select parse_ip('100.41.16.160','inet'):ipv4;

create schema if not exists enhanced;



--Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;


--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as DOW
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


create or replace table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;


create or replace table ags_game_audience.enhanced.logs_enhanced as(
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as DOW_NAME
--, hour(game_event_ltz) as hour
, tod_name
, city
, region
, country
, timezone as gamer_ltz_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int

left join ags_game_audience.raw.time_of_day_lu lu on hour(game_event_ltz) = lu.hour
);


select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


------ dngw03 ----
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
   'DNGW03' as step
   ,( select count(*) 
      from ags_game_audience.enhanced.logs_enhanced
      where dow_name = 'Sat'
      and tod_name = 'Early evening'   
      and gamer_name like '%prajina'
     ) as actual
   ,2 as expected
   ,'Playing the game on a Saturday evening' as description
); 


-------------------------------------------------------------------
------- Lesson 5: Productionizing Our Work ---------
-------------------------------------------------------------------
--first we dump all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then we put them all back in
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as DOW_NAME
--, hour(game_event_ltz) as hour
, tod_name
, city
, region
, country
, timezone as gamer_ltz_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int

left join ags_game_audience.raw.time_of_day_lu lu on hour(game_event_ltz) = lu.hour
);

-- 🥋 Create a Backup Copy of Your Table Using Zero-Copy Cloning

--clone the table to save this version as a backup (BU stands for Back Up)
create table ags_game_audience.enhanced.LOGS_ENHANCED_BU 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

truncate table ENHANCED.LOGS_ENHANCED;

------- INSERT MERGEs -------

MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as DOW_NAME
--, hour(game_event_ltz) as hour
, tod_name
, city
, region
, country
, timezone as gamer_ltz_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int

left join ags_game_audience.raw.time_of_day_lu lu on hour(game_event_ltz) = lu.hour
) r --we'll put our fancy select here
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME, CITY, REGION, COUNTRY, GAMER_LTZ_NAME) --list of columns
values (r.IP_ADDRESS, r.GAMER_NAME, r.GAME_EVENT_NAME, r.GAME_EVENT_UTC, r.GAME_EVENT_LTZ, r.DOW_NAME, r.TOD_NAME, r.CITY, r.REGION, r.COUNTRY, GAMER_LTZ_NAME) --list of columns (but we can mark as coming from the r select)
;

select * from ENHANCED.LOGS_ENHANCED;



------------- TASKS ----------
use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin; 

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as 
    MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as DOW_NAME
--, hour(game_event_ltz) as hour
, tod_name
, city
, region
, country
, timezone as gamer_ltz_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int

left join ags_game_audience.raw.time_of_day_lu lu on hour(game_event_ltz) = lu.hour
) r --we'll put our fancy select here
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME, CITY, REGION, COUNTRY, GAMER_LTZ_NAME) --list of columns
values (r.IP_ADDRESS, r.GAMER_NAME, r.GAME_EVENT_NAME, r.GAME_EVENT_UTC, r.GAME_EVENT_LTZ, r.DOW_NAME, r.TOD_NAME, r.CITY, r.REGION, r.COUNTRY, GAMER_LTZ_NAME) --list of columns (but we can mark as coming from the r select)
;


--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any! HINT: Probably NONE!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



------- DNGW04 ------
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW04' as step
 ,( select count(*)/iff (count(*) = 0, 1, count(*))
  from table(ags_game_audience.information_schema.task_history
              (task_name=>'LOAD_LOGS_ENHANCED'))) as actual
 ,1 as expected
 ,'Task exists and has been run at least once' as description 
 ); 


 -------------------------------------------------------------------
------- Lesson 6: Productionizing Across the Pipeline ---------
-------------------------------------------------------------------

create or replace view AGS_GAME_AUDIENCE.RAW.LOGS(
	IP_ADDRESS,
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	RAW_LOG
) as
select
    raw_log:ip_address::text as ip_address,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    raw_log
from game_logs
where raw_log:agent::text is null;


create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS (
	RAW_LOG VARIANT
);

list @uni_kishore_pipeline;

select $1,
from @uni_kishore_pipeline
(file_format => ff_json_logs);

copy into ags_game_audience.raw.pl_game_logs
from @uni_kishore_pipeline
file_format = (format_name = ff_json_logs)
force = FALSE; --nenaloaduje soubory, ktere uz jsou nahrane;

select * 
from ags_game_audience.raw.pl_game_logs;

truncate ags_game_audience.raw.pl_game_logs;


create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
	--warehouse=COMPUTE_WH
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	schedule='5 minute'
	as 
    copy into ags_game_audience.raw.pl_game_logs
from @uni_kishore_pipeline
file_format = (format_name = ff_json_logs)
force = FALSE;


--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.raw.pl_game_logs;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

------------ Step 3: The JSON-Parsing View ------
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	IP_ADDRESS,
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	RAW_LOG
) as
select
    raw_log:ip_address::text as ip_address,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    raw_log
from pl_game_logs
where raw_log:agent::text is null;


select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

------  Modify the Step 4 MERGE Task ! ----

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	--warehouse=COMPUTE_WH
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	--schedule='5 minute'
    after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
	as 
    MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as DOW_NAME
--, hour(game_event_ltz) as hour
, tod_name
, city
, region
, country
, timezone as gamer_ltz_name
from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
 JOIN IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int

left join ags_game_audience.raw.time_of_day_lu lu on hour(game_event_ltz) = lu.hour
) r --we'll put our fancy select here
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME, CITY, REGION, COUNTRY, GAMER_LTZ_NAME) --list of columns
values (r.IP_ADDRESS, r.GAMER_NAME, r.GAME_EVENT_NAME, r.GAME_EVENT_UTC, r.GAME_EVENT_LTZ, r.DOW_NAME, r.TOD_NAME, r.CITY, r.REGION, r.COUNTRY, GAMER_LTZ_NAME) --list of columns (but we can mark as coming from the r select)
;

execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
select * from ENHANCED.LOGS_ENHANCED;


select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;
truncate ENHANCED.LOGS_ENHANCED;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;

------ DNGW05 ---
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW05' as step
 ,(
   select max(tally) from (
       select CASE WHEN SCHEDULED_FROM = 'SCHEDULE' 
                         and STATE= 'SUCCEEDED' 
              THEN 1 ELSE 0 END as tally 
   from table(ags_game_audience.information_schema.task_history (task_name=>'GET_NEW_FILES')))
  ) as actual
 ,1 as expected
 ,'Task succeeds from schedule' as description
 ); 


  -------------------------------------------------------------------
------- Lesson 7: DE Practice Improvement & Cloud Foundations ---------
-------------------------------------------------------------------
--- A New Select with Metadata and Pre-Load JSON Parsing 

  SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');

create or replace TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS (
	LOG_FILE_NAME VARCHAR(100),
	LOG_FILE_ROW_ID NUMBER(18,0),
	LOAD_LTZ TIMESTAMP_LTZ(0),
	DATETIME_ISO8601 TIMESTAMP_NTZ(9),
	USER_EVENT VARCHAR(25),
	USER_LOGIN VARCHAR(100),
	IP_ADDRESS VARCHAR(100)
);

select * from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

-------------------------------------------------------------------
------- Lesson 8: Your Snowpipe! ---------
-------------------------------------------------------------------
--- Create Your Snowpipe! ---
CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

--  Update the LOAD_LOGS_ENHANCED Task ---
truncate enhanced.logs_enhanced;

alter task AGS_GAME_AUDIENCE.RAW.get_new_files suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	--after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    schedule = '5 minutes'
	as MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as DOW_NAME
--, hour(game_event_ltz) as hour
, tod_name
, city
, region
, country
, timezone as gamer_ltz_name
from AGS_GAME_AUDIENCE.raw.ED_PIPELINE_LOGS logs
 JOIN IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int

left join ags_game_audience.raw.time_of_day_lu lu on hour(game_event_ltz) = lu.hour
) r --we'll put our fancy select here
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME, CITY, REGION, COUNTRY, GAMER_LTZ_NAME) --list of columns
values (r.IP_ADDRESS, r.GAMER_NAME, r.GAME_EVENT_NAME, r.GAME_EVENT_UTC, r.GAME_EVENT_LTZ, r.DOW_NAME, r.TOD_NAME, r.CITY, r.REGION, r.COUNTRY, GAMER_LTZ_NAME);


select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));

---- Create a Stream ----

--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;

--check to see if any changes are pending (expect FALSE the first time you run it)
--after the Snowpipe loads a new file, expect to see TRUE
select system$stream_has_data('ed_cdc_stream');
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;


--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

--if you need to pause or unpause your pipe
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;

---------- Process the Rows from the Stream -----------

--make a note of how many rows are in the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

 
--process the stream by using the rows in a merge 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
 
--Did all the rows from the stream disappear? 
select * 
from ags_game_audience.raw.ed_cdc_stream; 

------------- Create a CDC-Fueled, Time-Driven Task --------------
--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
when system$stream_has_data('ed_cdc_stream')
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

select system$stream_has_data('ed_cdc_stream');

-------- DNGW06 -------
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW06' as step
 ,(
   select CASE WHEN pipe_status:executionState::text = 'RUNNING' THEN 1 ELSE 0 END 
   from(
   select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' )) as pipe_status)
  ) as actual
 ,1 as expected
 ,'Pipe exists and is RUNNING' as description
 ); 

 -------------------------------------------------------------------
------- Lesson 9: Curated Data ---------
-------------------------------------------------------------------
use role sysadmin;
create schema if not exists curated;

alter table ags_game_audience.enhanced.logs_enhanced_bu
rename to ags_game_audience.enhanced.logs_enhanced_backup;

--You can run this code in a WORKSHEET

--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;



--You can run this code in a WORKSHEET

select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;

-------- DNGW07 --------
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW07' as step
 ,( select count(*)/count(*) from snowflake.account_usage.query_history
    where query_text like '%case when game_session_length < 10%'
  ) as actual
 ,1 as expected
 ,'Curated Data Lesson completed' as description
 ); 
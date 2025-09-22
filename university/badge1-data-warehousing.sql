select 'Hello';
select 'Hello' as "Greeting";

show databases;
show schemas;
show schemas in account;

create or replace table ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   ); 

insert into root_depth 
values
(
    1,
    'S',
    'Shallow',
    'cm',
    30,
    45
)
;

insert into root_depth 
values
(
    2,
    'M',
    'Medium',
    'cm',
    45,
    60
),
(
    1,
    'D',
    'Deep',
    'cm',
    60,
    90
)
;
update root_depth
set root_depth_id = 3
where root_depth_name = 'Deep';


/* Session 6 */

use role accountadmin;

create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

show api integrations;

create or replace external function util_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
; 


use role accountadmin;
use database util_db; 
use schema public; 

select grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 


----------- End of DORA checks initialization --------

select * 
from garden_plants.information_schema.schemata;

SELECT * 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES'); 

select count(*) as schemas_found, '3' as schemas_expected 
from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');


------- DORA DWW01 ---------
--You can run this code, or you can use the drop lists in your worksheet to get the context settings right.
use database UTIL_DB;
use schema PUBLIC;
use role ACCOUNTADMIN;

--Do NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT
 'DWW01' as step
 ,( select count(*)  
   from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA 
   where schema_name in ('FLOWERS','VEGGIES','FRUITS')) as actual
  ,3 as expected
  ,'Created 3 Garden Plant schemas' as description
); 



----- DORA DWW02 -----
--Remember that every time you run a DORA check, the context needs to be set to the below settings. 
use database UTIL_DB;
use schema PUBLIC;
use role ACCOUNTADMIN;

--Do NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW02' as step 
 ,( select count(*) 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA 
   where schema_name = 'PUBLIC') as actual 
 , 0 as expected 
 ,'Deleted PUBLIC schema.' as description
); 

----- DORA DWW03 -----
-- Do NOT EDIT ANYTHING BELOW THIS LINE 
-- Remember to set your WORKSHEET context (do not add context to the grader call)
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW03' as step 
 ,( select count(*) 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
   where table_name = 'ROOT_DEPTH') as actual 
 , 1 as expected 
 ,'ROOT_DEPTH Table Exists' as description
); 


----- DORA DWW04 ------
--Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW04' as step
 ,( select count(*) as SCHEMAS_FOUND 
   from UTIL_DB.INFORMATION_SCHEMA.SCHEMATA) as actual
 , 2 as expected
 , 'UTIL_DB Schemas' as description
);


------ DORA DWW05 -----
--Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
 SELECT 'DWW05' as step 
,( select row_count 
  from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
  where table_name = 'ROOT_DEPTH') as actual 
, 3 as expected 
,'ROOT_DEPTH row count' as description
);  



/* 07 cviceni */

create table garden_plants.veggies.vegetable_details
(
plant_name varchar(25)
, root_depth_code varchar(1)    
);

SELECT
  *
FROM
  "GARDEN_PLANTS"."VEGGIES"."VEGETABLE_DETAILS"
LIMIT
  10;

--Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW06' as step
 ,( select count(*) 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
   where table_name = 'VEGETABLE_DETAILS') as actual
 , 1 as expected
 ,'VEGETABLE_DETAILS Table' as description
); 


---------
delete FROM 
  "GARDEN_PLANTS"."VEGGIES"."VEGETABLE_DETAILS"
where plant_name = 'Spinach'
    and root_depth_code = 'D'
;
--Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW07' as step
 ,( select row_count 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
   where table_name = 'VEGETABLE_DETAILS') as actual
 , 41 as expected
 , 'VEG_DETAILS row count' as description
); 


create or replace TABLE GARDEN_PLANTS.VEGGIES.FLOWER_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

alter table GARDEN_PLANTS.VEGGIES.FLOWER_DETAILS
rename to GARDEN_PLANTS.FLOWERS.FLOWER_DETAILS;



----- DWW08 ------
--Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
   SELECT 'DWW08' as step 
   ,( select iff(count(*)=0, 0, count(*)/count(*))
      from table(information_schema.query_history())
      where query_text like 'execute notebook%Uncle Yer%') as actual 
   , 1 as expected 
   , 'Notebook success!' as description 
); 

select *
from table(information_schema.query_history())
where query_text like 'execute NOTEBOOK%Uncle Yer%'
;
--------------

create or replace TABLE GARDEN_PLANTS.FRUITS.FRUIT_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

select * from GARDEN_PLANTS.FRUITS.FRUIT_DETAILS;


------ DWW09 -----
--Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW09' as step
 ,( select iff(count(*)=0, 0, count(*)/count(*)) 
    from snowflake.account_usage.query_history
    where query_text like 'execute streamlit "GARDEN_PLANTS"."FRUITS".%'
   ) as actual
 , 1 as expected
 ,'SiS App Works' as description
); 

select *
from table(information_schema.query_history())
where query_text like 'execute streamlit%';

----- DWW10 ------
-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW10' as step
  ,( select count(*) 
    from UTIL_DB.INFORMATION_SCHEMA.stages
    where stage_name='MY_INTERNAL_STAGE' 
    and stage_type is null) as actual
  , 1 as expected
  , 'Internal stage created' as description
 ); 

----------------------
create or replace table garden_plants.veggies.vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);

create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    type = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    field_delimiter = '|' --pipes as column separators
    skip_header = 1 --one header row to skip
    ;

copy into garden_plants.veggies.vegetable_details_soil_type
from @util_db.public.my_internal_stage
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );


----- DWW11 -----
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DWW11' as step
  ,( select row_count 
    from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
    where table_name = 'VEGETABLE_DETAILS_SOIL_TYPE') as actual
  , 42 as expected
  , 'Veg Det Soil Type Count' as description
 ); 


 ----------------------
 create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = ',' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;
 create or replace file format garden_plants.veggies.L9_CHALLENGE_FF
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = '\t' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;

    drop file format garden_plants.veggies.tabsep_dblquot_oneheadrow;
--The data in the file, with no FILE FORMAT specified
select $1
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv;

--Same file but with one of the file formats we created earlier  
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.L9_CHALLENGE_FF );


create or replace table LU_SOIL_TYPE(
SOIL_TYPE_ID number,	
SOIL_TYPE varchar(15),
SOIL_DESCRIPTION varchar(75)
 );

copy into garden_plants.veggies.LU_SOIL_TYPE
from @util_db.public.my_internal_stage
files = ( 'LU_SOIL_TYPE.tsv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF );

select * from garden_plants.veggies.LU_SOIL_TYPE;

-----------------------------

create or replace table garden_plants.veggies.VEGETABLE_DETAILS_PLANT_HEIGHT (
   PLANT_NAME text(20), 
   UOM text(1), 
   LOW_END_OF_RANGE number(2), 
   HIGH_END_OF_RANGE number(2)

   ); 

copy into garden_plants.veggies.VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files = ( 'veg_plant_height.csv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW );

select * from garden_plants.veggies.VEGETABLE_DETAILS_PLANT_HEIGHT;


---------- DWW12 ------
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (  
      SELECT 'DWW12' as step 
      ,( select row_count 
        from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
        where table_name = 'VEGETABLE_DETAILS_PLANT_HEIGHT') as actual 
      , 41 as expected 
      , 'Veg Detail Plant Height Count' as description   
); 

-------- DWW13 --------
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (  
     SELECT 'DWW13' as step 
     ,( select row_count 
       from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
       where table_name = 'LU_SOIL_TYPE') as actual 
     , 8 as expected 
     ,'Soil Type Look Up Table' as description   
);

------- DWW14 -----
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
     SELECT 'DWW14' as step 
     ,( select count(*) 
       from GARDEN_PLANTS.INFORMATION_SCHEMA.FILE_FORMATS 
       where FILE_FORMAT_NAME='L9_CHALLENGE_FF' 
       and FIELD_DELIMITER = '\t') as actual 
     , 1 as expected 
     ,'Challenge File Format Created' as description  
); 



------ Lesson 10: Data Storage Structures ------
use role sysadmin;

// Create a new database and set the context to use the new database
create database library_card_catalog comment = 'DWW Lesson 10 ';

//Set the worksheet context to use the new database
use database library_card_catalog;



use database library_card_catalog;
use role sysadmin;

// Create the book table and use AUTOINCREMENT to generate a UID for each new row

create or replace table book
( book_uid number autoincrement
 , title varchar(50)
 , year_published number(4,0)
);

// Insert records into the book table
// You don't have to list anything for the
// BOOK_UID field because the AUTOINCREMENT property 
// will take care of it for you

insert into book(title, year_published)
values
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

// Check your table. Does each row have a unique id? 
select * from book;


// Create Author table
create or replace table author (
   author_uid number 
  ,first_name varchar(50)
  ,middle_name varchar(50)
  ,last_name varchar(50)
);

// Insert the first two authors into the Author table
insert into author(author_uid, first_name, middle_name, last_name)  
values
(1, 'Fiona', '','Macdonald')
,(2, 'Gian','Paulo','Faleschini');

// Look at your table with it's new rows
select * 
from author;

use role sysadmin;

//See how the nextval function works
select seq_author_uid.nextval;

select seq_author_uid.nextval, seq_author_uid.nextval;
show sequences;



use role sysadmin;

//Drop and recreate the counter (sequence) so that it starts at 3 
// then we'll add the other author records to our author table
create or replace sequence library_card_catalog.public.seq_author_uid
start = 3 
increment = 1 
ORDER
comment = 'Use this to fill in the AUTHOR_UID every time you add a row';

//Add the remaining author records and use the nextval function instead 
//of putting in the numbers
insert into author(author_uid,first_name, middle_name, last_name) 
values
(seq_author_uid.nextval, 'Laura', 'K','Egendorf')
,(seq_author_uid.nextval, 'Jan', '','Grover')
,(seq_author_uid.nextval, 'Jennifer', '','Clapp')
,(seq_author_uid.nextval, 'Kathleen', '','Petelinsek');


----------------------
use database library_card_catalog;
use role sysadmin;


// Create the relationships table
// this is sometimes called a "Many-to-Many table"
create table book_to_author
( book_uid number
  ,author_uid number
);

//Insert rows of the known relationships
insert into book_to_author(book_uid, author_uid)
values
 (1,1)  // This row links the 2001 book to Fiona Macdonald
,(1,2)  // This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  // Links 2006 book to Laura K Egendorf
,(3,4)  // Links 2008 book to Jan Grover
,(4,5)  // Links 2016 book to Jennifer Clapp
,(5,6); // Links 2015 book to Kathleen Petelinsek


//Check your work by joining the 3 tables together
//You should get 1 row for every author
select * 
from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid; 


-------- DWW15 -------
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (  
     SELECT 'DWW15' as step 
     ,( select count(*) 
      from LIBRARY_CARD_CATALOG.PUBLIC.Book_to_Author ba 
      join LIBRARY_CARD_CATALOG.PUBLIC.author a 
      on ba.author_uid = a.author_uid 
      join LIBRARY_CARD_CATALOG.PUBLIC.book b 
      on b.book_uid=ba.book_uid) as actual 
     , 6 as expected 
     , '3NF DB was Created.' as description  
);

-------------- Lessson 11 --------------------------------------
-- Intro to Semi-Structured Data ----
--------------------------------------------------------------------
// JSON DDL Scripts
use database library_card_catalog;
use role sysadmin;

// Create an Ingestion Table for JSON Data
create or replace table library_card_catalog.public.author_ingest_json
(
  raw_author variant
);

//Create File Format for JSON Data 
create or replace file format library_card_catalog.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
enable_octal = FALSE
allow_duplicate = FALSE 
strip_outer_array = TRUE
strip_null_values = FALSE
ignore_utf8_errors = FALSE; 


--Same file but with the other file format we created earlier
select $1
from @util_db.public.my_internal_stage/author_with_header.json
(file_format => library_card_catalog.public.json_file_format );

copy into library_card_catalog.public.author_ingest_json
from @util_db.public.my_internal_stage
files = ( 'author_with_header.json')
file_format = ( format_name=library_card_catalog.public.json_file_format );

select * from library_card_catalog.public.author_ingest_json;



//returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID, raw_author:FIRST_NAME
from author_ingest_json;

//returns the data in a way that makes it look like a normalized table
SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;


use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW16' as step
  ,( select row_count 
    from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
    where table_name = 'AUTHOR_INGEST_JSON') as actual
  ,6 as expected
  ,'Check number of rows' as description
 ); 

 
-------------- Lessson 12 --------------------------------------
--- Nested Semi-Structured Data ----
----------------------------------------------------------------

// Create an Ingestion Table for the NESTED JSON Data
create or replace table library_card_catalog.public.nested_ingest_json 
(
  raw_nested_book VARIANT
);


select $1
from @util_db.public.my_internal_stage/json_book_author_nested.txt
(file_format => library_card_catalog.public.json_file_format );

copy into library_card_catalog.public.nested_ingest_json 
from @util_db.public.my_internal_stage
files = ( 'json_book_author_nested.txt')
file_format = ( format_name=library_card_catalog.public.json_file_format );

select * from library_card_catalog.public.nested_ingest_json ;

//a few simple queries
select raw_nested_book
from nested_ingest_json;

select raw_nested_book:year_published
from nested_ingest_json;

select raw_nested_book:authors
from nested_ingest_json;

--- FLATTEN COMMAND on Nested Data -----
//Use these example flatten commands to explore flattening the nested book and author data
select value:first_name
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);

select value:first_name
from nested_ingest_json
,table(flatten(raw_nested_book:authors));

//Add a CAST command to the fields returned
SELECT value:first_name::varchar, value:last_name::varchar
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);

//Assign new column  names to the columns using "AS"
select value:first_name::varchar as first_nm
, value:last_name::varchar as last_nm
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);


use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (   
     SELECT 'DWW17' as step 
      ,( select row_count 
        from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
        where table_name = 'NESTED_INGEST_JSON') as actual 
      , 5 as expected 
      ,'Check number of rows' as description  
); 


create database if not exists SOCIAL_MEDIA_FLOODGATES;

create or replace table SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST (
    RAW_STATUS VARIANT
);

//Create File Format for JSON Data 
create or replace file format SOCIAL_MEDIA_FLOODGATES.PUBLIC.nested_json_file_format
type = 'JSON' 
compression = 'AUTO' 
enable_octal = FALSE
allow_duplicate = FALSE 
strip_outer_array = TRUE
strip_null_values = FALSE
ignore_utf8_errors = FALSE; 

select $1
from @util_db.public.my_internal_stage/nutrition_tweets.json
(file_format => SOCIAL_MEDIA_FLOODGATES.PUBLIC.nested_json_file_format );

copy into SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST 
from @util_db.public.my_internal_stage
files = ( 'nutrition_tweets.json')
file_format = ( format_name=SOCIAL_MEDIA_FLOODGATES.PUBLIC.nested_json_file_format );

select * from SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST ;


//simple select statements -- are you seeing 9 rows?
select raw_status
from tweet_ingest;

select raw_status:entities
from tweet_ingest;

select raw_status:entities:hashtags
from tweet_ingest;

//Explore looking at specific hashtags by adding bracketed numbers
//This query returns just the first hashtag in each tweet
select raw_status:entities:hashtags[0].text
from tweet_ingest;

//This version adds a WHERE clause to get rid of any tweet that 
//doesn't include any hashtags
select raw_status:entities:hashtags[0].text
from tweet_ingest
where raw_status:entities:hashtags[0].text is not null;

//Perform a simple CAST on the created_at key
//Add an ORDER BY clause to sort by the tweet's creation date
select raw_status:created_at::date
from tweet_ingest
order by raw_status:created_at::date;

------------- Change the INPUT on the FLATTEN to Isolate an Entity --------------
select value
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls);

select value
from tweet_ingest
,table(flatten(raw_status:entities:urls));

/* both queries above have the same output! */

//Flatten and return just the hashtag text, CAST the text as VARCHAR
select value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);

//Add the Tweet ID and User ID to the returned table so we could join the hashtag back to it's source tweet
select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);
-------------- DWW18 --------------

use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
   SELECT 'DWW18' as step
  ,( select row_count 
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.TABLES 
    where table_name = 'TWEET_INGEST') as actual
  , 9 as expected
  ,'Check number of rows' as description  
 ); 


 -------------- Create a View of the URL Data Looking "Normalized" -----------------
 
 create or replace view social_media_floodgates.public.urls_normalized as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:display_url::text as url_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls)
);

use database social_media_floodgates;
use schema public;
---------- Create a View that Makes the Hashtag Data Appear Normalized----------------
 create or replace view social_media_floodgates.public.hashtags_normalized as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::text as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags)
);


select * from social_media_floodgates.public.hashtags_normalized;

--------- DWW19 ----------------
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW19' as step
  ,( select count(*) 
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.VIEWS 
    where table_name = 'HASHTAGS_NORMALIZED') as actual
  , 1 as expected
  ,'Check number of rows' as description
 ); 

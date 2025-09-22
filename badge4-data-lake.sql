-------------------------------------------------------------------
------- Lesson 2: Revieing Data Structuring & Stage Types ---------
-------------------------------------------------------------------
use role accountadmin;

select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 

create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);

use role sysadmin;
create database  ZENAS_ATHLEISURE_DB;
drop schema  ZENAS_ATHLEISURE_DB.public;

create schema if not exists PRODUCTS;

------ DLKW01 -----
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW01' as step
  ,( select count(*)  
      from ZENAS_ATHLEISURE_DB.INFORMATION_SCHEMA.STAGES 
      where stage_schema = 'PRODUCTS'
      and 
      (stage_type = 'Internal Named' 
      and stage_name = ('PRODUCT_METADATA'))
      or stage_name = ('SWEATSUITS')
   ) as actual
, 2 as expected
, 'Zena stages look good' as description
); 


-------------------------------------------------------------------
-------     Lesson 3: Leaving the Data Where it Lands     ---------
-------------------------------------------------------------------
list @product_metadata;

select $1
from @product_metadata; 

select $1
from @zenas_athleisure_db.products.product_metadata/product_coordination_suggestions.txt;
select $1
from @zenas_athleisure_db.products.product_metadata/sweatsuit_sizes.txt;
select $1
from @zenas_athleisure_db.products.product_metadata/swt_product_line.txt;

create file format zmd_file_format_1
RECORD_DELIMITER = '^';

select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

--- FIELD_DELIMITER je oddelovac sloupcu, RECORD_DELIMITER je oddelovac radku/zaznamu ---

select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
TRIM_SPACE = TRUE; 

create or replace view zenas_athleisure_db.products.SWEATBAND_COORDINATION as
select REPLACE($1, chr(13)||char(10)) as "PRODUCT_CODE", 
REPLACE($2, chr(13)||char(10)) as "HAS_MATCHING_SWEATSUIT"
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

-------
create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

--- REPLACE($1, chr(13)||char(10))  will replace CRLF (Carriage Return Line Feed) as the record delimiter
create view zenas_athleisure_db.products.sweatsuit_sizes as 
select REPLACE($1, chr(13)||char(10))  as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';

select * from zenas_athleisure_db.products.sweatsuit_sizes;
-------
create or replace file format zmd_file_format_2
RECORD_DELIMITER = ';'
FIELD_DELIMITER = '|'
TRIM_SPACE = TRUE;

create or replace view zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE as
select 
    REPLACE($1, chr(13)||char(10)) as "PRODUCT_CODE", 
    REPLACE($2, chr(13)||char(10)) as "HEADBAND_DESCRIPTION", 
    REPLACE($3, chr(13)||char(10)) as "WRISTBAND_DESCRIPTION"
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2 );


---------- DLKW02 --------
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
   'DLKW02' as step
   ,( select sum(tally) from
        ( select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE
        where length(product_code) > 7 
        union
        select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES
        where LEFT(sizes_available,2) = char(13)||char(10))     
     ) as actual
   ,0 as expected
   ,'Leave data where it lands.' as description
);

select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;
select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;
select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;


-------------------------------------------------------------------
----- Lesson 4: Working with Non-Loaded Unstructured Data ---------
-------------------------------------------------------------------
list @sweatsuits;

select $1
from @sweatsuits/purple_sweatsuit.png; 

select metadata$filename, max(metadata$file_row_number) as number_of_rows
from @sweatsuits
group by metadata$filename;

--- directory stage table and its functions --
select * 
from directory(@sweatsuits);

select REPLACE(relative_path, '_', ' ') as no_underscores_filename
, REPLACE(no_underscores_filename, '.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

-- nesting functions together, using aliases in other functions, etc.
select initcap(REPLACE(REPLACE(relative_path, '_', ' '),'.png')) as product_name
from directory(@sweatsuits);

--create an internal table for some sweatsuit info
create or replace table zenas_athleisure_db.products.sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

--fill the new table with some data
insert into  zenas_athleisure_db.products.sweatsuits 
          (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);


create or replace view PRODUCT_LIST as
select 
    initcap(replace(replace(dir.relative_path, '_',' '), '.png')) as product_name,
    dir.relative_path as file_name,
    s.color_or_style,
    s.price,
    dir.file_url

from directory(@sweatsuits) dir
left join zenas_athleisure_db.products.sweatsuits s on dir.relative_path = s.FILE_NAME
;

use role sysadmin;
create or replace view catalog as
select * 
from product_list p
cross join sweatsuit_sizes
order by p.product_name;


select count(*) from catalog;

------ DLKW03 -----
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW03' as step
 ,( select count(*) from ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG) as actual
 ,180 as expected
 ,'Cross-joined view exists' as description
); 

-- Add a table to map the sweatsuits to the sweat band sets
create table zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

--populate the upsell table
insert into zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code;

-------- DLKW04 -------
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW04' as step
 ,( select count(*) 
  from zenas_athleisure_db.products.catalog_for_website 
  where upsell_product_desc not like '%e, Bl%') as actual
 ,6 as expected
 ,'Relentlessly resourceful' as description
); 


use database zenas_athleisure_db;
use schema products;


-------------------------------------------------------------------
-------      Lesson 5: Mel's Concept Kickoff      ---------
-------------------------------------------------------------------
use role sysadmin;
create database if not exists MELS_SMOOTHIE_CHALLENGE_DB;
use database MELS_SMOOTHIE_CHALLENGE_DB;
drop schema public;
create schema if not exists TRAILS;
CREATE STAGE TRAILS_GEOJSON 
	DIRECTORY = ( ENABLE = true );
CREATE STAGE TRAILS_PARQUET 
	DIRECTORY = ( ENABLE = true );

select *
from @trails_geojson
(file_format => ff_json);

select *
from @trails_parquet
(file_format => ff_parquet);


------ DLKW05 ------
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW05' as step
 ,( select sum(tally)
   from
     (select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.stages 
      union all
      select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.file_formats)) as actual
 ,4 as expected
 ,'Camila\'s Trail Data is Ready to Query' as description
 ); 

 ----Querying the Parquet File ------
 use database MELS_SMOOTHIE_CHALLENGE_DB;
 use schema trails;

create or replace view CHERRY_CREEK_TRAIL as
select
    $1:sequence_1 as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng,
    $1:longitude::number(11,8) as lat,
    --$1:sequence_2 as sequence_2,
    --$1:elevation as elevation
    lng || ' ' || lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

select top 100
 lng || ' ' || lat as coord_pair,
 'POINT(' || coord_pair || ')'
from CHERRY_CREEK_TRAIL;


--- limit points for WKT geo playground is 2450 points ----
select 
    'LINESTRING('|| listagg(coord_pair, ',') within group (order by point_id) ||')' as my_linestring
from cherry_creek_trail
where point_id <= 2450
group by trail_name;

------ geoJSON file-----
list @trails_geojson;

select $1
from @trails_geojson
(file_format => ff_json);

--- normalizing data without loading it---
create or replace view DENVER_AREA_TRAILS as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);



------ DLKW06 -----
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW06' as step
 ,( select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.views 
      where table_name in ('CHERRY_CREEK_TRAIL','DENVER_AREA_TRAILS')) as actual
 ,2 as expected
 ,'Mel\'s views on the geospatial data from Camila' as description
 ); 


 -------------------------------------------------------------------
----- Lesson 7: Exploring GeoSpatial Functions ---------
-------------------------------------------------------------------
use database mels_smoothie_challenge_db;
use schema trails;

---- TO_GEOGRAPHY() function -----
select 
    'LINESTRING('|| listagg(coord_pair, ',') within group (order by point_id) ||')' as my_linestring
,st_length(to_geography(my_linestring)) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail
group by trail_name;

select *
from DENVER_AREA_TRAILS;

select
    feature_name,
    st_length(to_geography(geometry)) as wo_lenght,
    st_length(to_geography(geometry)) as geom_lenght,
from DENVER_AREA_TRAILS;

select get_ddl('view', 'DENVER_AREA_TRAILS');
create or replace view DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,st_length(to_geography(geometry)) as trail_lenght
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);



--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;

select * from DENVER_AREA_TRAILS_2;

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, to_geography(geometry), trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, to_geography(geometry), trail_length
from DENVER_AREA_TRAILS_2;

--Add more GeoSpatial Calculations to get more GeoSpecial Information! 
create or replace view trails_and_boundaries as
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

-----  A Polygon Can be Used to Create a Bounding Box -----
select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;

----- DKLW07 ----
use database util_db;
use schema public;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
  'DLKW07' as step
   ,( select round(max(max_northsouth))
      from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES)
      as actual
 ,40 as expected
 ,'Trails Northern Extent' as description
 ); 


-------------------------------------------------------------------
-- Lesson 8: Supercharging Development with Marketplace Data ------
-------------------------------------------------------------------

--- sample geo queries on the top of Denver open street data (data product) maps--
// Give me the length of a Way
SELECT
ID,
ST_LENGTH(COORDINATES) AS LENGTH
FROM DENVER.V_OSM_DEN_WAY;

// List the number of nodes in a Way
SELECT
ID,
ST_NPOINTS(COORDINATES) AS NUM_OF_NODES
FROM DENVER.V_OSM_DEN_WAY;

// Give me the distance between two Ways
SELECT
 A.ID AS ID_1,
 B.ID AS ID_2,
 ST_DISTANCE(A.COORDINATES, B.COORDINATES) AS DISTANCE
FROM (SELECT
 ID,
 COORDINATES
FROM DENVER.V_OSM_DEN_WAY
WHERE ID = 705859567) AS A
INNER JOIN (SELECT
 ID,
 COORDINATES
FROM DENVER.V_OSM_DEN_WAY
WHERE ID = 705859570) AS B;

// Give me all amenities from education category in a radius of 2,000 metres from a point
SELECT
*
FROM DENVER.V_OSM_DEN_AMENITY_EDUCATION
WHERE ST_DWITHIN(ST_POINT(-1.049212522000000e+02,
    3.969829250000000e+01),COORDINATES,2000);

// Give me all food and beverage Shops in a radius of 2,000 metres from a point

SELECT
*
FROM DENVER.V_OSM_DEN_SHOP_FOOD_BEVERAGES  
WHERE ST_DWITHIN(ST_POINT(-1.049632800000000e+02,
    3.974338330000000e+01),COORDINATES,2000);

;

------- Using Variables in Snowflake Worksheets -----


-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lng='-105.00840763333615'; 
set loc_lat='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lng,$mc_lat)
        ,st_makepoint($loc_lng,$loc_lat)
        ) as mc_to_cp;

use database mels_smoothie_challenge_db;
create schema if not exists LOCATIONS;

----- Let's Create a UDF for Measuring Distance from Melanie's Café ----
CREATE OR REPLACE FUNCTION distance_to_mc(loc_lng number(38,32), loc_lat number(38,32))
  RETURNS FLOAT
  AS
  $$
    st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085'),
        st_makepoint(loc_lng, loc_lat)
    )
  $$
  ;

  --Tivoli Center into the variables 
set tc_lng='-105.00532059763648'; 
set tc_lat='39.74548137398218';

select distance_to_mc($tc_lng,$tc_lat);

---------- 🥋 Analyze Melanie's Competition -------
create or replace view COMPETITION as 

select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

select * from competition;


CREATE OR REPLACE FUNCTION distance_to_mc(lng_and_lat GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lng_and_lat
        )
  $$
  ;

SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_to_melanies
 ,*
FROM  competition
ORDER by distance_to_melanies;

-----------------------

-- Tattered Cover Bookstore McGregor Square
set tcb_lng='-104.9956203'; 
set tcb_lat='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lng,$tcb_lat);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lng,$tcb_lat));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

--------- 🎯 Analyze Potential Promotion Partners --------------
create or replace view  DENVER_BIKE_SHOPS as
select
    name,
    st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085'),--Melanie's shop
        coordinates
    ) as distance_to_melanies,
    coordinates
from openstreetmap_denver.denver.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where shop = 'bicycle';

select * from denver_bike_shops;


------ DLKW08 ----
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
  'DLKW08' as step
  ,( select truncate(distance_to_melanies)
      from mels_smoothie_challenge_db.locations.denver_bike_shops
      where name like '%Mojo%') as actual
  ,14084 as expected
  ,'Bike Shop View Distance Calc works' as description
 ); 

-------------------------------------------------------------------
-- Lesson 9: Lions & Tigers & Bears, Oh My!!!     ------
--- MVs, External Tables, Iceberg Tables ----
-------------------------------------------------------------------

---- 🥋 Let's TRY TO CREATE a Super-Simple, Stripped Down External Table ----
create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(100) as (metadata$filename::varchar(100))
) 
location= @EXTERNAL_AWS_DLKW
auto_refresh = true
file_format = (type = parquet);

select * from t_cherry_creek_trail;


------ Create a Materialized View Version of Our New External Table -----
-- !!! nemuzu udelat MV nad snowflake stage, ale muzu na stage udelat External Table. A nad tou External Table udelat MV!!!
create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
select 
 value:sequence_1 as point_id,
 value:trail_name::varchar as trail_name,
 value:latitude::number(11,8) as lng,
 value:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair,
 locations.distance_to_mc(st_makepoint(lng, lat)) as distance_to_melanies
from t_cherry_creek_trail;

---- DLKW09 ----
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
  'DLKW09' as step
  ,( select row_count
     from mels_smoothie_challenge_db.information_schema.tables
     where table_schema = 'TRAILS'
    and table_name = 'SMV_CHERRY_CREEK_TRAIL')   
   as actual
  ,3526 as expected
  ,'Secure Materialized View Created' as description
 );

----- 🥋 Set Up Your Iceberg External Volume and User -----
CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://uni-dlkw-iceberg'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/dlkw_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'dlkw_iceberg_id'
         )
      );
 DESC EXTERNAL VOLUME iceberg_external_volume;
 show external volumes;

 --- Create an Iceberg Database ---
 create database my_iceberg_db
 catalog = 'SNOWFLAKE'
 external_volume = 'iceberg_external_volume';

 set table_name = 'CCT_'||current_account();
 select $table_name;

create iceberg table identifier($table_name) (
    point_id number(10,0)
    , trail_name string
    , coord_pair string
    , distance_to_melanies decimal(20,10)
    , user_name string
)
  BASE_LOCATION = $table_name
  AS SELECT top 100
    point_id
    , trail_name
    , coord_pair
    , distance_to_melanies
    , current_user()
  FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;

select * from identifier($table_name); 
select * from CCT_FQB52979;

update identifier($table_name)
set user_name = 'I am amazing!!'
where point_id = 1;


------ DLKW10 ----
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
  'DLKW10' as step
  ,( select row_count
      from MY_ICEBERG_DB.INFORMATION_SCHEMA.TABLES
      where table_catalog = 'MY_ICEBERG_DB'
      and table_name like 'CCT_%'
      and table_type = 'BASE TABLE')   
   as actual
  ,100 as expected
  ,'Iceberg table created and populated!' as description
 ); 

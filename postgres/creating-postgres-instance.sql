-----------------------------------------------
-- postgres instance in Snowflake --
-----------------------------------------------
/* 
from: 
https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/
https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-create-instance

credit consumption price list: https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf

STANDARD_M = 0.0356 kredits/hour of compute - this is ~ 26 kredits/month (= ~ 100USD)
Storage = ~ 130USD/TB/month
*/

CREATE POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION"
/*Snowflake doesn't support hyphens in the intance name >> can by bypassed by double-quotes*/
  COMPUTE_FAMILY = 'STANDARD_M'
  STORAGE_SIZE_GB = 10
  AUTHENTICATION_AUTHORITY = POSTGRES
   POSTGRES_VERSION = 18 
  --[ NETWORK_POLICY = '<network_policy>' ]
   HIGH_AVAILABILITY = FALSE 
  --[ POSTGRES_SETTINGS = '<json_string>' ] /* https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-server-settings */
   COMMENT = 'First instance for "Getting started with Snowflake postgres"' ;

SHOW POSTGRES INSTANCES;

--------------------------------------------------------------------------------
/* There is a need to have a connection from SQL client on the local machine to the Snowflake's postgres.
= INGRESS network rule is needed, that needs to be attached to network policy and that policy needs to be attached to postgres instance.

source: https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-network
*/
-- Create a database for network policies
CREATE DATABASE NETWORK_CONFIG;

-- Create the ingress rule
CREATE NETWORK RULE NETWORK_CONFIG.PUBLIC.PG_INGRESS_FROM_LOCAL
  TYPE = IPV4
  VALUE_LIST = ('193.179.119.36/32')
  MODE = POSTGRES_INGRESS;

-- Create the network policy using the rule
CREATE NETWORK POLICY MY_LOCAL_ACCESS_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('NETWORK_CONFIG.PUBLIC.PG_INGRESS_FROM_LOCAL')
  COMMENT = 'Allow access from my local machine';


-- Apply to your Postgres instance
ALTER POSTGRES INSTANCE "DEV-POSTGRES-ONDREJBLAHA"
  SET NETWORK_POLICY = 'MY_LOCAL_ACCESS_POLICY';


---------------------------------------------------------------
------- Postgres instance costs --------
---------------------------------------------------------------
/* https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-cost */

-- View Postgres instance usage
SELECT service_type, count(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
group by 1

limit 100;
--WHERE SERVICE_TYPE like '%POSTGRES%'
--in ('POSTGRES_COMPUTE', 'POSTGRES_COMPUTE_HA')
ORDER BY START_TIME DESC;

-- View credit consumption
SELECT 
  SERVICE_TYPE,
  SUM(CREDITS_USED) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE SERVICE_TYPE = 'POSTGRES'
  AND START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY SERVICE_TYPE;


select *
from SNOWFLAKE.ACCOUNT_USAGE.POSTGRES_STORAGE_USAGE_HISTORY
;


-- ============================================
-- Snowflake Postgres Instance Management
-- ============================================
show postgres instances;

-- START THE POSTGRES INSTANCE
ALTER POSTGRES INSTANCE "DEV-POSTGRES-ONDREJBLAHA" RESUME;

-- STOP THE POSTGRES INSTANCE
ALTER POSTGRES INSTANCE "DEV-POSTGRES-ONDREJBLAHA" SUSPEND;

/*
-- CHECK STATUS
SELECT NAME, STATE 
FROM SNOWFLAKE.INFORMATION_SCHEMA.POSTGRES_INSTANCES
WHERE NAME = 'DEV-POSTGRES-ONDREJBLAHA';




-- 1. CHECK CURRENT STATUS
SELECT 
  NAME,
  STATE,
  COMPUTE_FAMILY,
  STORAGE_SIZE_GB,
  HIGH_AVAILABILITY,
  CREATED
FROM SNOWFLAKE.INFORMATION_SCHEMA.POSTGRES_INSTANCES
WHERE NAME = 'DEV_POSTGRES_ONDREJBLAHA';

-- 2. START THE POSTGRES INSTANCE
ALTER POSTGRES INSTANCE DEV_POSTGRES_ONDREJBLAHA RESUME;

-- Wait a moment for it to start, then verify
SELECT NAME, STATE 
FROM SNOWFLAKE.INFORMATION_SCHEMA.POSTGRES_INSTANCES
WHERE NAME = 'DEV_POSTGRES_ONDREJBLAHA';
-- STATE should be 'ACTIVE'

-- 3. STOP THE POSTGRES INSTANCE
ALTER POSTGRES INSTANCE DEV_POSTGRES_ONDREJBLAHA SUSPEND;

-- Verify it stopped
SELECT NAME, STATE 
FROM SNOWFLAKE.INFORMATION_SCHEMA.POSTGRES_INSTANCES
WHERE NAME = "DEV-POSTGRES-ONDREJBLAHA";
-- STATE should be 'SUSPENDED'


*/
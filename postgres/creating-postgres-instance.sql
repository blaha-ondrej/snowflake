-----------------------------------------------
-- postgres instance in Snowflake --
-----------------------------------------------
/* 
from: 
https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/
https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-create-instance
*/

CREATE POSTGRES INSTANCE "DEV-POSTGRES-ONDREJBLAHA"
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



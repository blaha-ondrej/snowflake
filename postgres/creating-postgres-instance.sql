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
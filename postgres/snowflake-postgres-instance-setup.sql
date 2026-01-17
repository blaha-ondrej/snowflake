-- =============================================
-- SNOWFLAKE POSTGRES SETUP SCRIPT
-- Run this script in Snowflake Worksheet
-- =============================================

-- =============================================
-- PART 1: CREATE POSTGRES INSTANCE
-- =============================================
-- Documentation: https://docs.snowflake.com/en/user-guide/snowflake-postgres/overview
-- Pricing: https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf
--
-- Cost Estimates:
-- - STANDARD_M compute: ~0.0356 credits/hour (~26 credits/month = ~$100 USD/month if running 24/7)
-- - Storage: ~$130 USD/TB/month (10 GB = ~$1.30/month)
--
-- IMPORTANT: Instance names with hyphens must be in double quotes

CREATE POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION"
  COMPUTE_FAMILY = 'STANDARD_M'          -- Options: STANDARD_S, STANDARD_M, STANDARD_L, STANDARD_XL
  STORAGE_SIZE_GB = 10                   -- Minimum: 10 GB
  AUTHENTICATION_AUTHORITY = POSTGRES    -- Options: POSTGRES, SNOWFLAKE
  POSTGRES_VERSION = 18                  -- Options: 15, 16, 17, 18 (use latest stable)
  HIGH_AVAILABILITY = FALSE              -- TRUE = doubles cost (for production), FALSE = cheaper (for dev)
  COMMENT = 'First instance for "Getting started with Snowflake postgres"';

-- Alternative: Without hyphens (recommended to avoid needing quotes)
-- CREATE POSTGRES INSTANCE DEV_POSTGRES_INTRODUCTION
--   COMPUTE_FAMILY = 'STANDARD_M'
--   STORAGE_SIZE_GB = 10
--   AUTHENTICATION_AUTHORITY = POSTGRES
--   POSTGRES_VERSION = 18 
--   HIGH_AVAILABILITY = FALSE 
--   COMMENT = 'First instance for "Getting started with Snowflake postgres"';


-- =============================================
-- PART 2: CHECK INSTANCE STATUS
-- =============================================
-- View all Postgres instances
SHOW POSTGRES INSTANCES;

-- Get detailed information about your instance
SELECT 
  NAME,
  STATE,                    -- CREATING, ACTIVE, SUSPENDED, FAILED
  COMPUTE_FAMILY,
  STORAGE_SIZE_GB,
  HIGH_AVAILABILITY,
  CREATED
FROM SNOWFLAKE.INFORMATION_SCHEMA.POSTGRES_INSTANCES
WHERE NAME = 'DEV-POSTGRES-INTRODUCTION';

-- Get connection details (URL, username, password)
-- This will show the connection string, admin username, and password
DESCRIBE POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION";

-- NOTE: Instance creation typically takes 2-5 minutes
-- Wait until STATE = 'ACTIVE' before proceeding


-- =============================================
-- PART 3: NETWORK RULES AND POLICIES
-- =============================================
-- Documentation: https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-network
--
-- INGRESS = incoming traffic (SQL client connecting TO Postgres)
-- EGRESS = outgoing traffic (Postgres connecting TO external services)
--
-- For local SQL client connections, we need INGRESS rules
-- Replace '193.179.119.36' with your actual IP address from https://whatismyipaddress.com/

-- Step 1: Create a database for network policies
CREATE DATABASE IF NOT EXISTS NETWORK_CONFIG;

-- Step 2: Create the ingress rule
-- IMPORTANT: Replace '193.179.119.36' with YOUR actual IP address
CREATE NETWORK RULE NETWORK_CONFIG.PUBLIC.PG_INGRESS_FROM_LOCAL
  TYPE = IPV4
  VALUE_LIST = ('193.179.119.36/32')    -- /32 = single IP address
  MODE = POSTGRES_INGRESS;

-- If your IP changes frequently, you can add multiple IPs:
-- VALUE_LIST = ('193.179.119.36/32', '198.51.100.10/32')
-- Or use a broader range: '193.179.119.0/24'

-- Step 3: Create the network policy using the rule
CREATE NETWORK POLICY MY_LOCAL_ACCESS_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('NETWORK_CONFIG.PUBLIC.PG_INGRESS_FROM_LOCAL')
  COMMENT = 'Allow access from my local machine';

-- Step 4: Apply the policy to your Postgres instance
ALTER POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION"
  SET NETWORK_POLICY = 'MY_LOCAL_ACCESS_POLICY';

-- Verify network policy is applied
SELECT 
  NAME,
  STATE,
  NETWORK_POLICY
FROM SNOWFLAKE.INFORMATION_SCHEMA.POSTGRES_INSTANCES
WHERE NAME = 'DEV-POSTGRES-INTRODUCTION';


-- =============================================
-- PART 4: INSTANCE MANAGEMENT
-- =============================================

-- START the Postgres instance (resume if suspended)
ALTER POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION" RESUME;

-- STOP the Postgres instance (suspend to save compute costs)
-- Storage costs continue even when suspended
ALTER POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION" SUSPEND;

-- Check instance state after operation
SELECT NAME, STATE 
FROM SNOWFLAKE.INFORMATION_SCHEMA.POSTGRES_INSTANCES
WHERE NAME = 'DEV-POSTGRES-INTRODUCTION';

-- IMPORTANT: Always suspend your instance when not in use to minimize costs!


-- =============================================
-- PART 5: COST MONITORING
-- =============================================
-- Documentation: https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-cost

-- View all service types in metering history
SELECT 
  SERVICE_TYPE, 
  COUNT(*) as record_count
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
GROUP BY SERVICE_TYPE
ORDER BY SERVICE_TYPE;

-- View Postgres compute usage (credit consumption)
-- NOTE: ACCOUNT_USAGE views have 45 min to 3 hour latency
SELECT 
  SERVICE_TYPE,
  SUM(CREDITS_USED) AS TOTAL_CREDITS,
  SUM(CREDITS_USED) * 4 AS ESTIMATED_USD    -- Assuming $4 per credit (adjust based on your rate)
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE SERVICE_TYPE IN ('POSTGRES_COMPUTE', 'POSTGRES_COMPUTE_HA')
  AND START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY SERVICE_TYPE
ORDER BY TOTAL_CREDITS DESC;

-- View Postgres storage usage history
SELECT 
  USAGE_DATE,
  POSTGRES_INSTANCE_NAME,
  AVERAGE_STORAGE_BYTES / (1024*1024*1024) AS AVERAGE_STORAGE_GB,
  AVERAGE_FAILOVER_STORAGE_BYTES / (1024*1024*1024) AS AVERAGE_FAILOVER_STORAGE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.POSTGRES_STORAGE_USAGE_HISTORY
WHERE POSTGRES_INSTANCE_NAME = 'DEV-POSTGRES-INTRODUCTION'
ORDER BY USAGE_DATE DESC
LIMIT 30;

-- Daily cost breakdown (last 30 days)
SELECT 
  DATE(START_TIME) AS usage_date,
  SERVICE_TYPE,
  SUM(CREDITS_USED) AS daily_credits,
  SUM(CREDITS_USED) * 4 AS estimated_daily_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE SERVICE_TYPE LIKE '%POSTGRES%'
  AND START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY DATE(START_TIME), SERVICE_TYPE
ORDER BY usage_date DESC, SERVICE_TYPE;

-- Hourly usage pattern (last 7 days)
SELECT 
  DATE_TRUNC('hour', START_TIME) AS usage_hour,
  SERVICE_TYPE,
  SUM(CREDITS_USED) AS hourly_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE SERVICE_TYPE IN ('POSTGRES_COMPUTE', 'POSTGRES_COMPUTE_HA')
  AND START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('hour', START_TIME), SERVICE_TYPE
ORDER BY usage_hour DESC;


-- =============================================
-- PART 6: OPTIONAL - RESOURCE MONITOR
-- =============================================
-- Create a resource monitor to control costs
-- This sends notifications and can auto-suspend when credit quota is reached

CREATE RESOURCE MONITOR postgres_dev_monitor
  WITH CREDIT_QUOTA = 100                    -- Set your monthly credit limit
  TRIGGERS 
    ON 75 PERCENT DO NOTIFY                  -- Email notification at 75%
    ON 90 PERCENT DO SUSPEND                 -- Suspend at 90%
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;     -- Immediately suspend at 100%

-- View resource monitors
SHOW RESOURCE MONITORS;


-- =============================================
-- PART 7: CLEANUP (OPTIONAL)
-- =============================================
-- Run these commands when you want to remove everything

-- Drop the Postgres instance (WARNING: This deletes all data!)
-- DROP POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION";

-- Drop network policy
-- DROP NETWORK POLICY MY_LOCAL_ACCESS_POLICY;

-- Drop network rule
-- DROP NETWORK RULE NETWORK_CONFIG.PUBLIC.PG_INGRESS_FROM_LOCAL;

-- Drop network config database
-- DROP DATABASE NETWORK_CONFIG;


-- =============================================
-- PART 8: CONNECTION INFORMATION SUMMARY
-- =============================================
-- After creating the instance, use DESCRIBE to get connection details:
-- 
-- DESCRIBE POSTGRES INSTANCE "DEV-POSTGRES-INTRODUCTION";
--
-- This will provide:
-- - Connection URL (postgres://...)
-- - Admin Username (snowflake_admin)
-- - Admin Password (auto-generated)
-- - Host (for JDBC connections)
-- - Port (5432)
--
-- DBeaver Connection Settings:
-- - Host: [from DESCRIBE output]
-- - Port: 5432
-- - Database: postgres (default database inside the instance)
-- - Username: snowflake_admin
-- - Password: [from DESCRIBE output]
-- - SSL: Enable with mode 'require' (no certificate verification)
--
-- JDBC URL format:
-- jdbc:postgresql://[HOST]:5432/postgres?user=snowflake_admin&password=[PASSWORD]&ssl=true&sslmode=require


-- =============================================
-- END OF SNOWFLAKE SETUP SCRIPT
-- =============================================
-- Next steps:
-- 1. Wait for instance STATE = 'ACTIVE'
-- 2. Get connection details with DESCRIBE command
-- 3. Connect via DBeaver using the provided credentials
-- 4. Run the postgres_sample_database.sql script in DBeaver
-- =============================================

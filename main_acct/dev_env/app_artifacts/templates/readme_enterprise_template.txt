# <APP_TITLE> - ENTERPRISE

## Prerequisites
Prior to using the this app, the following one-time setup steps below **must be executed**.  Please make ONLY the changes mentioned in the **NOTES** below.  
### Any other changes may result in the setup process failing.

**NOTES:**  
- Replace ```<MY_ROLE>``` with either the ACCOUNTADMIN role or a role that has been granted ACCOUNTADMIN. 
  - ACCOUNTADMIN privileges are required for this step.
- Replace ```<MY_WAREHOUSE>``` with the desired warehouse.  
- For this step, an XSMALL warehouse can be used.
- Replace all ```<APP_NAME>``` references with the name of the native app, as installed in the consumer account.
  - The App Name can be found by executing (as ACCOUNTADMIN or the role that installed the app):  ```SHOW APPLICATIONS;``` (reference the **name** column)

```
SET APP_NAME = '<APP_NAME>';
SET MY_ROLE = '<MY_ROLE>';
SET MY_WAREHOUSE = '<MY_WAREHOUSE>';

USE ROLE IDENTIFIER($MY_ROLE);
USE WAREHOUSE IDENTIFIER($MY_WAREHOUSE);

CREATE DATABASE IF NOT EXISTS SIDECAR;
CREATE SCHEMA IF NOT EXISTS SIDECAR.RUNNER;
CREATE OR REPLACE PROCEDURE SIDECAR.RUNNER.SidecarRunner(app_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_sidecar_sql'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark.functions import col
  
def run_sidecar_sql(session, app_name):
  df = session.sql(f"SELECT * FROM {app_name}.SETUP.SQL").collect()
  for row in df:
    session.sql(row[0]).collect()
  return "success"
$$;

-- use app database
USE DATABASE IDENTIFIER($APP_NAME);

--load install consumer setup sql commands
CALL UTIL_APP.LOAD_INSTALL_SQL($APP_NAME, (select current_user()));

-- Parameters:
  -- app_name VARCHAR - The name of the Native App installed
  -- app_user VARCHAR - The current user

--call SidecarRunner to execute commands
CALL SIDECAR.RUNNER.SidecarRunner($APP_NAME);

-- Parameters:
  -- app_name VARCHAR - The name of the Native App installed

USE ROLE IDENTIFIER($MY_ROLE);

--create the event table, if the account does not have one
CREATE OR REPLACE PROCEDURE C_[[APP_CODE]]_HELPER_DB.PRIVATE.DETECT_EVENT_TABLE()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
        snowflake.execute({sqlText:"SHOW PARAMETERS LIKE '%%event_table%%' IN ACCOUNT"});
        var table_name = snowflake.execute({sqlText:'SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));'});
        table_name.next();
        table_name = table_name.getColumnValue(1);
        if(table_name == '')
        {
           snowflake.execute({sqlText:"CREATE DATABASE IF NOT EXISTS EVENTS"});
           snowflake.execute({sqlText:"CREATE SCHEMA IF NOT EXISTS EVENTS"});
           snowflake.execute({sqlText:"CREATE EVENT TABLE IF NOT EXISTS EVENTS"});
           snowflake.execute({sqlText:"ALTER ACCOUNT SET EVENT_TABLE = EVENTS.EVENTS.EVENTS"});
           return 'ADDED EVENT TABLE'
        }
        return table_name
    $$;

CALL C_[[APP_CODE]]_HELPER_DB.PRIVATE.DETECT_EVENT_TABLE();

ALTER APPLICATION IDENTIFIER($APP_NAME) SET SHARE_EVENTS_WITH_PROVIDER=TRUE;

-- use app database
USE DATABASE IDENTIFIER($APP_NAME);

--insert initial logs --REQUIRED TO ENABLE APP
CALL PROCS_APP.LOG_SHARE_INSERT();

--grant account privileges to application
GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_NAME);
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_NAME);

--use app database
USE DATABASE IDENTIFIER($APP_NAME);

--call configure_tracker
CALL UTIL_APP.CONFIGURE_TRACKER();

--unset session variables
UNSET (APP_NAME, MY_ROLE, MY_WAREHOUSE);

SELECT 'Done' AS STATUS;
```

## App Usage
Please refer to the Consumer Guide for details on how to use the app.
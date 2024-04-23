# <APP_TITLE>  - FREE

## Prerequisites
Prior to using the this app, the following one-time setup steps below must be executed:

**NOTES:**  
- The ACCOUNTADMIN role is required for this step.
- Replace ```<MY_WAREHOUSE>``` with the desired warehouse.  
- For this step, an XSMALL warehouse can be used.
- Replace all ```<APP_NAME>``` references with the name of the native app, as installed in the consumer account.
  - The App Name can be found by executing (as ACCOUNTADMIN or the role that installed the app):  ```SHOW APPLICATIONS;``` (reference the **name** column)

```
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE <MY_WAREHOUSE>;

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


--load install consumer setup sql commands
CALL <APP_NAME>.UTIL_APP.LOAD_INSTALL_SQL('<APP_NAME>', (select current_user()));

-- Parameters:
  -- app_name VARCHAR - The name of the Native App installed
  -- app_user VARCHAR - The current user

--call SidecarRunner to execute commands
CALL SIDECAR.RUNNER.SidecarRunner('<APP_NAME>');

-- Parameters:
  -- app_name VARCHAR - The name of the Native App installed

USE ROLE ACCOUNTADMIN;

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

ALTER APPLICATION <APP_NAME> SET SHARE_EVENTS_WITH_PROVIDER=TRUE;

--insert initial logs
CALL <APP_NAME>.PROCS_APP.LOG_SHARE_INSERT();
```

## App Usage
Please refer to the Consumer Guide for details on how to use the app.
# <APP_TITLE> - FREE

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
[[TC_ACCESS]]
--grant account privileges to application
GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_NAME);
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_NAME);

-- use app database
USE DATABASE IDENTIFIER($APP_NAME);
[[TC_FLAG]]
--insert initial logs --REQUIRED TO ENABLE APP
CALL PROCS_APP.LOG_SHARE_INSERT();

--call configure_tracker  --REQUIRED TO ENABLE APP
CALL UTIL_APP.CONFIGURE_TRACKER();

--unset session variables
UNSET (APP_NAME, MY_ROLE, MY_WAREHOUSE);

SELECT 'Done' AS STATUS;
```

## App Usage
Please refer to the Consumer Guide for details on how to use the app.
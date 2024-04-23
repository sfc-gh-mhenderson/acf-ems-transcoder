/*************************************************************************************************************
Procedure:          EVENTS.EVENTS.STREAM_EVENTS
Create Date:        2023-10-19
Author:             Marc Henderson
Description:        This procedure creates a stream on the event table and tasks to insert events into the 
                    replicated/shared table.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): STREAM: <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>.EVENTS.EVENTS_STREAM
                    TABLE:  <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>.EVENTS.EVENTS_TEMP
                    TASKS:  <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>.EVENTS.EVENTS_TASK_i (i = 1-60)
Used By:            Provider
Parameter(s):       app_codes ARRAY - array of app_codes to remove app events for
Usage:              CALL EVENTS.EVENTS.STREAM_EVENTS(TO_ARRAY('<app_codes>'));

Copyright © 2024 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-09          Marc Henderson                      Initial build
2024-02-29          Marc Henderson                      Added support for creating stream/tasks for multiple 
                                                        apps per run                                                    
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 02_create_stream_events_procedure.sql
!print **********

--set session vars
SET SHARE_DB = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION();

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE EVENTS_WH;
USE DATABASE EVENTS;

CREATE OR REPLACE PROCEDURE EVENTS.STREAM_EVENTS(app_codes ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":6},"attributes":{"role":"provider","component":"stream_events"}}'
EXECUTE AS CALLER
AS
$$
  function sleep(milliseconds) {
    const date = Date.now();
    let currentDate = null;
    do {
      currentDate = Date.now();
    } while (currentDate - date < milliseconds);
  }

  try {
    //get current region
    var rset = snowflake.execute({sqlText: `SELECT CURRENT_REGION();`});
    rset.next();
    let current_region = rset.getColumnValue(1);

    //create stream and tasks for each app code
    for(i = 0; i <= APP_CODES.length; i++) {
      //get app code
      let app_code = APP_CODES[i];

      //create stream on events table
      snowflake.execute({sqlText:`CREATE OR REPLACE STREAM ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_STREAM
                                      ON TABLE EVENTS.EVENTS.EVENTS
                                      APPEND_ONLY = TRUE
                                      SHOW_INITIAL_ROWS = TRUE
                                      COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":6},"attributes":{"role":"provider","component":"${app_code}_events_stream"}}';`});
      
      //create tasks to insert events into the replicated/shared table
      for(i = 1; i <= 20; i++) {
          snowflake.execute({sqlText:`CREATE OR REPLACE TASK ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_TASK_${i}
                                        SCHEDULE = '1 minute'
                                        COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":6},"attributes":{"role":"provider","component":"${app_code}_events_task_${i}"}}'
                                        WHEN
                                          SYSTEM$STREAM_HAS_DATA('${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_STREAM')
                                        AS
                                          EXECUTE IMMEDIATE
                                          \$\$
                                            DECLARE
                                              uuid VARCHAR;
                                              c_uuid CURSOR FOR SELECT REPLACE(UUID_STRING(), '-', '_');
                                            BEGIN
                                              OPEN c_uuid;
                                              FETCH c_uuid INTO uuid;

                                              LET create_temp_table_stmt VARCHAR := 'CREATE OR REPLACE TABLE ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_TEMP_' || uuid || ' (
                                                                                      timestamp TIMESTAMP_NTZ
                                                                                      ,start_timestamp TIMESTAMP_NTZ
                                                                                      ,observed_timestamp TIMESTAMP_NTZ
                                                                                      ,trace OBJECT
                                                                                      ,resource OBJECT
                                                                                      ,resource_attributes OBJECT
                                                                                      ,scope OBJECT
                                                                                      ,scope_attributes OBJECT
                                                                                      ,record_type STRING
                                                                                      ,record OBJECT
                                                                                      ,record_attributes OBJECT 
                                                                                      ,value VARIANT
                                                                                      ,exemplars ARRAY
                                                                                    );';
                                              EXECUTE IMMEDIATE :create_temp_table_stmt;

                                              LET insert_temp_table_stmt VARCHAR := 'INSERT INTO ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_TEMP_' || uuid || ' SELECT
                                                                                      timestamp
                                                                                      ,start_timestamp
                                                                                      ,observed_timestamp
                                                                                      ,trace
                                                                                      ,resource
                                                                                      ,resource_attributes
                                                                                      ,scope
                                                                                      ,scope_attributes
                                                                                      ,record_type
                                                                                      ,record
                                                                                      ,record_attributes
                                                                                      ,value
                                                                                      ,exemplars
                                                                                    FROM ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_STREAM
                                                                                    WHERE value:app_code = \\\'&{APP_CODE}\\\';';
                                              EXECUTE IMMEDIATE :insert_temp_table_stmt;

                                              LET insert_event_table_stmt VARCHAR := 'INSERT INTO ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS SELECT * FROM ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_TEMP_' || uuid;
                                              EXECUTE IMMEDIATE :insert_event_table_stmt;

                                              LET drop_temp_table_stmt VARCHAR := 'DROP TABLE ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_TEMP_' || uuid;
                                              EXECUTE IMMEDIATE :drop_temp_table_stmt;

                                              RETURN 'done';
                                            END;
                                          \$\$;
          `});
      }

      //start tasks
      for(i = 1; i <= 20; i++) {
          //sleep 3 secs before starting the next task
          if (i > 1) {
            sleep(3000); 
          }
          snowflake.execute({sqlText:`ALTER TASK ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS_TASK_${i} RESUME;`});
      }
    }

    return `Success`;
  
  } catch (err) {
    var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");
    return `Error: ${result}`;
  }
    
$$
;

--call STREAM_EVENTS
CALL EVENTS.STREAM_EVENTS(TO_ARRAY('&APP_CODE'));

--unset vars
UNSET (SHARE_DB);

!print **********
!print End 02_create_stream_events_procedure.sql
!print **********
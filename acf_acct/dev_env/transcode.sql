USE DATABASE P_EMS_TRANSCODER_SOURCE_DB_DEV;
USE SCHEMA P_EMS_TRANSCODER_SOURCE_DB_DEV.PROCS_APP;

create or replace procedure transcode(
        inputTableName VARCHAR,
        pidColumn VARCHAR,
        luidColumn VARCHAR,
        partnerList VARCHAR,
        resultsTableName VARCHAR
    )
    returns String
    language python
    runtime_version = 3.11
    packages =('snowflake-snowpark-python')
    handler = 'main'
    as 
$$
import snowflake.snowpark as snowpark
import json

def main(session: snowpark.Session, inputTableName: str, pidColumn: str, luidColumn: str, partnerList: str, resultsTableName: str): 
    try:
        #set startTimestamp
        startTimestamp = session.sql("SELECT SYSDATE()").collect()[0][0]

        # Get the Request ID
        dataframe = session.sql("SELECT REQUEST_ID FROM UTIL_APP.REQUEST_ID_TEMP")
        requestId = dataframe.collect()[0][0]

        #set proc parameters string
        procParametersEsc = f"{inputTableName},{pidColumn},{luidColumn},[{partnerList}],{resultsTableName}"

        # Get local consumer Snowflake Account Locator
        dataframe = session.sql("SELECT CURRENT_ACCOUNT()");
        accountLocator = dataframe.collect()[0][0]

        # Get local app key
        dataframe = session.sql("SELECT APP_KEY FROM APP.APP_KEY")
        localAppKey = dataframe.collect()[0][0]

        # Get App license/mode
        dataframe = session.sql("SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'app_mode'")
        appLicense = dataframe.collect()[0][0]

        #get total record count from input table - this will be used to set each partner transcoded record count, if allowed
        totalRecords = session.sql(f"SELECT COUNT(*) FROM {inputTableName}").collect()[0][0]

        # Set the local Consumer Name select based on the App mode / license: 
        # Prefix PD_ to consumer name if this is a paid license 
        # Then check if either enterprise or an optional demo mode 
        # If adding a demo app_mode type, it is recommended to copy the ENTERPRISE app mode
        # Else leave it alone
        getConsumerNameSQL = "SELECT CURRENT_ACCOUNT_NAME() as acct_name"

        if appLicense == "paid":
            getConsumerNameSQL = "SELECT 'PD_' || CURRENT_ACCOUNT_NAME() as acct_name"

        if "enterprise" in appLicense.lower():
            getConsumerNameSQL = "SELECT consumer_name FROM UTIL_APP.METADATA_C_V"

        # Get local Consumer name for Experian Client (not Experian Partner)
        dataframe = session.sql(getConsumerNameSQL)
        localConsumerName = dataframe.collect()[0][0]

        # Get local Consumer Key for Decrypting
        dataframe = session.sql("SELECT value FROM METADATA.METADATA_V " +
                                "WHERE LOWER(key) = 'client_code' " +
                                f"AND ACCOUNT_LOCATOR = '{accountLocator}'"
                               )
        localConsumerKey = dataframe.collect()[0][0]

        #set key_type
        key_type = ""
        
        if pidColumn and luidColumn:
            key_type = "PID_LUID"

        if pidColumn and not luidColumn:
            key_type = "PID"

        if not pidColumn and luidColumn:
            key_type = "LUID"

        if not pidColumn and not luidColumn:
            #log error msg
            session.sql(f"""CALL UTIL_APP.APP_LOGGER('{accountLocator}', '{localConsumerName}', '{localAppKey}', '{appLicense}', 'log', 'request', '[{{"request_id":"{requestId}"}}, {{"proc_name":"transcode"}}, {{"proc_parameters":"{procParametersEsc}"}}]', SYSDATE(), 'ERROR', '"PID or  LUID column was not submitted. Please resubmit with either a PID and/or LUID column value."')""").collect()
        
            return "ERROR: PID or  LUID column was not submitted. Please resubmit with either a PID and/or LUID column value."

        # Get the secured Experian Partner keys for allowed partners/collaborators
        allowedPartnersDF = session.sql("SELECT f.value:partner_name::STRING as partner_name," +
                                            "f.value:partner_client_code::STRING as partner_client_code," +
                                            "f.value:access_expiration_timestamp::TIMESTAMP as access_expiration_timestamp , " +
                                            "CASE" +
                                            "    WHEN f.value:access_expiration_timestamp::TIMESTAMP >= SYSDATE() THEN 'Unexpired'" +
                                            "    ELSE 'Expired' " +
                                            "END as access_window_status " +
                                        "FROM TABLE(FLATTEN(input => parse_json(" +
                                            "SELECT value " +
                                            "FROM METADATA.METADATA_V " +
                                            "WHERE LOWER(key) = 'allowed_partners' " +
                                            "AND ACCOUNT_LOCATOR = '" + accountLocator + 
                                        "'), path => 'allowed_partners')) f")

        # First part of SQL to generate Results Table
        resultsTableSQL = f"CREATE OR REPLACE TABLE {resultsTableName} AS SELECT *, "

        #create a list to store partner-specific objects that include partner_name and totalRecords_transcoded, 
        partnerTranscodingMetricsList = []

        for partnerName in partnerList.split(","):
            partnerName = partnerName.strip() #remove any white space on either side of name
        
            #set partner transcoded count to the total_record count
            partnerTotalRecordsTranscoded = totalRecords

            # Collect Partner allowed / expired statusses
            status_str = ""
            if len(allowedPartnersDF.where(allowedPartnersDF.PARTNER_NAME == partnerName).select("PARTNER_CLIENT_CODE").collect()) == 0:
                status_str = "Not Allowed"
            else: 
                if allowedPartnersDF.where(allowedPartnersDF.PARTNER_NAME == partnerName).select("ACCESS_WINDOW_STATUS").collect()[0][0] == 'Expired':
                    status_str = "Expired"

            # Add Columns to CREATE Results Table SQL statement
            # Check partner Allowed / Expired statusses from access_expiration_timestamp from METADATA.METADATA_V, if true then do not return keys 
            if status_str in ["Not Allowed", "Expired"]:
                #set partnerTotalRecordsTranscoded to 0, since the partner cannot be accessed
                partnerTotalRecordsTranscoded = 0

                if len(pidColumn) > 0 or pidColumn.lower() != 'n/a':
                    resultsTableSQL += f"'{status_str}' AS {partnerName.replace(' ', '_')}_PID,"
                    
                if len(luidColumn) > 0 or luidColumn.lower() != 'n/a':
                    resultsTableSQL += f"'{status_str}' AS {partnerName.replace(' ', '_')}_LUID,"
            else:
                partner_code = allowedPartnersDF.where(allowedPartnersDF.PARTNER_NAME == partnerName).select("PARTNER_CLIENT_CODE").collect()[0][0]
                
                if len(pidColumn) > 0 or pidColumn.lower() != 'n/a':
                    resultsTableSQL += f"""FUNCS_APP.SCRAMBLE(FUNCS_APP.DESCRAMBLE({pidColumn}, '{localConsumerKey}'), '{partner_code}') AS
                                       {partnerName.replace(' ', '_')}_PID,"""
                                       
                if len(luidColumn) > 0 or luidColumn.lower() != 'n/a':
                    resultsTableSQL += f"""FUNCS_APP.SCRAMBLE(FUNCS_APP.DESCRAMBLE({luidColumn}, '{localConsumerKey}'), '{partner_code}') AS
                                   {partnerName.replace(' ', '_')}_LUID,"""

            #generate partner transcoding metrics object, then append it to the partnerTranscodingMetricsList list
            partner_transcoding_dict ={"partner_name":f"{partnerName}","total_records_transcoded":partnerTotalRecordsTranscoded, "notes":f"{status_str}"}

            if partner_transcoding_dict not in partnerTranscodingMetricsList:
                partnerTranscodingMetricsList.append(partner_transcoding_dict)

        #strip off final comma
        resultsTableSQL = resultsTableSQL.rstrip(',')
                
        # Final part of SQL to generate Results Table
        resultsTableSQL += f" FROM {inputTableName}"

        # Create resutls table 🤞
        session.sql(resultsTableSQL).collect()

        #set endTimestamp
        endTimestamp = session.sql("SELECT SYSDATE()").collect()[0][0]

        # Write results to run_tracker
        session.sql(f"INSERT INTO APP.RUN_TRACKER(timestamp, request_id, request_type, input_table, output_table) VALUES(SYSDATE(), " \
                    f"'{requestId}', 'transcode', '{inputTableName}', '{resultsTableName}');").collect()

        #log transcoding metrics via the APP_LOGGER (adds metrics to the events table)
        session.sql(f"""CALL UTIL_APP.APP_LOGGER('{accountLocator}'
                                                            ,'{localConsumerName}'
                                                            ,'{localAppKey}'
                                                            ,'{appLicense}'
                                                            ,'metric'
                                                            ,'request'
                                                            ,'[{{"request_id":"{requestId}"}}, {{"proc_name":"transcode"}}, {{"proc_parameters":"{procParametersEsc}"}}]'
                                                            ,SYSDATE()
                                                            ,'COMPLETE'
                                                            ,'{{
                                                                "metric_type":"transcode_summary",
                                                                "metrics":{{
                                                                    "input_object":"{inputTableName}",
                                                                    "key_type":"{key_type}",
                                                                    "pid_column":"{pidColumn}",
                                                                    "luid_column":"{luidColumn}",
                                                                    "partners":{json.dumps(partnerTranscodingMetricsList)},
                                                                    "results_table":"{resultsTableName}",
                                                                    "total_records":{totalRecords},
                                                                    "start_timestamp":"{startTimestamp}",
                                                                    "end_timestamp":"{endTimestamp}",
                                                                }}
                                                            }}'
                                                            )
                                                        """).collect()

        #log transcoding metrics via the METRICS_LOGGER (adds metrics to the local metrics table)
        session.sql(f"""CALL UTIL_APP.METRICS_LOGGER('{accountLocator}'
                                                            ,'{localConsumerName}'
                                                            ,'{localAppKey}'
                                                            ,'{appLicense}'
                                                            ,'metric'
                                                            ,'request'
                                                            ,'[{{"request_id":"{requestId}"}}, {{"proc_name":"transcode"}}, {{"proc_parameters":"{procParametersEsc}"}}]'
                                                            ,SYSDATE()
                                                            ,'COMPLETE'
                                                            ,'{{
                                                                "metric_type":"transcode_summary",
                                                                "metrics":{{
                                                                    "input_object":"{inputTableName}",
                                                                    "key_type":"{key_type}",
                                                                    "pid_column":"{pidColumn}",
                                                                    "luid_column":"{luidColumn}",
                                                                    "partners":{json.dumps(partnerTranscodingMetricsList)},
                                                                    "results_table":"{resultsTableName}",
                                                                    "total_records":{totalRecords},
                                                                    "start_timestamp":"{startTimestamp}",
                                                                    "end_timestamp":"{endTimestamp}"
                                                                }}
                                                            }}'
                                                            )
                                                        """).collect()
                                                        
        #Log trancoding complete
        session.sql(f"""CALL UTIL_APP.APP_LOGGER('{accountLocator}'
                                                            ,'{localConsumerName}'
                                                            ,'{localAppKey}'
                                                            ,'{appLicense}'
                                                            ,'log'
                                                            ,'request'
                                                            ,'[{{"request_id":"{requestId}"}}, {{"proc_name":"transcode"}}, {{"proc_parameters":"{procParametersEsc}"}}]'
                                                            ,SYSDATE()
                                                            ,'COMPLETE'
                                                            ,'Results are located in: {resultsTableName}.'
                                                            )
                                                        """).collect()

        return f"Results are located in: {resultsTableName}."

    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        #log error msg
        session.sql(f"""CALL UTIL_APP.APP_LOGGER('{accountLocator}', '{localConsumerName}', '{localAppKey}', '{appLicense}', 'log', 'request', '[{{"request_id":"{requestId}"}}, {{"proc_name":"transcode"}}, {{"proc_parameters":"{procParametersEsc}"}}]', SYSDATE(), 'ERROR', '"{error_eraw}"')""").collect()

        msg_return = "Error: " + error_eraw

        raise Exception(msg_return)     

$$
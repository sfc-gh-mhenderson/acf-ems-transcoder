create procedure transcode(
        inputTableName VARCHAR,
        pidColumn VARCHAR,
        luidColumn VARCHAR,
        partnerKeyType VARCHAR,
        partnerList VARCHAR,
        resultsTableName VARCHAR
    )
    returns String
    language python
    runtime_version = 3.11
    packages =('snowflake-snowpark-python')
    handler = 'main'
    as '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import json

def main(session: snowpark.Session, inputTableName: str, pidColumn: str, luidColumn: str, partnerKeyType: str, partnerList: str, resultsTableName: str): 
    try:
        #MARC: set startTimestamp
        startTimestamp = session.sql("SELECT SYSDATE()").collect()[0][0]
        
        # Get the Request ID
        dataframe = session.sql("SELECT REQUEST_ID FROM UTIL_APP.REQUEST_ID_TEMP")
        requestId = dataframe.collect()[0][0]

        #MARC: set proc parameters string
        procParametersEsc = f"{inputTableName},{pidColumn},{luidColumn},{partnerKeyType},{partnerList},{resultsTableName}"
    
        # Get local consumer Snowflake Account Locator
        dataframe = session.sql("SELECT CURRENT_ACCOUNT()");
        accountLocator = dataframe.collect()[0][0]
    
        # Get local app key
        dataframe = session.sql("SELECT APP_KEY FROM APP.APP_KEY")
        localAppKey = dataframe.collect()[0][0]
    
        # Get App license/mode
        dataframe = session.sql("SELECT value FROM APP.APP_MODE WHERE LOWER(key) = ''app_mode''")
        appLicense = dataframe.collect()[0][0]

        #MARC get total record count from input table - this will be used to set each partner transcoded record count, if allowed
        totalRecords = session.sql(f"SELECT COUNT(*) FROM {inputTableName}").collect()[0][0]
    
        # Set the local Consumer Name select based on the App mode / license: 
        # Prefix PD_ to consumer name if this is a paid license 
        # Then check if either enterprise or an optional demo mode 
        # If adding a demo app_mode type, it is recommended to copy the ENTERPRISE app mode
        # Else leave it alone
        getConsumerNameSQL = "SELECT CURRENT_ACCOUNT_NAME() as acct_name"
        
        if appLicense == "paid":
            getConsumerNameSQL = "SELECT ''PD_'' || CURRENT_ACCOUNT_NAME() as acct_name"
            
        if "enterprise" in appLicense.lower():
            getConsumerNameSQL = "SELECT consumer_name FROM UTIL_APP.METADATA_C_V"
    
        # Get local Consumer name for Experian Client (not Experian Partner)
        dataframe = session.sql(getConsumerNameSQL)
        localConsumerName = dataframe.collect()[0][0]
    
        # Get local Consumer Key for Decrypting
        dataframe = session.sql("SELECT value FROM METADATA.METADATA_V " +
                                "WHERE LOWER(key) = ''client_code'' " +
                                f"AND ACCOUNT_LOCATOR = ''{accountLocator}''"
                               )
        localConsumerKey = dataframe.collect()[0][0]
    
        # Get the secured Experian Partner keys for allowed partners/collaborators
        allowedPartnersDF = session.sql("SELECT f.value:partner_name::STRING as partner_name," +
                                            "f.value:partner_client_code::STRING as partner_client_code," +
                                            "f.value:access_expiration_timestamp::TIMESTAMP as access_expiration_timestamp , " +
                                            "CASE" +
                                            "    WHEN f.value:access_expiration_timestamp::TIMESTAMP >= CURRENT_TIMESTAMP() THEN ''Unexpired''" +
                                            "    ELSE ''Expired'' " +
                                            "END as access_window_status " +
                                        "FROM TABLE(FLATTEN(input => parse_json(" +
                                            "SELECT value " +
                                            "FROM METADATA.METADATA_V " +
                                            "WHERE LOWER(key) = ''allowed_partners'' " +
                                            "AND ACCOUNT_LOCATOR = ''" + accountLocator + 
                                        "''), path => ''allowed_partners'')) f")
        allowedPartnersDF.show()

        # Now set the keys for the requested partners by finding each key in the requested partner list argument
        partnerNames = json.loads(partnerList)
        partnerNameKey = {}
        # First part of SQL to generate Results Table
        resultsTableSQL = f"CREATE OR REPLACE TABLE {resultsTableName} AS SELECT *, "

        #MARC:  create a list to store partner-specific objects that include partner_name and totalRecords_transcoded, 
        partnerTranscodingMetricsList = []

        for item in partnerNames:
            #print(item[''partner_name''])
            partnerName = item[''partner_name'']
            
            #MARC set partner transcoded count to the total_record count
            partnerTotalRecordsTranscoded = totalRecords
            
            # Collect Partner allowed / expired statusses
            if len(allowedPartnersDF.where(allowedPartnersDF.PARTNER_NAME == partnerName).select("PARTNER_CLIENT_CODE").collect()) == 0:
                partnerNameKey[partnerName] = "Not Allowed"
            else:
                if allowedPartnersDF.where(allowedPartnersDF.PARTNER_NAME == partnerName).select("ACCESS_WINDOW_STATUS").collect()[0][0] == ''Expired'':
                    partnerNameKey[partnerName] = ''Expired''
                else:
                    partnerNameKey[partnerName] = allowedPartnersDF.where(allowedPartnersDF.PARTNER_NAME == partnerName).select("PARTNER_CLIENT_CODE").collect()[0][0]

            # Add Columns to CREATE Results Table SQL statement
            # This control logic has two main branches based on PID/LUID argument passed: the first condition will return PID and LUID columns, while the else condition will return either PID or LUID
            if partnerKeyType == "PID_LUID": # This partnerKeyType creates outputs columns for both key types
                # print(partnerNameKey[partnerName])
                # Check partner Allowed / Expired statusses from access_expiration_timestamp from METADATA.METADATA_V, if true then do not return keys 
                if partnerNameKey[partnerName] == "Not Allowed" or partnerNameKey[partnerName] == ''Expired'':
                    #MARC: set partnerTotalRecordsTranscoded to 0, since the partner cannot be accessed
                    partnerTotalRecordsTranscoded = 0
                    
                    resultsTableSQL += f"''{partnerNameKey[partnerName]}'' AS " + f''{partnerName.replace(" ", "_")}_PID,'' \\
                                       f"''{partnerNameKey[partnerName]}'' AS " + f''{partnerName.replace(" ", "_")}_LUID,''
                else:
                    resultsTableSQL += f"FUNCS_APP.SCRAMBLE(FUNCS_APP.DESCRAMBLE({pidColumn}, ''{localConsumerKey}''), ''{partnerNameKey[partnerName]}'') AS " \\
                                       f''{partnerName.replace(" ", "_")}_PID,'' \\
                                       f"FUNCS_APP.SCRAMBLE(FUNCS_APP.DESCRAMBLE({luidColumn}, ''{localConsumerKey}''), ''{partnerNameKey[partnerName]}'') AS " \\
                                       f''{partnerName.replace(" ", "_")}_LUID,''
            else: # This partnerKeyType creates either PID or LUID outputs columns depending on the argument passed
                # Check partner Allowed / Expired statusses again from access_expiration_timestamp from METADATA.METADATA_V, if true then do not return keys 
                if partnerNameKey[partnerName] == "Not Allowed" or partnerNameKey[partnerName] == ''Expired'':
                    #MARC: set partnerTotalRecordsTranscoded to 0, since the partner cannot be accessed
                    partnerTotalRecordsTranscoded = 0
                    resultsTableSQL += f"''{partnerNameKey[partnerName]}'') AS "
                else:
                    resultsTableSQL += f"FUNCS_APP.SCRAMBLE(FUNCS_APP.DESCRAMBLE({partnerKeyType}, ''{localConsumerKey}''), ''{partnerNameKey[partnerName]}'') AS "
    
                resultsTableSQL += f''{partnerName.replace(" ", "_")}_{partnerKeyType},''

            #MARC:  generate partner transcoding metrics object, then append it to the partnerTranscodingMetricsList list
            partner_transcoding_dict ={"partner_name":f"{partnerName}","total_records_transcoded":partnerTotalRecordsTranscoded}
            
            if partner_transcoding_dict not in partnerTranscodingMetricsList:
                partnerTranscodingMetricsList.append(partner_transcoding_dict)
    
        # Final part of SQL to generate Results Table
        resultsTableSQL += f"FROM {inputTableName}"
        print("-------------resultsTableSQL--------------")
        print(resultsTableSQL)
    
        # Create resutls table 🤞
        session.sql(resultsTableSQL).collect()

        #MARC: set endTimestamp
        endTimestamp = session.sql("SELECT SYSDATE()").collect()[0][0]
        
        # Write results to run_tracker
        session.sql(f"INSERT INTO APP.RUN_TRACKER(timestamp, request_id, request_type, input_table, output_table) VALUES(SYSDATE(), " \\
                    f"''{requestId}'', ''transcode'', ''{inputTableName}'', ''{resultsTableName}'');").collect()

        #MARC log transcoding metrics via the APP_LOGGER (adds metrics to the events table)
        appLogSQL = ("CALL UTIL_APP.APP_LOGGER(" +
                            f"''{accountLocator}''," +
                            f"''{localConsumerName}''," +
                            f"''{localAppKey}''," +
                            f"''{appLicense}''," +
                            "''metric''," + 
                            "''request''," +
                            "''[" +
                                f''{{"request_id": "{requestId}"}},'' +
                                f''{{"proc_name": "transcode"}},'' +
                                f''{{"proc_parameters": "{procParametersEsc}"}}'' +
                            "]''," +
                            "SYSDATE()," +
                            "''COMPLETE''," +
                            "''{" +
                                ''"metric_type": "transcode_summary",'' +
                                ''"metrics":{'' +
                                    f''"input_object": "{inputTableName}",'' +
                                    f''"key_type": "{partnerKeyType}",'' +
                                    f''"pid_column": "{pidColumn}",'' +
                                    f''"luid_column": "{luidColumn}",'' +
                                    ''"partners": "'' + json.dumps(partnerTranscodingMetricsList, indent=2) + ''",'' +
                                    f''"results_table": "{resultsTableName}",'' +
                                    f''"total_records": {totalRecords},'' +
                                    f''"start_timestamp": "{startTimestamp}",'' +
                                    f''"end_timestamp": "{endTimestamp}"'' +
                                "}" +
                            "}''" +
                        ")"
                    )
        session.sql(appLogSQL).collect()

        #MARC log transcoding metrics via the METRICS_LOGGER (adds metrics to app local metrics table)
        metricsLogSQL = (''CALL UTIL_APP.METRICS_LOGGER('' +
                                f"''{accountLocator}''," +
                                f"''{localConsumerName}''," +
                                f"''{localAppKey}''," +
                                f"''{appLicense}''," +
                                "''metric''," +
                                "''request''," +
                                "''[" +
                                    f''{{"request_id": "{requestId}"}},'' +
                                    f''{{"proc_name": "transcode"}},'' +
                                    f''{{"proc_parameters": "{procParametersEsc}"}}'' +
                                "]''," +
                                "SYSDATE()," +
                                "''COMPLETE''," +
                                "''{" +
                                    ''"metric_type": "transcode_summary",'' +
                                    ''"metrics":{'' +
                                        f''"input_object": "{inputTableName}",'' +
                                        f''"key_type": "{partnerKeyType}",'' +
                                        f''"pid_column": "{pidColumn}",'' +
                                        f''"luid_column": "{luidColumn}",'' +
                                        ''"partners": "'' + json.dumps(partnerTranscodingMetricsList, indent=2) + ''",'' +
                                        f''"results_table": "{resultsTableName}",'' +
                                        f''"total_records": {totalRecords},'' +
                                        f''"start_timestamp": "{startTimestamp}",'' +
                                        f''"end_timestamp": "{endTimestamp}"'' +
                                    "}" +
                                "}''" +
                            ")"
                        )
        session.sql(metricsLogSQL).collect()
        
        #Log trancoding complete
        procParams = f"{inputTableName}, {partnerKeyType}, {partnerList}, {resultsTableName}"
        session.sql(f"CALL UTIL_APP.APP_LOGGER(''{accountLocator}'', ''{localConsumerName}'', ''{localAppKey}'', ''{appLicense}'', ''log'', ''request''," \\
                    f''[{{"request_id":"{requestId}"}}, {{"proc_name":"transcode"}}, {{"proc_parameters":"{procParams}"}}]'' \\
                    f", SYSDATE(), ''COMPLETE'',''Results are located in: {resultsTableName}.''")
    
        return f"Results are located in: {resultsTableName}."
    
    except Exception as err:
        print(err)
        return f"Error: {err}"      
    
def test_main(session: snowpark.Session):
    return main(session,''P_URQUAN_JJ_SOURCE_DB_DEV.DATA.MY_PID_AND_LUID'', ''PID'', ''LUID'', ''PID_LUID'', ''[{"partner_name": "CLIENT_A"}, {"partner_name": "CLIENT_C"}, {"partner_name": "ORZ INC"}]'', ''OUTPUT_TABLE_NAME4'')';
/*************************************************************************************************************
Objects :           Provider Event Account Setup
Create Date:        2023-10-19
Author:             Marc Henderson
Description:        This script creates and sets the event table in the Provider's event account
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Objects(s):         WAREHOUSE:      <APP_CODE>_EVENTS_WH
                    DATABASE:       <APP_CODE>_EVENTS
                    SCHEMA:         <APP_CODE>_EVENTS.EVENTS
                    TABLE (EVENT):  <APP_CODE>_EVENTS.EVENTS.EVENTS  
                    SCHEMA:         <APP_CODE>_EVENTS.EVENTS.EVENTS
                    DATEBASE        <APP_CODE>_EVENTS_TO_<ACF_ACCOUNT_LOCATOR>
                    SCHEMA:         <APP_CODE>_EVENTS_TO_<ACF_ACCOUNT_LOCATOR>.EVENTS
                    TABLE:          <APP_CODE>_EVENTS_TO_<ACF_ACCOUNT_LOCATOR>.EVENTS.EVENTS
                    SHARE:          
                    
Used By:            Provider

Copyright © 2024 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-09          Marc Henderson                      Initial build
*************************************************************************************************************/

!print **********
!print Begin 01_account_setup.sql
!print **********

--set session vars
SET EVENT_WH = 'EVENTS_WH';
SET SHARE_DB = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION();
SET SHARE_SCH = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION() || '.EVENTS';
SET SHARE_TBL = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION() || '.EVENTS.EVENTS';
SET SHARE_NAME = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION() || '_SHARE';

USE ROLE ACCOUNTADMIN;

--create warehouse for processing events
CREATE OR REPLACE WAREHOUSE IDENTIFIER($EVENT_WH) WITH WAREHOUSE_SIZE = 'XSMALL' 
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":6},"attributes":{"role":"provider","component":"events_warehouse"}}';
;

USE WAREHOUSE IDENTIFIER($EVENT_WH) ;

--create event table
CREATE DATABASE IF NOT EXISTS EVENTS;
CREATE SCHEMA IF NOT EXISTS EVENTS.EVENTS;
CREATE EVENT TABLE IF NOT EXISTS EVENTS.EVENTS.EVENTS;

--set account's event table
ALTER ACCOUNT SET EVENT_TABLE = 'EVENTS.EVENTS.EVENTS';

--create database to share this app's events, using streams/tasks, to provider's main account.
CREATE OR REPLACE DATABASE IDENTIFIER($SHARE_DB);
CREATE OR REPLACE SCHEMA IDENTIFIER($SHARE_SCH);
CREATE OR REPLACE TABLE IDENTIFIER($SHARE_TBL) LIKE IDENTIFIER('EVENTS.EVENTS.EVENTS');
ALTER TABLE IDENTIFIER($SHARE_TBL) SET CHANGE_TRACKING=TRUE; --so the main account can stream changes

--create share
CREATE SHARE IF NOT EXISTS IDENTIFIER($SHARE_NAME);
GRANT USAGE ON DATABASE IDENTIFIER($SHARE_DB) TO SHARE IDENTIFIER($SHARE_NAME);
GRANT USAGE ON SCHEMA IDENTIFIER($SHARE_SCH) TO SHARE IDENTIFIER($SHARE_NAME);
GRANT SELECT ON TABLE IDENTIFIER($SHARE_TBL) TO SHARE IDENTIFIER($SHARE_NAME);

--create private listing to share events with main account
--NOTE:  LISTING API IS PUPR.  THE PROVIDER MUST ACCEPT TERMS FIRST
--see:  https://other-docs.snowflake.com/en/progaccess/listing-progaccess-about
CREATE EXTERNAL LISTING IDENTIFIER($SHARE_DB)
SHARE IDENTIFIER($SHARE_NAME) AS
$$
 title: "&{APP_CODE} App Events from account: &{EVENTS_ACCOUNT_LOCATOR}."
 description: "The share that contains &{APP_CODE} App Events from account: &{EVENTS_ACCOUNT_LOCATOR}."
 listing_terms:
   type: "OFFLINE"
 auto_fulfillment:
   refresh_schedule: "5 MINUTE"
   refresh_type: "FULL_DATABASE"
 targets:
   accounts: ["&{ORG_NAME}.&{ACF_ACCOUNT_NAME}"]
$$
PUBLISH=TRUE REVIEW=TRUE;

--unset vars
UNSET (EVENT_WH, SHARE_DB, SHARE_SCH, SHARE_TBL, SHARE_NAME);

!print **********
!print End 01_account_setup.sql
!print **********
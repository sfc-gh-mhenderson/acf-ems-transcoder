/*************************************************************************************************************
Script :            setup.sql
Create Date:        2023-07-18
Author:             Marc Henderson
Description:        This script calls the sql scripts that create the DEV environment source objects.
Called by:          SCRIPT(S):
                        dev_env/setup/setup.sh
Calls:              SCRIPTS:
                        scripts/01_dev_env_objects.sql

Used By:            Provider
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-07-18          Marc Henderson                      Initial build
*************************************************************************************************************/

!set exit_on_error=True;
!set variable_substitution=true;

--source objects:
!source  ../scripts/01_dev_env_objects.sql
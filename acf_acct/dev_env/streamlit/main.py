# Import python packages
import streamlit as st

from snowflake.snowpark.context import get_active_session
from snowflake.connector.pandas_tools import pd_writer
import snowflake.snowpark.functions as F
import snowflake.permissions as permissions
from abc import ABC, abstractmethod
import base64
import datetime
import json
import pandas as pd
import plotly.figure_factory as ff
import plotly.express as px
import plotly.graph_objects as go
import random
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import time
import utils.utils as u #remevoe utils. prefix when deploying

if "page" not in st.session_state:
    st.session_state.page = "home"

if st.session_state.page in ["run_history"]:
    st.set_page_config(layout="wide")
else:
    st.set_page_config(layout="centered")
    

def clear_session_vars():
    #navigation session vars
    if "current_step" in st.session_state:
        del st.session_state.current_step
        
    if "disable_step_2" in st.session_state:
        del st.session_state.disable_step_2
    
    if "disable_step_3" in st.session_state:
        del st.session_state.disable_step_3

    if "disable_step_4" in st.session_state:
        del st.session_state.disable_step_4
    
    #Step 1 session vars    
    if "ref" in st.session_state:
        del st.session_state.ref
        
    if "refs_selected" in st.session_state:
        del st.session_state.refs_selected
        
    if "reference_tables" in st.session_state:
        del st.session_state.reference_tables
        
    if "sb_select_transcode_obj_idx" in st.session_state:
        del st.session_state.sb_select_transcode_obj_idx
        
    if "picked_obj" in st.session_state:
        del st.session_state.picked_obj
        
    if "picked_obj_selected" in st.session_state:
        del st.session_state.picked_obj_selected
        
    if "results_table_name" in st.session_state:
        del st.session_state.results_table_name
        
    #Step 2 session vars
    if "sb_select_pid_col_idx" in st.session_state:
        del st.session_state.sb_select_pid_col_idx
        
    if "sb_select_luid_col_idx" in st.session_state:
        del st.session_state.sb_select_luid_col_idx
        
    if "master_column_list" in st.session_state:
        del st.session_state.master_column_list
            
    if "pid_column_list" in st.session_state:
        del st.session_state.pid_column_list
        
    if "luid_column_list" in st.session_state:
        del st.session_state.luid_column_list
        
    if "pid_column" in st.session_state:
        del st.session_state.pid_column
        
    if "luid_column" in st.session_state:
        del st.session_state.luid_column
        
    if "key_type" in st.session_state:
        del st.session_state.key_type
        
        
    #Step 3 session vars
    if "transcoding_partners" in st.session_state:
        del st.session_state.transcoding_partners
        
    if "master_partner_list" in st.session_state:
        del st.session_state.master_partner_list

    #clear all cached data
    st.cache_data.clear()

#setting custom width for larger st.dialogs
st.markdown(
    """
<style>
div[data-testid="stDialog"] div[role="dialog"]:has(.large-dialog) {
    width: 85%;
}

div[data-testid="stDialog"] div[role="dialog"]:has(.medium-dialog) {
    width: 65%;
}
</style>
""",
    unsafe_allow_html=True,
)

def set_form_step(action,step=None):
    if action == "Next":
        st.session_state.current_step = st.session_state.current_step + 1
    if action == "Back":
        st.session_state.current_step = st.session_state.current_step - 1
    if action == "Jump":
        st.session_state.current_step = step
        

def input_callback(wizard, session_key, input_key):
    st.session_state[session_key] = st.session_state[input_key]
    
    #enable steps
    if wizard.lower() == "transcode":
        if st.session_state.current_step == 1:
            if input_key.lower() == "txt_results_table_name":
                if st.session_state.results_table_name == "":
                    st.session_state.disable_step_2 = True
                else:
                    st.session_state.disable_step_2 = False
        if st.session_state.current_step == 2:
            st.session_state.disable_step_3 = False
        if st.session_state.current_step == 3:
            st.session_state.disable_step_4 = False


def selectbox_callback(wizard, val, idx, sb_list):
    if st.session_state[val] in sb_list:
        st.session_state[idx] = sb_list.index(st.session_state[val])
        
        if val == "sb_select_transcode_obj" and st.session_state[val] != "(None selected)":
            st.session_state.picked_obj_selected = True
        
        if val == "sb_select_pid_col":
            luid_symmetric_set_diff = set(st.session_state.master_column_list) ^ {st.session_state[val]} #master list minus selected pid col
            st.session_state.luid_column_list = [o for o in st.session_state.master_column_list if o in luid_symmetric_set_diff] #maintains list order
            
            #if "N/A" is not in luid_symmetric_set_diff list, add it back to the beginning
            if "N/A" not in st.session_state.luid_column_list:
                st.session_state.luid_column_list = ["N/A"] + st.session_state.luid_column_list
            
            st.session_state.sb_select_luid_col_idx = st.session_state.luid_column_list.index(st.session_state.luid_column)
            
            if st.session_state[val] == "N/A" and st.session_state.luid_column == "N/A":
                st.session_state.disable_step_3 = True
                
            if st.session_state[val] != "N/A" or st.session_state.luid_column != "N/A":
                st.session_state.disable_step_3 = False

        if val == "sb_select_luid_col":
            pid_symmetric_set_diff = set(st.session_state.master_column_list) ^ {st.session_state[val]} #master list minus selected luid col
            st.session_state.pid_column_list = [o for o in st.session_state.master_column_list if o in pid_symmetric_set_diff] #maintains list order  
            
            #if "N/A" is not in pid_symmetric_set_diff list, add it back to the beginning
            if "N/A" not in st.session_state.pid_column_list:
                st.session_state.pid_column_list = ["N/A"] + st.session_state.pid_column_list

            st.session_state.sb_select_pid_col_idx = st.session_state.pid_column_list.index(st.session_state.pid_column)
            
            if st.session_state[val] == "N/A" and st.session_state.pid_column == "N/A":
                st.session_state.disable_step_3 = True
                
            if st.session_state[val] != "N/A" or st.session_state.pid_column != "N/A":
                st.session_state.disable_step_3 = False

    else:
       st.session_state[idx] = 0 

    #enable steps
    if wizard.lower() == "transcode":
        if st.session_state.current_step == 1:
            if st.session_state.results_table_name != "":
                st.session_state.disable_step_2 = False
        if st.session_state.current_step == 3:
            st.session_state.disable_step_4 = False

            
def multiselect_callback(wizard, val):
    if val == "ms_transcoding_partners":
        st.session_state.transcoding_partners = st.session_state[val]
    
    #enable steps
    if wizard.lower() == "transcode":
        if st.session_state.current_step == 1:
            st.session_state.disable_step_2 = False
        if st.session_state.current_step == 2:
            st.session_state.disable_step_3 = False
        if st.session_state.current_step == 3:
            if st.session_state.transcoding_partners:
                st.session_state.disable_step_4 = False
            else:
                st.session_state.disable_step_4 = True
      

@st.dialog("Prerequisites")
def render_prereqs():
    prereqs = f"""
        - The input table or view must contain Experian PIDs and/or LUIDs.
            - The Transcoder native app will map input PIDs to the partner(s) PIDs and input LUIDs to the partner(s) LUIDS.
            - :red[**NOTE:**] The Transcoder native app **does not** perform PID-to-LUID or LUID-to-PID mapping.
        - At a minimum, the role using this native app should be granted the ```SELECT``` privilege any input table or view.
        """
    st.markdown(prereqs)
    
    
@st.dialog("Benchmarking")
def benchmarking():
    st.html("<span class='large-dialog'></span>")
    set_benchmarks = {'Input Record Volume': ['Up to 100k', 'Up to 1M', 'Up to 10M', 'Up to 100M', '100M+'],
		  		'Recommended Warehouse Size': ['MD', 'LG', 'XL','2XL', '4XL'],
				'Estimated Run Time in Min': [4,3,5,11,10],
				'Standard Warehouse Credits/Hour': [4,8,16,32,128],
				'Estimated Credits': ['0.27', '0.40', '1.33','5.87','21.33']}

    df_benchmarks = pd.DataFrame(set_benchmarks)
    
    st.write('Below is a table to help you select a warehouse based on the number of input records, along with the estimated timing and credit usage. Change your warehouse size before mapping your data.')
	
    st.markdown(df_benchmarks.style.set_table_styles([{'selector': 'th', 'props': [{'background-color', '#DFDFDF'},('font-size', '14px')]}]).set_properties(**{'color': '#000000','font-size': '14px','font-weight':'regular', 'width':'550px'}).hide(axis = "index").to_html(), unsafe_allow_html = True)


@st.dialog("App Settings")
def app_settings():
    st.html("<span class='medium-dialog'></span>")
    df_app_settings = pd.DataFrame(session.sql("SELECT key, value FROM UTIL_APP.METADATA_C_V").collect())
    
    #split df_app_settings in halves, due to amount of settings
    midpoint = int(round(len(df_app_settings) / 2))
    df_app_settings_1 = df_app_settings.iloc[:midpoint]
    df_app_settings_2 = df_app_settings.iloc[midpoint:]
    
    col1, col2 = st.columns([1,1])
    
    with col1:
        st.markdown(df_app_settings_1.style.set_properties(subset=[df_app_settings_1.columns[0]], **{'background-color': '#DFDFDF', 'color': 'black', 'font-weight': 'bold'}).set_properties(**{'color': '#000000','font-size': '14px','font-weight':'regular', 'width':'350px'}).hide(axis = "index").hide(axis = "columns").to_html(), unsafe_allow_html = True)
              
    with col2:
        st.markdown(df_app_settings_2.style.set_properties(subset=[df_app_settings_2.columns[0]], **{'background-color': '#DFDFDF', 'color': 'black', 'font-weight': 'bold'}).set_properties(**{'color': '#000000','font-size': '14px','font-weight':'regular', 'width':'350px'}).hide(axis = "index").hide(axis = "columns").to_html(), unsafe_allow_html = True)
             
             
@st.dialog("Partner Settings")
def partner_settings():
    st.html("<span class='medium-dialog'></span>")
    df_partner_settings = pd.DataFrame(session.sql("""SELECT 
                                                            PARTNER_NAME AS "Partner Name"
                                                            ,ACCESS_WINDOW AS "Access Window"
                                                            ,ACCESS_EXPIRATION_DATE AS "Access Expiration"
                                                            ,ACCESS_EXPIRED AS "Access Expired?"
                                                            ,TOTAL_REQUESTS AS "Total Requests"
                                                            ,TOTAL_RECORDS_TRANSCODED AS "Total Records Transcoded"
                                                            ,LAST_REQUEST_TIMESTAMP AS "Last Request Timestamp"
                                                        FROM UTIL_APP.PARTNERS_C_V""").collect())
    st.markdown(df_partner_settings.style.set_table_styles([{'selector': 'th', 'props': [('font-size', '14px'), ('font-weight', 'bold'), ('background-color','#DFDFDF')]}]).set_properties(**{'color': '#000000','font-size': '14px','font-weight':'regular', 'width':'350px'}).hide(axis = "index").to_html(), unsafe_allow_html = True)

             

def render_transcode_wizard():
    #navigation session vars
    if "current_step" not in st.session_state:
        st.session_state.current_step = 1
        
    if "disable_step_2" not in st.session_state:
        st.session_state.disable_step_2 = True
    
    if "disable_step_3" not in st.session_state:
        st.session_state.disable_step_3 = True

    if "disable_step_4" not in st.session_state:
        st.session_state.disable_step_4 = True
    
    #Step 1 session vars    
    if "ref" not in st.session_state:
        st.session_state.ref = ""
        
    if "refs_selected" not in st.session_state:
        st.session_state.refs_selected = {}
        
    if "reference_tables" not in st.session_state:
        st.session_state.reference_tables = {}
        
    if "sb_select_transcode_obj_idx" not in st.session_state:
        st.session_state.sb_select_transcode_obj_idx = 0
        
    if "picked_obj" not in st.session_state:
        st.session_state.picked_obj = None
        
    if "picked_obj_selected" not in st.session_state:
        st.session_state.picked_obj_selected = False
        
    if "results_table_name" not in st.session_state:
        st.session_state.results_table_name = ""
        
    #Step 2 session vars
    if "sb_select_pid_col_idx" not in st.session_state:
        st.session_state.sb_select_pid_col_idx = 0
        
    if "sb_select_luid_col_idx" not in st.session_state:
        st.session_state.sb_select_luid_col_idx = 0
        
    if "pid_column" not in st.session_state:
        st.session_state.pid_column = ""
        
    if "luid_column" not in st.session_state:
        st.session_state.luid_column = ""
        
    if "key_type" not in st.session_state:
        st.session_state.key_type = ""
        
        
    #Step 3 session vars
    if "transcoding_partners" not in st.session_state:
        st.session_state.transcoding_partners = []
        
    if "master_partner_list" not in st.session_state:
        st.session_state.master_partner_list = []
                    
    st.markdown("<h2 style='text-align: center; color: black;'>Transcode IDs</h2>", unsafe_allow_html=True)
        
    ###### Top Navigation ######
    btn_type_step1_select_obj = "primary" if st.session_state.current_step == 1 else "secondary"
    btn_type_step2_select_cols = "primary" if st.session_state.current_step == 2 else "secondary"
    btn_type_step3_select_partners = "primary" if st.session_state.current_step == 3 else "secondary"
    btn_type_step4_transcode = "primary" if st.session_state.current_step == 4 else "secondary"

    st.write("")
    st.write("")  
    step_cols = st.columns([0.5, .55, .55, .55, .55, 0.5])
    step_cols[1].button("STEP 1", on_click=set_form_step, args=["Jump", 1], type=btn_type_step1_select_obj, disabled=False)
    step_cols[2].button("STEP 2", on_click=set_form_step, args=["Jump", 2], type=btn_type_step2_select_cols, disabled=st.session_state.disable_step_2)        
    step_cols[3].button("STEP 3", on_click=set_form_step, args=["Jump", 3], type=btn_type_step3_select_partners, disabled=st.session_state.disable_step_3) 
    step_cols[4].button("STEP 4", on_click=set_form_step, args=["Jump", 4], type=btn_type_step4_transcode, disabled=st.session_state.disable_step_4)  
    
    st.write("") 
    st.write("") 
        
    ###### Step 1: Select Table or View ######
    if st.session_state.current_step == 1:        
        st.subheader("**STEP 1: Select Table or View containing PIDs/LUIDs**")
        st.write("")

        reference_association_tbl = permissions.get_reference_associations("transcode_table")
        reference_association_vw = permissions.get_reference_associations("transcode_view")

        if len(reference_association_tbl) == 0 and len(reference_association_vw) == 0 :   
            st.warning("""Please Select a Table or View""")
        else:
            if len(reference_association_tbl) > 0:   
                ref_tables = {}
                for table in reference_association_tbl:
                    sqlstring = f"""show columns in table reference('transcode_table','{table}')"""
                    table_info = pd.DataFrame(session.sql(sqlstring).collect())
                    ref_tables[table_info["table_name"].iloc[0]] = table
                    st.session_state.upload_type = 'table'
                st.session_state.reference_tables = ref_tables

            if len(reference_association_vw) > 0:   
                ref_views = {}
                for view in reference_association_vw:
                    sqlstring = f"""show columns in table reference('transcode_view','{view}')"""
                    view_info = pd.DataFrame(session.sql(sqlstring).collect())
                    ref_views[view_info["table_name"].iloc[0]] = view
                    
                    #only set the upload type to 'view' when Select View is clicked
                    if st.session_state.btn_clicked == 'view':
                        st.session_state.upload_type = 'view' 
                st.session_state.reference_views = ref_views
                
        col1, col2, col3, col4 = st.columns(4, gap="small")

        clicked_tbl = False
        clicked_vw = False

        with col2:  
            clicked_tbl = st.button("Select Table")
        with col3:
            clicked_vw = st.button("Select View")       

        if clicked_tbl:
            st.session_state.btn_clicked = 'table'
            st.session_state.upload_type = 'table'
            permissions.request_reference("transcode_table")
            
        if clicked_vw:
            st.session_state.btn_clicked = 'view'
            st.session_state.upload_type = 'view'
            permissions.request_reference("transcode_view")

        if "upload_type" in st.session_state:
            upType = st.session_state.upload_type
            if(upType == "table"):
                st.session_state.ref = 'transcode_table'
                st.session_state.refs_selected = st.session_state.reference_tables
            if(upType == "view"):
                st.session_state.ref = 'transcode_view'
                st.session_state.refs_selected = st.session_state.reference_views

            #set selectbox text and values based on selected object
            st.session_state.picked_obj = st.selectbox(f"{upType.capitalize()}"
                                                      ,["(None selected)"]+list(st.session_state.refs_selected.keys())
                                                      , key="sb_select_transcode_obj"
                                                      , on_change=selectbox_callback
                                                      , args=("transcode", "sb_select_transcode_obj", "sb_select_transcode_obj_idx", list(st.session_state.refs_selected.keys())))

            if(st.session_state.picked_obj_selected):
                st.success(f"You have chosen {upType}: **{st.session_state.picked_obj}** 🎉")
                st.write("")
                
                txt_results_table_name = st.text_input("Enter Results Table Name:"
                                           , st.session_state.results_table_name
                                           , key = "txt_results_table_name"
                                           , on_change = input_callback
                                           , args = ("transcode","results_table_name","txt_results_table_name")
                                           )
                if txt_results_table_name != "":
                    tableName = txt_results_table_name.rsplit('.', 1)[-1]

                    if session.sql(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'RESULTS_APP' AND TABLE_NAME = '{tableName}';").collect()[0][0] > 0:
                        st.warning(f"Warning: Table '{tableName}' already exists in schema 'RESULTS_APP' and will be overwritten if you continue.")
                    st.session_state.disable_step_2 = False
            
    ###### Step 2: Select PID and LUID columns ######
    if st.session_state.current_step == 2:                     
        st.subheader("**STEP 2: Select PID and/or LUID columns**")
        st.write("")
        if st.session_state.picked_obj:
            cols = pd.DataFrame(session.sql(f"show columns in table reference('{st.session_state.ref}','{st.session_state.refs_selected[st.session_state.picked_obj]}')").collect())
            
            if "master_column_list" not in st.session_state:
                st.session_state.master_column_list = ["N/A"]+cols["column_name"].to_list()
            
            if "pid_column_list" not in st.session_state:
                st.session_state.pid_column_list = ["N/A"]+cols["column_name"].to_list()
                
            if "luid_column_list" not in st.session_state:
                st.session_state.luid_column_list = ["N/A"]+cols["column_name"].to_list()
                
            if st.session_state.master_column_list:
                st.session_state.pid_column = st.selectbox("Select PID Column:"
                                       , st.session_state.pid_column_list
                                       , index = st.session_state.sb_select_pid_col_idx
                                       , key="sb_select_pid_col"
                                       , on_change=selectbox_callback
                                       , args=("transcode", "sb_select_pid_col", "sb_select_pid_col_idx", st.session_state.pid_column_list))
                
                if st.session_state.pid_column != "N/A":
                    st.success(f"PID Column: **{st.session_state.pid_column}**")
                
                st.write("")
                
                st.session_state.luid_column = st.selectbox("Select LUID Column:"
                                       , st.session_state.luid_column_list
                                       , index = st.session_state.sb_select_luid_col_idx
                                       , key="sb_select_luid_col"
                                       , on_change=selectbox_callback
                                       , args=("transcode", "sb_select_luid_col", "sb_select_luid_col_idx", st.session_state.luid_column_list))
                
                if st.session_state.luid_column != "N/A":
                    st.success(f"LUID Column: **{st.session_state.luid_column}**")
                    
                #set key type
                if st.session_state.pid_column != "N/A" and st.session_state.luid_column == "N/A":
                    st.session_state.key_type = "PID"
                
                if st.session_state.pid_column == "N/A" and st.session_state.luid_column != "N/A":
                    st.session_state.key_type = "LUID"
                    
                if st.session_state.pid_column != "N/A" and st.session_state.luid_column != "N/A":
                    st.session_state.key_type = "PID_LUID"
                
                #disable step 3 if PID/LUID column is "N/A" or empty
                if st.session_state.pid_column in ["N/A", ""] and st.session_state.luid_column in ["N/A", ""]:
                    st.session_state.disable_step_3 = True
                    
                #enable step 3
                if st.session_state.pid_column not in ["N/A", ""] or st.session_state.luid_column not in ["N/A", ""]:
                    st.session_state.disable_step_3 = False
                    
                
                

    ###### Step 3: Choose Transcoding Partner(s) ######            
    if st.session_state.current_step == 3:
        st.subheader("**STEP 3: Choose Transcoding Partner(s)**")
        st.write("")
        
        #get allowed partners
        allowed_partner_str = pd.DataFrame(session.sql("SELECT value FROM UTIL_APP.METADATA_C_V WHERE LOWER(key) = 'allowed_partners'").collect()).iloc[0,0]
        
        st.session_state.master_partner_list = allowed_partner_str.split(",")
        
        st.multiselect("Select Transcoding Partner(s):"
                        ,options=st.session_state.master_partner_list
                        ,default=st.session_state.transcoding_partners
                        ,key="ms_transcoding_partners"
                        ,on_change=multiselect_callback
                        ,args=("transcode", "ms_transcoding_partners"))
        
        partner_str = ""
        if st.session_state.transcoding_partners:
            for p in st.session_state.transcoding_partners:
                partner_str += f"\n- **{p.strip()}**"
                
            st.success(f"Partners Selected: \n{partner_str}")
            
            st.session_state.disable_step_4 = False

            #Check if requested partners are were proccessed in last 24 hours or are in progress
            partnersProcessed = session.sql(f"""SELECT upper(listagg(parse_json(MSG:message:metrics:partners)[0]:partner_name::VARCHAR, ',')) as partner_name
                            FROM APP.METRICS
                            where DATEDIFF('HOUR', TO_TIMESTAMP(MSG:message:metrics:start_timestamp::VARCHAR), SYSTIMESTAMP()) <= 24;""").collect()[0][0]
            partnersProcessedList = partnersProcessed.split(',')
            upperTranscodePartnerlist = list(map(str.upper, st.session_state.transcoding_partners))
            if all(item in partnersProcessedList for item in upperTranscodePartnerlist) and len(st.session_state.transcoding_partners) > 0:
                st.warning("Note: The selected partners have recently been processed or are in process. Continuing will process the partners again.")
        
    ###### Step 4: Confirm Settings and Transcode ######
    if st.session_state.current_step == 4:
        flag_transcode = False
                
        st.subheader("**STEP 4: Confirm Settings and Transcode**")
        st.write("")
        st.write("Please confirm the following settings before clicking Transcode.")
        st.write("")
                
        partner_str_settings = "\n".join([f"<li>{p.strip()}</li>" for p in st.session_state.transcoding_partners])
        
        #create transcode settings dataframe
        transcode_settings_data = {"Selected Input Table/View:": [st.session_state.picked_obj]
                ,"PID Columnn:": [st.session_state.pid_column]
                ,"LUID Column:": [st.session_state.luid_column]
                ,"Key Type": [st.session_state.key_type]
                ,"Transcoding Partners:": [f"<ul>{partner_str_settings}</ul>"]
                ,"Results Table:": [f"RESULTS_APP.{st.session_state.results_table_name}"]
                }
        df_transcode_settings = pd.DataFrame(transcode_settings_data).transpose()
        
        st.write("")
        
        st.markdown("<h4 style='text-align: center; color: black;'>Transcoding Settings</h4>", unsafe_allow_html=True)
        st.write("")
        st.markdown(df_transcode_settings.style.set_table_styles([{'selector': 'th', 'props': [{'background-color', '#DFDFDF'},('font-size', '16px')]}]).set_properties(**{'color': '#000000','font-size': '16px','font-weight':'regular', 'width':'550px'}).hide(axis = 1).hide(axis = 1).to_html(), unsafe_allow_html = True)
        st.write("")
        
        st.write("")
        
        col1, col2, col3, col4, col5 = st.columns([1,1,1,1,1])

        with col3:
            btn_transcode = st.button("Transcode", type="primary", disabled=False)

        if btn_transcode:
            flag_transcode = True
            
        st.write("")

        if flag_transcode:
            #create a view of the consumer's input table, based on selected columns
            session.sql(f"CREATE OR REPLACE VIEW UTIL_APP.{st.session_state.picked_obj} AS SELECT * FROM reference('{st.session_state.ref}','{st.session_state.refs_selected[st.session_state.picked_obj]}')").collect()
            input_table = f"UTIL_APP.{st.session_state.picked_obj}"
            results_table = f"RESULTS_APP.{st.session_state.results_table_name}"
            partners_csv = ",".join(st.session_state.transcoding_partners)
                
            with st.spinner("Transcoding..."):
                call_request = session.sql(f"CALL PROCS_APP.REQUEST(OBJECT_CONSTRUCT('input_table','{input_table}', 'results_table', '{results_table}', 'proc_name','transcode', 'proc_parameters',ARRAY_CONSTRUCT_COMPACT('{input_table}','{st.session_state.pid_column}','{st.session_state.luid_column}','{partners_csv}','{results_table}'))::varchar)").collect()
                result = pd.DataFrame(call_request).iloc[0]['REQUEST']
                
                if any(err in result.lower() for err in ["error", "failed"]):
                    st.error("**Transcoding Failed**", icon="🚨")
                    st.write(result)
                else :
                    st.success(f"Transcoding complete. Results table is located in {results_table} 🎉")
                    
                #drop input view
                session.sql(f"DROP VIEW IF EXISTS UTIL_APP.{st.session_state.picked_obj}").collect()

    ###### Bottom Navigation ###### 
    st.divider()
    disable_back_button = True if st.session_state.current_step == 1 else False
    disable_next_button = True if st.session_state.current_step == 4 or (st.session_state.current_step == 1 and st.session_state.disable_step_2) or (st.session_state.current_step == 2 and st.session_state.disable_step_3) or (st.session_state.current_step == 3 and st.session_state.disable_step_4) else False

    form_footer_cols = st.columns([14,1.875,1.875])

    form_footer_cols[0].button("Home", type="secondary", on_click=set_page, args=["home"])
    form_footer_cols[1].button("Back", type="secondary", on_click=set_form_step, args=["Back"], disabled=disable_back_button)
    form_footer_cols[2].button("Next", type="primary", on_click=set_form_step, args=["Next"], disabled=disable_next_button)
    
    
def render_run_history_view():
    st.markdown("<h2 style='text-align: center; color: black;'>Query Comparisons</h2>", unsafe_allow_html=True)
    st.write("")
    st.write("The table below provides details of each Transcode run")
    st.write("#")

    #create table header
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns([2.5,1.5,1,1,1,1,2,1,1,1.5,1.5,1])

    col1.markdown('<span style="font-size: 14px;">**Request ID**</span>', unsafe_allow_html=True)
    col2.markdown('<span style="font-size: 14px;">**Input Object**</span>', unsafe_allow_html=True)
    col3.markdown('<span style="font-size: 14px;">**Key Type**</span>', unsafe_allow_html=True)
    col4.markdown('<span style="font-size: 14px;">**PID Column**</span>', unsafe_allow_html=True)
    col5.markdown('<span style="font-size: 14px;">**LUID Column**</span>', unsafe_allow_html=True)
    col6.markdown('<span style="font-size: 14px;">**Partners**</span>', unsafe_allow_html=True)
    col7.markdown('<span style="font-size: 14px;">**Results Table**</span>', unsafe_allow_html=True)
    col8.markdown('<span style="font-size: 14px;">**Total Records**</span>', unsafe_allow_html=True)
    col9.markdown('<span style="font-size: 14px;">**Status**</span>', unsafe_allow_html=True)
    col10.markdown('<span style="font-size: 14px;">**Start Timestamp**</span>', unsafe_allow_html=True)
    col11.markdown('<span style="font-size: 14px;">**End Timestamp**</span>', unsafe_allow_html=True)
    col12.markdown('<span style="font-size: 14px;">**Preview**</span>', unsafe_allow_html=True)

    df_transcode_log = pd.DataFrame(session.sql(f"""SELECT
                                                        MSG:event_attributes[0]:request_id::VARCHAR request_id
                                                        ,MAX(MSG:message:metrics:input_object::VARCHAR) input_object
                                                        ,MAX(MSG:message:metrics:key_type::VARCHAR) key_type
                                                        ,MAX(MSG:message:metrics:pid_column::VARCHAR) pid_column
                                                        ,MAX(MSG:message:metrics:luid_column::VARCHAR) luid_column
                                                        ,MSG:message:metrics:partners::VARCHAR partners
                                                        ,MAX(MSG:message:metrics:results_table::VARCHAR) results_table
                                                        ,MAX(MSG:message:metrics:total_records::NUMBER(38,0)) total_records
                                                        ,CASE 
                                                            WHEN DATEDIFF('HOUR', TO_TIMESTAMP(MAX(MSG:message:metrics:start_timestamp::VARCHAR)), SYSTIMESTAMP()) >= 24 
                                                                AND MIN(MSG:status::VARCHAR) <> 'COMPLETE'
                                                            THEN 'FAILED'
                                                            ELSE MIN(MSG:status::VARCHAR)
                                                         END as status
                                                        ,LEFT(MAX(MSG:message:metrics:start_timestamp::VARCHAR), 19) start_timestamp
                                                        ,LEFT(MAX(MSG:message:metrics:end_timestamp::VARCHAR), 19) end_timestamp
                                                    FROM APP.METRICS
                                                    WHERE LOWER(MSG:entry_type) = 'metric'
                                                    AND LOWER(MSG:message:metric_type) = 'transcode_summary'
                                                    GROUP BY 1,6
                                                    ORDER BY 11 DESC;""").collect())

    for index, row in df_transcode_log.iterrows():
        request_id = str(row["REQUEST_ID"])
        input_object = row["INPUT_OBJECT"]
        key_type = str(row["KEY_TYPE"])
        pid_column = str(row["PID_COLUMN"])
        luid_column = str(row["LUID_COLUMN"])
        partners = str(row["PARTNERS"])
        results_table = str(row["RESULTS_TABLE"])
        total_records = str(row["TOTAL_RECORDS"])
        status = str(row["STATUS"])
        start_timestamp = str(row["START_TIMESTAMP"])
        end_timestamp = str(row["END_TIMESTAMP"])
        
        col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns([2.5,1.5,1,1,1,1,2,1,1,1.5,1.5,1])

        col1.markdown(f'<span style="font-size: 12px;">{request_id}</span>', unsafe_allow_html=True)
        col2.markdown(f'<span style="font-size: 12px;">{input_object}</span>', unsafe_allow_html=True)
        col3.markdown(f'<span style="font-size: 12px;">{key_type}</span>', unsafe_allow_html=True)
        col4.markdown(f'<span style="font-size: 12px;">{pid_column}</span>', unsafe_allow_html=True)
        col5.markdown(f'<span style="font-size: 12px;">{luid_column}</span>', unsafe_allow_html=True)
        
        for p_dict in json.loads(partners):
            p = p_dict["partner_name"]
            col6.markdown(f'<li style="font-size: 12px;">{p.strip()}</li>', unsafe_allow_html=True)
        
        col7.markdown(f'<span style="font-size: 12px;">{results_table}</span>', unsafe_allow_html=True)
        col8.markdown(f'<span style="font-size: 12px;">{total_records}</span>', unsafe_allow_html=True)
        col9.markdown(f'<span style="font-size: 12px;">{status}</span>', unsafe_allow_html=True)
        col10.markdown(f'<span style="font-size: 12px;">{start_timestamp}</span>', unsafe_allow_html=True)
        col11.markdown(f'<span style="font-size: 12px;">{end_timestamp}</span>', unsafe_allow_html=True)

        if col12.button("Preview", key=f"btn_{request_id}", type="primary"):
            results_preview(results_table)


    st.write("#")

    #home button
    col1, col2, col3 = st.columns([3.5,3.5,0.975])

    with col1:
        st.write("")
        st.write("")
        st.button("Home", type="secondary", on_click=set_page, args=["home"])
        
        
@st.dialog("Results Preview (up to 10 rows)")
def results_preview(results_table):
    st.html("<span class='large-dialog'></span>")
    st.markdown(f"<h4 style='text-align: center; color: black;'>Table: {results_table}</h4>", unsafe_allow_html=True)
    st.write("")
    
    df_results_table = pd.DataFrame(session.sql(f"SELECT * FROM {results_table} LIMIT 10").collect())
    
    st.markdown(df_results_table.style.set_table_styles([{'selector': 'th', 'props': [{'background-color', '#DFDFDF'},('font-size', '14px')]}]).set_properties(**{'color': '#000000','font-size': '16px','font-weight':'regular', 'width':'1100px'}).hide(axis = "index").to_html(), unsafe_allow_html = True)
    

def set_page(page: str):
    st.session_state.page = page


class Page(ABC):
    @abstractmethod
    def __init__(self):
        pass
    
    @abstractmethod
    def print_page(self):
        pass


class BasePage(Page):
    def __init__(self):
        pass
    
    def print_page(self):
        col1, col2, col3 = st.columns([0.1,2.5,0.1])

        with col2:
            u.render_image("img/Experian_logo.png")
            
            app_mode = ''
            
            try :
                app_mode = pd.DataFrame(session.sql("SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'app_mode'").collect()).iloc[0,0]
            except :
                st.rerun()
            
            st.markdown(f"<h1 style='text-align: center; color: black;'>TRANSCODER Version: {app_mode.upper()}</h1>", unsafe_allow_html=True)
            st.write("")
            st.write("The Experian Transcoder native app allows an Experian client to transcode their unique Experian PIDs and/or LUIDs to PIDs and/or LUIDs unique to one or more allowed partners. The native app generates the transcoded PIDs/LUIDs results as a cross-walk table.")
        st.divider()


class home(BasePage):
    def __init__(self):
        self.name="home"
        
    def print_page(self):
        super().print_page()
        
        app_mode = ''
        app_name = ''
        account_locator = ''
        consumer_name = ''
        app_key_metadata = ''
        app_key_local = ''
        
        app_name = pd.DataFrame(session.sql("SELECT CURRENT_DATABASE()").collect()).iloc[0,0]
        df_metadata_c_v = pd.DataFrame(session.sql(f"SELECT account_locator FROM {app_name}.UTIL_APP.METADATA_C_V").collect())
        
        if not df_metadata_c_v.empty:
            try :
                app_mode = pd.DataFrame(session.sql("SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'app_mode'").collect()).iloc[0,0]
                account_locator = pd.DataFrame(session.sql(f"SELECT account_locator FROM {app_name}.UTIL_APP.METADATA_C_V LIMIT 1").collect()).iloc[0,0]
                consumer_name =  pd.DataFrame(session.sql(f"SELECT consumer_name FROM {app_name}.UTIL_APP.METADATA_C_V LIMIT 1;").collect()).iloc[0,0]
                app_key_metadata= pd.DataFrame(session.sql(f"SELECT LOWER(value) FROM {app_name}.METADATA.METADATA_V WHERE LOWER(key) = 'app_key' AND LOWER(account_locator) = LOWER('{account_locator}') AND LOWER(consumer_name) = LOWER('{consumer_name}')").collect()).iloc[0,0]   
                app_key_local= pd.DataFrame(session.sql(f"SELECT LOWER(app_key) FROM {app_name}.APP.APP_KEY").collect()).iloc[0,0] 
            except :
                st.rerun()
                
            enable_check  = pd.DataFrame(session.sql(f"SELECT value FROM {app_name}.UTIL_APP.METADATA_C_V WHERE LOWER(key) = 'enabled'").collect()).iloc[0,0]
            trust_center_enforcement = pd.DataFrame(session.sql(f"SELECT value FROM {app_name}.METADATA.METADATA_V WHERE LOWER(key) = 'trust_center_enforcement'").collect()).iloc[0,0]
            events_shared = pd.DataFrame(session.sql(f"SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'events_shared'").collect()).iloc[0,0]
            tracker_configured = pd.DataFrame(session.sql(f"SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'tracker_configured'").collect()).iloc[0,0]
            trust_center_access = pd.DataFrame(session.sql(f"SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'trust_center_access'").collect()).iloc[0,0]
            
            if events_shared.lower() == 'n' or tracker_configured.lower() == 'n':
                st.error('Please complete installation by running the setup script found in the ⓘ icon in the top right of the page.', icon="🚨")
            elif trust_center_enforcement.lower() == 'y' and trust_center_access.lower() == 'n':
                st.error('Trust Center Access has not been granted. Please ensure the appropriate grants were executed in the setup script found in the ⓘ icon in the top right of the page.', icon="🚨")
            elif (app_key_metadata == '') or (app_key_metadata != app_key_local):
                st.warning("Please wait a moment while we finish onboarding your account. This automated process may take a few minutes. If you continue to have issues, please contact us immediately.")
                st.button('Contact Us', key='contact_1', type="primary", on_click=set_page,args=("contact",))
            elif len(enable_check) > 0:
                if enable_check.lower()  == 'y':
                    #clear session_state when at home screen
                    clear_session_vars()
        
                    col1, col2, col3, col4 = st.columns([1.075,0.85,0.85,1.075])

                    with col2:
                        st.write("")
                        if st.button("Prerequisites", type="primary"):
                            render_prereqs()
                            
                    with col3:
                        st.write("")
                        if st.button("Benchmarking", type="primary"):
                            benchmarking()

                    st.divider()
        
                    col1, col2 = st.columns(2, gap="small")
                    with col1:
                        st.markdown("<h3 style='text-align: center; color: black;'>Transcode</h3>", unsafe_allow_html=True)
                        cq_col1, cq_col2, cq_col3 = st.columns([0.3,0.75,0.25], gap="small")
                        with cq_col2:  
                            u.render_image_menu("img/ra_table.png")
                        st.markdown("""
                                        Choose table or view that contains your Experian PIDs/LUIDs to transcode. 
                                        """)
                        st.write("")

                        cq_col1, cq_col2, cq_col3 = st.columns([0.4,0.75,0.25], gap="small")
                        with cq_col2: 
                            st.button("Transcode", type="primary", on_click=set_page,args=("transcode",), key="btn_transcode") 
                    
                    with col2:
                        st.markdown("<h3 style='text-align: center; color: black;'>Run History</h3>", unsafe_allow_html=True)
                        qc_col1, qc_col2, qc_col3 = st.columns([0.3,0.75,0.25], gap="small")
                        with qc_col2:  
                            u.render_image_menu("img/analytics.png")
                        st.markdown("""
                                        View details of transcoding run history, including results table preview. 
                                        """)
                        st.write("") 

                        qc_col1, qc_col2, qc_col3 = st.columns([0.4,0.75,0.25], gap="small")
                        with qc_col2:
                                st.button("Run History", type="primary", on_click=set_page,args=("run_history",), key="btn_run_history")
                            
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    
                    col1, col2 = st.columns(2, gap="small")
                    with col1:
                        st.markdown("<h3 style='text-align: center; color: black;'>App Settings</h3>", unsafe_allow_html=True)
                        cq_col1, cq_col2, cq_col3 = st.columns([0.275,0.75,0.25], gap="small")
                        with cq_col2:  
                            u.render_image_menu("img/services.png")
                        st.markdown("""
                                        View app settings, including number or requests, records processed, etc. 
                                        """)
                        st.write("")

                        cq_col1, cq_col2, cq_col3 = st.columns([0.35,0.75,0.25], gap="small")
                        with cq_col2: 
                            if st.button("App Settings", type="primary"):
                                app_settings()
                            
                    with col2:
                        st.markdown("<h3 style='text-align: center; color: black;'>Partner Settings</h3>", unsafe_allow_html=True)
                        cq_col1, cq_col2, cq_col3 = st.columns([0.25,0.75,0.25], gap="small")
                        with cq_col2:  
                            u.render_image_menu("img/users.png")
                        st.markdown("""
                                        View allowed transcoding partners, usage expiration, etc. 
                                        """)
                        st.write("")

                        cq_col1, cq_col2, cq_col3 = st.columns([0.25,0.75,0.25], gap="small")
                        with cq_col2: 
                            if st.button("Partner Settings", type="primary"):
                                partner_settings()  
                else:
                    st.warning("Your app usage has been disabled. Please contact us if you would like to re-enable your app usage.")
                    st.button('Contact Us', key='contact_2', type="primary", on_click=set_page,args=("contact",))
            else:
                st.warning("Please wait a moment while we finish onboarding your account. This automated process may take a few minutes. If you continue to have issues, please contact us immediately.")
                st.button('Contact Us', key='contact_3', type="primary", on_click=set_page,args=("contact",))         
        else:
            st.error('This account has not been onboarded. Please consult Experian to get enabled to use this app.', icon="🚨")


########################################################################### Transcode

class create_transcode_page(BasePage):
    def __init__(self):
        self.name="transcode"
        
    def print_page(self):
        super().print_page()

        #render Transcode wizard
        render_transcode_wizard()

########################################################################### Run History

class create_run_hisory_page(BasePage):
    def __init__(self):
        self.name="run_history"
        
    def print_page(self):
        super().print_page()

        #render Run History Page
        render_run_history_view()

############################################################################## Main ####################################################################################################

pages = [home(), create_transcode_page(), create_run_hisory_page()]

session = get_active_session()

def main():
    for page in pages:
        if page.name == st.session_state.page:
            if page.name == "run_history":
                st.session_state.layout="wide"
            else:
                st.session_state.layout="centered"
            
            page.print_page();

main()
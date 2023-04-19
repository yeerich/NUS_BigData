import gspread
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
import datetime
import pandas as pd
import streamlit as st


def append_df_gsheets(user_input, feedback_input, feedback_rating, recommended_course_num, show_score):
    
    # Authenticate with Google
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Create a connection object.
    credentials = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    
    gc = gspread.authorize(credentials)

    # open a google sheet
    gs = gc.open_by_key("1elCcmaDeDBN5LnYmH9cz003VauWo4_2Kl7AxxXYreec")

    # current date and time
    ct = datetime.datetime.now()
    ct = ct.strftime("%d/%m/%Y %H:%M:%S")
   
    # write to dataframe
    df = pd.DataFrame({'Search_input':[user_input], 'Feedback':[feedback_input], 'User_rating':[feedback_rating], 'Recommended_course_number':[recommended_course_num], 'Show_score':[show_score], 'Timestamp':[ct]})
        
    # write to googlesheet
    df_values = df.values.tolist()
    gs.values_append('Sheet1', {'valueInputOption': 'RAW'}, {'values': df_values})
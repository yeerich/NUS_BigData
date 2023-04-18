import gspread
# from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import datetime
import pandas as pd


def append_df_gsheets(user_input, feedback_input, feedback_rating, recommended_course_num, show_score):
    
    # Authenticate with Google
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']

    credentials = Credentials.from_service_account_file('feedback/streamlit-course-recommender-331dc6768feb.json', scopes=scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    # open a google sheet
    gs = gc.open_by_key('1elCcmaDeDBN5LnYmH9cz003VauWo4_2Kl7AxxXYreec')

    # current date and time
    ct = datetime.datetime.now()
    ct = ct.strftime("%d/%m/%Y %H:%M:%S")
   
    # write to dataframe
    df = pd.DataFrame({'Search_input':[user_input], 'Feedback':[feedback_input], 'User_rating':[feedback_rating], 'Recommended_course_number':[recommended_course_num], 'Show_score':[show_score], 'Timestamp':[ct]})
        
    # write to googlesheet
    df_values = df.values.tolist()
    gs.values_append('Sheet1', {'valueInputOption': 'RAW'}, {'values': df_values})
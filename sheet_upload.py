#Let's run our data and get a statistical summary. First we have to create a new column and do some math into the new column
# Import our libraries
import pandas as pd
import numpy as np
import json
import requests
from scraper import get_current
import gspread
import pygsheets
from google.oauth2.service_account import Credentials  
from gspread_dataframe import set_with_dataframe

# URLs for 10 day and current
url_ten = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=10&LeagueID=00&Location=&MeasureType=Four%20Factors&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
url_current = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Four%20Factors&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='

#Call 10 day data
df_ten_day = get_current(url_ten)
#print(df_ten_day.head(5))

#Call current data
df_current = get_current(url_current)

#Add neccessary columns to our data sets
def calc_dev(df):
    df['Shooting Dev'] = df['EFG_PCT']-df['OPP_EFG_PCT']
    df['Rebounding Dev'] = df['OREB_PCT']-df['OPP_OREB_PCT']
    df['Turnover Dev'] = df['TM_TOV_PCT']-df['OPP_TOV_PCT']
    df['Free Throw Dev'] = df['FTA_RATE']-df['OPP_FTA_RATE']
    return df

#Apply the function to our data sets
df_current = calc_dev(df_current)
df_ten_day = calc_dev(df_ten_day)

#Print the first 5 rows of the data sets
# print(df_current.head(2))
# print(df_ten_day.head(2))

#Merge data sets
merged_df = pd.merge(
    df_current, 
    df_ten_day, 
    on='TEAM_ID', 
    suffixes=('_current', '_ten_day'))

#Calculate the difference between the 10 day and current data
merged_df["Rebounding Dev Diff"] = merged_df["Rebounding Dev_ten_day"] - merged_df["Rebounding Dev_current"]
merged_df["Shooting Dev Diff"] = merged_df["Shooting Dev_ten_day"] - merged_df["Shooting Dev_current"]
merged_df["Turnover Dev Diff"] = merged_df["Turnover Dev_ten_day"] - merged_df["Turnover Dev_current"]
merged_df["Free Throw Dev Diff"] = merged_df["Free Throw Dev_ten_day"] - merged_df["Free Throw Dev_current"]


def append_predicted_wins(merged_df):
    # Compute predicted win percentage directly into the final column
    merged_df['Predicted Wins'] = round(
        (0.5006 + 
        (merged_df['Shooting Dev_current'] * 4.6282) +
        (merged_df['Rebounding Dev_current'] * 1.5397) +
        (merged_df['Turnover Dev_current'] * -3.7446) +
        (merged_df['Free Throw Dev_current'] * 0.6154)) * 82
    )

    merged_df['Predicted Wins 10 day'] = round(
        (0.5006 + 
        (merged_df['Shooting Dev_ten_day'] * 4.6282) +
        (merged_df['Rebounding Dev_ten_day'] * 1.5397) +
        (merged_df['Turnover Dev_ten_day'] * -3.7446) +
        (merged_df['Free Throw Dev_ten_day'] * 0.6154)) * 82
    )

    return merged_df

#Apply the function to our data set
merged_df = append_predicted_wins(merged_df)



#---------------------------------------------Sheet Upload------------------------------------------------------------

gc = gspread.oauth()

sh = gc.open("NBA Four Factors")

worksheet = sh.worksheet("Sheet1")

#Clear existing data
worksheet.clear()

#Convert DataFrame to lists 
data_upload = [merged_df.columns.values.tolist()] + merged_df.values.tolist()

#Update
worksheet.update('A1', data_upload)

print("Successfully uploaded")

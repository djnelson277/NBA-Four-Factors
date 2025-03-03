import pandas as pd
import numpy as np
import json
import requests
import boto3
import io

# Import headers
headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'https://www.nba.com',
    'Referer': 'https://www.nba.com/',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'Host': 'stats.nba.com',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true'
}

# URLs for NBA stats
url_ten = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=10&LeagueID=00&Location=&MeasureType=Four%20Factors&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
url_current = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Four%20Factors&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='

# Function to fetch data from NBA API
def get_current(url):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if url == url_current:
                print("Successfully got data for current ytd!")
            if url == url_ten:
                print("Successfully got data for 10 day!")
        else:
            print(f"Failed to get data: Status code {response.status_code}")
            print("Response:", response.text)
            return None
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

    # Get the headers and rows from the first result set
    headers_ = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']

    return pd.DataFrame(rows, columns=headers_)

# Function to calculate deviations
def calc_dev(df):
    df['Shooting Dev'] = df['EFG_PCT'] - df['OPP_EFG_PCT']
    df['Rebounding Dev'] = df['OREB_PCT'] - df['OPP_OREB_PCT']
    df['Turnover Dev'] = df['TM_TOV_PCT'] - df['OPP_TOV_PCT']
    df['Free Throw Dev'] = df['FTA_RATE'] - df['OPP_FTA_RATE']
    return df

# Function to upload DataFrame to S3
def upload_to_s3(df, bucket_name, file_name):
    try:
        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload CSV to S3
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
        print(f"Successfully uploaded {file_name} to {bucket_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Lambda handler function
def lambda_handler(event, context):
    # Fetch data
    df_ten_day = get_current(url_ten)
    df_current = get_current(url_current)

    if df_ten_day is not None and df_current is not None:
        # Calculate deviations
        df_current = calc_dev(df_current)
        df_ten_day = calc_dev(df_ten_day)

        # Merge data sets
        merged_df = pd.merge(
            df_current,
            df_ten_day,
            on='TEAM_ID',
            suffixes=('_current', '_ten_day')
        )

        # Calculate differences
        merged_df["Rebounding Dev Diff"] = merged_df["Rebounding Dev_ten_day"] - merged_df["Rebounding Dev_current"]
        merged_df["Shooting Dev Diff"] = merged_df["Shooting Dev_ten_day"] - merged_df["Shooting Dev_current"]
        merged_df["Turnover Dev Diff"] = merged_df["Turnover Dev_ten_day"] - merged_df["Turnover Dev_current"]
        merged_df["Free Throw Dev Diff"] = merged_df["Free Throw Dev_ten_day"] - merged_df["Free Throw Dev_current"]

        # Upload to S3
        bucket_name = 'nbafourfactors'
        file_name = 'nba_four_factors.csv'
        upload_to_s3(merged_df, bucket_name, file_name)

        return {
            'statusCode': 200,
            'body': 'Successfully processed and uploaded data to S3'
        }
    else:
        return {
            'statusCode': 500,
            'body': 'Failed to fetch data from NBA API'
        }
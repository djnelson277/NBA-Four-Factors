# Import our libraries
import pandas as pd
import numpy as np
import json
import requests


#import headers
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

url_ten = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=10&LeagueID=00&Location=&MeasureType=Four%20Factors&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='

url_current = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Four%20Factors&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='

def get_current(url):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if url == url_current:
                print("Successfully got data for current ytd!")
            if url == url_ten:
                print("Successfully got data for 10 day!")
            #print(json.dumps(data, indent=2)[:500])    
            
        else:
            print(f"Failed to get data: Status code {response.status_code}")
            print("Response:", response.text)
    except Exception as e:
        print(f"Error occurred: {e}")
    # Get the headers and rows from the first result set
    headers_ = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']

    return pd.DataFrame(rows, columns=headers_)


# This block will only execute if the script is run directly, not when imported
if __name__ == "__main__":
    print(get_current(url_ten).head(5))

# df_ten_day = get_current(url_ten)
# print(df_ten_day.head(5))

# get_current(url_current)

# # Get the headers and rows from the first result set
# headers_ = data['resultSets'][0]['headers']
# rows = data['resultSets'][0]['rowSet']

# # Create DataFrame
# df = pd.DataFrame(rows, columns=headers)

# def get_df():
#     return df

# print(get_df())
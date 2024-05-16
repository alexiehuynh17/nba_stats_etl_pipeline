from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

from bs4 import BeautifulSoup as bs
import requests
import time

import random
from tqdm import tqdm
import pandas as pd
from dump import *
from nba_cfg import header_agent

def get_nba_stats_data():
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    params = {
        'LeagueID': '00',
        'PerMode': 'Totals',
        'Scope': 'S',
        'Season': '2023-24',
        'SeasonType': 'Regular Season',
        'StatCategory': 'PTS',
    }
    official_players_board = requests.get('https://stats.nba.com/stats/leagueLeaders', params=params, headers=headers)
    official_players_data = official_players_board.json()
    headers = official_players_data["resultSet"]["headers"]
    rows = official_players_data["resultSet"]["rowSet"]
    df_player_leader = pd.DataFrame(rows, columns=headers)
    df_player_leader.to_csv('/opt/airflow/tmps/data/nba_player_leader.csv', index=False)
    dump_data_to_bucket(df_player_leader, "nba_player_leader")

    get_player_information(df_player_leader)
    # df_player_leader.to_csv("tmps/player_leader_2.csv",index=False)


def get_nba_team_stats():
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    params = {
        'Conference': '',
        'DateFrom': '',
        'DateTo': '',
        'Division': '',
        'GameScope': '',
        'GameSegment': '',
        'Height': '',
        'ISTRound': '',
        'LastNGames': '0',
        'LeagueID': '00',
        'Location': '',
        'MeasureType': 'Base',
        'Month': '0',
        'OpponentTeamID': '0',
        'Outcome': '',
        'PORound': '0',
        'PaceAdjust': 'N',
        'PerMode': 'Totals',
        'Period': '0',
        'PlayerExperience': '',
        'PlayerPosition': '',
        'PlusMinus': 'N',
        'Rank': 'N',
        'Season': '2023-24',
        'SeasonSegment': '',
        'SeasonType': 'Regular Season',
        'ShotClockRange': '',
        'StarterBench': '',
        'TeamID': '0',
        'TwoWay': '0',
        'VsConference': '',
        'VsDivision': '',
    }

    official_team_board = requests.get('https://stats.nba.com/stats/leaguedashteamstats', params=params, headers=headers)
    official_team_data = official_team_board.json()
    headers = official_team_data["resultSets"][0]["headers"]
    rows = official_team_data["resultSets"][0]["rowSet"]
    df_team_leader = pd.DataFrame(rows, columns=headers)
    df_team_leader.to_csv('/opt/airflow/tmps/data/nba_team_leader.csv', index=False)
    dump_data_to_bucket(df_team_leader, "nba_team_leader")


def get_data(driver):
    player_raw_infor = []

    player_stats = driver.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div[2]/section/div[1]/section[2]/div/div[1]/div")
        
    player_infors_top = driver.find_elements(By.XPATH,"/html/body/div[1]/div[2]/div[2]/section/div[1]/section[2]/div/div[2]/div[2]/div")
    player_infors_bottom = driver.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div[2]/section/div[1]/section[2]/div/div[2]/div[4]/div")
    try:
        position_infor = driver.find_element(By.XPATH,"/html/body/div[1]/div[2]/div[2]/section/div[1]/section[1]/div[2]/div/div[2]/div[1]/p[1]")
        position_infor = position_infor.text
    except:
        position_infor = ""
    for idx in range(1,len(player_stats), 2):
        player_raw_infor.append(player_stats[idx].text.split("\n")[1])
    if len(player_raw_infor) < 4:
        while (len(player_raw_infor)) < 4:
            player_raw_infor.append("--")
            
    for idx in range(0,len(player_infors_top), 2):
        player_raw_infor.append(player_infors_top[idx].text.split("\n")[1])
    for idx in range(0,len(player_infors_bottom), 2):
        player_raw_infor.append(player_infors_bottom[idx].text.split("\n")[1])
    return player_raw_infor + [position_infor]


def get_player_information(df_player_leader):
    options = Options()
    user_agent = random.choice(header_agent)
    options.add_argument('--headless')
    options.add_argument(f"--user-agent={user_agent}")

    # driver = webdriver.Firefox(service=service, options=options)
    driver = webdriver.Remote("http://186.20.0.9:4444", options=options)
    all_player_data = []
    
    for idx in tqdm(range(len(df_player_leader[:]))):
        player_raw_infor = []                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
        player_id = df_player_leader.iloc[idx].iloc[0]
        try:
            driver.get(f"https://www.nba.com/stats/player/{df_player_leader.iloc[idx].iloc[0]}/traditional")
            time.sleep(7)
            player_raw_infor = get_data(driver=driver)

            if len(player_raw_infor) < 2:
                driver.get(f"https://www.nba.com/stats/player/{df_player_leader.iloc[idx].iloc[0]}")
                time.sleep(7)
                player_raw_infor = get_data(driver=driver)

            all_player_data.append([player_id] + player_raw_infor)
        except Exception as e:
            print(e)
    driver.close()

    player_infor_header = ['PLAYER_ID', 'PPG', 'RPG', 'APG', 'PIE', 'HEIGHT', 'WEIGHT', 'COUNTRY', 'LAST ATTENDED', 'AGE', 'BIRTHDATE', 'DRAFT', 'EXPERIENCE', 'POSITION']
    df_all_player = pd.DataFrame(all_player_data, columns=player_infor_header)
    df_all_player.to_csv('/opt/airflow/tmps/data/nba_player_information.csv', index=False)
    dump_data_to_bucket(df_all_player, "nba_player_information")


def get_team_box_scores_rs():
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    params = {
        'Counter': '1000',
        'DateFrom': '',
        'DateTo': '',
        'Direction': 'DESC',
        'ISTRound': '',
        'LeagueID': '00',
        'PlayerOrTeam': 'T',
        'Season': '2023-24',
        'SeasonType': 'Regular Season',
        'Sorter': 'DATE',
    }

    response = requests.get('https://stats.nba.com/stats/leaguegamelog', params=params, headers=headers)
    team_box_scores_rs = response.json()
    header_team_box_scores_rs = team_box_scores_rs["resultSets"][0]["headers"]
    data_team_box_scores_rs = team_box_scores_rs["resultSets"][0]["rowSet"]
    df_data_team_box_scores_rs = pd.DataFrame(data_team_box_scores_rs, columns=header_team_box_scores_rs)
    df_data_team_box_scores_rs.to_csv('/opt/airflow/tmps/data/team_box_scores_rs.csv', index=False)
    dump_data_to_bucket(df_data_team_box_scores_rs, "team_box_scores_rs")


def get_player_box_scores_rs():
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    params = {
        'Counter': '1000',
        'DateFrom': '',
        'DateTo': '',
        'Direction': 'DESC',
        'ISTRound': '',
        'LeagueID': '00',
        'PlayerOrTeam': 'P',
        'Season': '2023-24',
        'SeasonType': 'Regular Season',
        'Sorter': 'DATE',
    }

    response = requests.get('https://stats.nba.com/stats/leaguegamelog', params=params, headers=headers)

    player_box_scores_rs = response.json()
    header_player_box_scores_rs = player_box_scores_rs["resultSets"][0]["headers"]
    data_player_box_scores_rs = player_box_scores_rs["resultSets"][0]["rowSet"]
    df_data_player_box_scores_rs = pd.DataFrame(data_player_box_scores_rs, columns=header_player_box_scores_rs)
    df_data_player_box_scores_rs.to_csv('/opt/airflow/tmps/data/player_box_scores_rs.csv', index=False)
    dump_data_to_bucket(df_data_player_box_scores_rs, "player_box_scores_rs")
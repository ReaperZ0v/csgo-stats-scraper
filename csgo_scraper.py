from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from datetime import datetime
import pandas as pd 
import requests 
import glob
import time 
import os 

def csgo_scrape(page_range):
    for x in range(1, int(page_range)):
        site_resp = requests.get(f"https://tracker.gg/csgo/leaderboards/stats/all/default?page={x}").text 
        time.sleep(2)
        soup_object = BeautifulSoup(site_resp, "lxml")

        usernames = soup_object.find_all("td", class_="username")
        kills = soup_object.find_all("td", class_="stat")
        matches_played = soup_object.find_all("td", class_="stat collapse")

        packaged = {
            "username": [name.text.strip() for name in usernames],
            "kills": [kill.text.strip() for kill in kills][:100],
            "matches played": [match.text.strip() for match in matches_played]
        }

        data_frame = pd.DataFrame.from_dict(packaged)
        data_frame.to_csv(f"csgo_stats_page{x}.csv", index=False)

        now = datetime.now()
        print(f"[ALERT][{now}] Page {x} Data Exfiltrated to ===> scraped_jobs_page{x}.csv")

        packaged["username"].clear()
        packaged["kills"].clear()
        packaged["matches played"].clear()

if __name__ == "__main__":
    csgo_scrape(10)
    files = [i for i in glob.glob('*.csv')]

    merged_csv = pd.concat([pd.read_csv(f) for f in files])
    merged_csv.to_csv("csgo_stats_data.csv", index=False)

    for file in files:
        if file == "csgo_stats_data.csv":
            continue

        else:
            os.remove(file)

    df = pd.read_csv("csgo_stats_data.csv")
    print(f"[SUCCESS] Total Data Exfiltrated is at a metric of {len(df)} rows")







import os
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import pandas as pd

headers = {"User-Agent": "Mozilla/5.0"};

def get_player_info(id):
    url = f"https://vi.soccerwiki.org/player.php?pid={id}";
    response = requests.get(url, headers=headers);
    soup = BeautifulSoup(response.text, "lxml");
    playerElement = soup.find("div", class_="player-info-main");
    infoElements = playerElement.find_all("p", class_="player-info-subtitle mb-2");
    name = infoElements[0].find("span").next_sibling.strip();
    age = infoElements[4].find("span").next_sibling.strip();
    stats = infoElements[3].find("span",class_="promo-creative-tickets-number").get_text(strip=True);
    height = infoElements[6].find("span").next_sibling.strip();
    weight = infoElements[7].find("span").next_sibling.strip();
    squad_number = playerElement.find("span",string=re.compile(r"Squad Number")).next_sibling.strip();
    return {
        "id" : id,
        "name" : name,
        "age" : age,
        "stats" : stats,
        "height" : height,
        "weight" : weight,
        "squad_number" : squad_number
    }

def get_player_ids_in_club(id):
    url = f"https://vi.soccerwiki.org/squad.php?clubid={id}";
    response = requests.get(url, headers=headers);
    soup = BeautifulSoup(response.text, "lxml");
    table_data = soup.find("table", id= "datatable")
    player_ids = list(set([p["href"].split("pid=")[1] for p in table_data.find_all("a", href=re.compile(r"^/player\.php\?pid="))]))
    return player_ids;

def get_player_ids_in_league(id):
    url = f"https://vi.soccerwiki.org/league.php?leagueid={id}";
    player_ids = []
    player_infos = []
    response = requests.get(url, headers=headers);
    soup = BeautifulSoup(response.text, "lxml");
    clubs_table = soup.find_all("table",class_= "table-custom table-roster")[1];
    club_ids =list(set([c["href"].split("clubid=")[1] for c in clubs_table.find_all("a", href=re.compile(r"^/squad\.php\?clubid="))]))
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(get_player_ids_in_club, cid): cid for cid in club_ids}
        for f in as_completed(futures):
            player_ids.extend(f.result())
    print(len(player_ids),"players");
    with ThreadPoolExecutor(max_workers=45) as executor:
        futures = {executor.submit(get_player_info, pid): pid for pid in player_ids}
        for f in as_completed(futures):
            player_infos.append(f.result())
    return sorted(player_infos,key=lambda p: p["id"])
def save_excel(player_infos,filename):
    df = pd.DataFrame(player_infos)
    df.to_excel(filename, index=False, engine="openpyxl")
    print(f"Đã lưu {len(df)} dòng vào {filename}")
player_infos = get_player_ids_in_league(89)
save_excel(player_infos,os.path.join(os.path.dirname(__file__), "..", "data","data.xlsx"))
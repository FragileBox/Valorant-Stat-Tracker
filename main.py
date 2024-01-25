import os
import requests
import datetime
import time
import ast

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SPREADSHEET_ID = "13aTYvUE4rg2GUR3_-YczFqKMQoAIt4AhbOsKgz7jlvE"

#Getting list of usernames
infile = open("users.txt", "r")
users = [x.split(",")[0].strip() for x in infile.readlines()]
infile.close()

def pull_data(url, para):
    resp = requests.get(url=url, params=para)
    return resp.json()

def main():
    credentials = None
    if os.path.exists("token.json"):
        credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            credentials = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(credentials.to_json())

    try:
        service = build("sheets", "v4", credentials = credentials)
        sheets = service.spreadsheets()

        no_updated = 0
        updated = ""

        temp = dict()
        for username in users:
            temp[username] = dict()
            temp[username]["stored"] = 0
            temp[username]["update"] = 0
        usernames = temp

        temp = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        col = [x for x in temp]
        for start in temp:
            for end in temp:
                col.append(f"{start}{end}")
        agents = ["Astra", "Breach", "Brimstone", "Chamber", "Cypher", "Deadlock", "Fade", "Gekko", "Harbor", "Jett", "ISO", "KAY/O", "Killjoy", "Neon", "Omen", "Phoenix", "Raze", "Reyna", "Sage", "Skye", "Sova", "Viper", "Yoru"]

        #Stat Puller from API
        while no_updated < len(users):
            try:
                result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!B1:ZZ1").execute()
                stored_usernames = result.get("values", ",")[0]

                col_pos = 0
                not_in = 1
                for username in usernames:
                    col_pos += 1
                    if username in stored_usernames:
                        usernames[username]["stored"] = stored_usernames.index(username)+1
                    else:
                        usernames[username]["stored"] = len(stored_usernames) + not_in
                        not_in += 1
                    usernames[username]["update"] = col_pos

                usernames = dict(sorted(usernames.items(), key=lambda x:x[1]["stored"]))

                for username in usernames:
                    if username not in stored_usernames:
                        usernames[username]["stored"] = usernames[username]["update"]
                
                for username in usernames:
                    if updated != "" and list(usernames.keys()).index(updated) >= list(usernames.keys()).index(username):
                        no_updated += 1
                        continue
                        
                    if type(username) == list:
                        username = "#".join(username)
                    
                    get_pos = usernames[username]["stored"]
                    store_pos = usernames[username]["update"]
                    
                    print(f"Player: {username}")
                    #Username + matches to pull
                    username = username.split("#")

                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}1",
                                           valueInputOption="USER_ENTERED", body={"values": [[f"{username[0]}#{username[1]}"]]}).execute()
                    time.sleep(1)

                    #puuid & region
                    while True:
                        try:
                            pull_cont = pull_data(f"https://api.henrikdev.xyz/valorant/v1/account/{username[0]}/{username[1]}", dict())
                            puuid = pull_cont["data"]["puuid"]
                            region = pull_cont["data"]["region"]
                        except Exception as e:
                            continue
                        else:
                            break

                    #rank
                    while True:
                        try:
                            pull_cont = pull_data(f"https://api.henrikdev.xyz/valorant/v1/by-puuid/mmr/{region}/{puuid}", dict())
                            rank = pull_cont["data"]["currenttierpatched"]
                        except Exception:
                            continue
                        else:
                            break

                    #matchIDs
                    episode_start = datetime.datetime(2023,11,1) #Change this when episode changes
                    while True:
                        try:
                            pull_cont = pull_data(f"https://api.henrikdev.xyz/valorant/v1/lifetime/matches/{region}/{username[0]}/{username[1]}?mode=competitive&size=999999", dict())
                            matchIDs = []
                            for match in pull_cont["data"]:
                                date = [int(x) for x  in match["meta"]["started_at"][:10].split("-")]
                                date = datetime.datetime(date[0], date[1], date[2])
                                if date >= episode_start:
                                    matchIDs.append(match["meta"]["id"])
                        except Exception:
                            continue
                        else:
                            break
                    
                    print(f"{len(matchIDs)} matches found!")

                    #getting processed matches
                    print("Obtaining processed matches...")
                    if f"{username[0]}#{username[1]}" in stored_usernames:
                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[get_pos]}3:{col[get_pos]}999").execute()
                        values = result.get("values", ",")
                        time.sleep(1)
                        stored_matchIDs = [x for x in [x[0] for x in values] if x != ","]
                    else:
                        stored_matchIDs = []

                    head = len(stored_matchIDs)+3
                    tail = len(stored_matchIDs)+len(matchIDs)+3

                    if get_pos != store_pos and stored_matchIDs != []:
                        true_head = 3
                        for matchID in stored_matchIDs:
                            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}{true_head}",
                                                   valueInputOption="USER_ENTERED", body={"values": [[f"{matchID}"]]}).execute()
                            time.sleep(1)
                            true_head += 1
                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[get_pos]}2").execute()
                        values = result.get("values")[0][0]
                        time.sleep(1)
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}2",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{values}"]]}).execute()
                        time.sleep(1)
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}1",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{username[0]}#{username[1]}"]]}).execute()
                        time.sleep(1)
                        usernames[username]["stored"] = usernames[username]["update"]

                    for map in ["Overall", "Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset", "Agents"]:
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}1",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{username[0]}#{username[1]}"]]}).execute()
                        time.sleep(1)

                    #getting processed stats
                    if stored_matchIDs == []:
                        full_stats = {"player": f"{username[0]}#{username[1]}", "agent_pool": {}, "stats": {"Overall": {"matches": 0},"Ascent": {"matches": 0},"Bind": {"matches": 0},"Breeze": {"matches": 0},"Fracture": {"matches": 0},"Haven": {"matches": 0},"Icebox": {"matches": 0},"Lotus": {"matches": 0},"Pearl": {"matches": 0},"Split": {"matches": 0},"Sunset": {"matches": 0}}}
                    else:
                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[get_pos]}2").execute()
                        values = result.get("values")[0][0]
                        time.sleep(1)
                        full_stats = ast.literal_eval(values)
                    full_stats["stats"]["Overall"]["rank"] = rank
                    full_stats["agent_pool"]["rank"] = rank

                    print("Obtained processed matches!")
                    
                    #per match
                    latest = True
                    match_no = 1
                    for matchID in matchIDs[:]:
                        if matchID not in stored_matchIDs:
                            if latest == True:
                                for table in ["Overall", "Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset", "Agents"]:
                                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!A1",
                                                           valueInputOption="USER_ENTERED", body={"values": [["Outdated!"]]}).execute()

                            while True:
                                try:
                                    pull_cont = pull_data(f"https://api.henrikdev.xyz/valorant/v2/match/{matchID}", dict())
                                    match = pull_cont["data"]
                                except Exception:
                                    continue
                                else:
                                    break
                            
                            if full_stats["stats"]["Overall"]["matches"] != 0:
                                result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[get_pos]}2").execute()
                                values = result.get("values")[0][0]
                                full_stats = ast.literal_eval(values)

                            rounds = match["metadata"]["rounds_played"]
                            FK = 0
                            FD = 0
                            TFK = 0 #True First Kills
                            TFD = 0 #True First Deaths
                            HS = 0 #Headshot %

                            #Map
                            map = match["metadata"]["map"]
                            
                            #PlayerID & team for other stats
                            count = 0
                            for player in match["players"]["all_players"]:
                                if f'{player["puuid"]}' == puuid:
                                    player_id = count
                                    team = player["team"].lower()
                                count += 1

                            #Agent pool & winrate(agent & map)
                            agent = match["players"]["all_players"][player_id]["character"]
                            if match["teams"][team]["has_won"]:
                                win = 1
                            else:
                                win = 0
                            if not(agent in list(full_stats["agent_pool"].keys())):
                                full_stats["agent_pool"][agent] = {"played": 1, "wins": win}
                            else:
                                full_stats["agent_pool"][agent]["wins"] += win
                                full_stats["agent_pool"][agent]["played"] += 1

                            #Kills, Death, Assists & HS%
                            kills = match["players"]["all_players"][player_id]["stats"]["kills"]
                            deaths = match["players"]["all_players"][player_id]["stats"]["deaths"]
                            assists = match["players"]["all_players"][player_id]["stats"]["assists"]
                            HS = (match["players"]["all_players"][player_id]["stats"]["headshots"]/(match["players"]["all_players"][player_id]["stats"]["headshots"]+match["players"]["all_players"][player_id]["stats"]["bodyshots"]+match["players"]["all_players"][player_id]["stats"]["legshots"]))*100                  

                            #DD stats
                            dmg_dealt = match["players"]["all_players"][player_id]["damage_made"]
                            dmg_received = match["players"]["all_players"][player_id]["damage_received"]

                            #rounds stats
                            ar = 0
                            arw = 0
                            dr = 0
                            drw = 0
                            if team == "blue":
                                for round in range(len(match["rounds"])):
                                    if round <= 12:
                                        if match["rounds"][round]["winning_team"].lower() == team:
                                            drw +=1
                                        dr += 1
                                    else:
                                        if match["rounds"][round]["winning_team"].lower() == team:
                                            arw += 1
                                        ar += 1
                            else:
                                for round in range(len(match["rounds"])):
                                    if round <= 12:
                                        if match["rounds"][round]["winning_team"].lower() == team:
                                            arw +=1
                                        ar += 1
                                    else:
                                        if match["rounds"][round]["winning_team"].lower() == team:
                                            drw += 1
                                        dr += 1

                            #PlayerID for KAST
                            count = 0
                            for player in match["rounds"][0]["player_stats"]:
                                if player["player_display_name"] == f"{username[0]}#{username[1]}":
                                    player_id = count
                                count += 1

                            #KAST, FK, FD, TFK & TFD
                            impact = 0
                            for round in match["rounds"]:
                                after = 999999999999
                                traded_killer = 999999999999
                                earliest = 99999999999
                                for player in round["player_stats"]:
                                    if player["kill_events"] != []:
                                        for event in player["kill_events"]:
                                            if event["kill_time_in_round"] < earliest:
                                                if earliest != 99999999999:
                                                    after = earliest
                                                earliest = event["kill_time_in_round"]
                                                killer_start = event["killer_display_name"]
                                                victim_start = event["victim_display_name"]
                                for player in round["player_stats"]:
                                    if player["kill_events"] != []:
                                        for event in player["kill_events"]:
                                            if event["victim_display_name"] == killer_start:
                                                traded_killer = event["kill_time_in_round"]
                                if round["player_stats"][player_id]["kill_events"] != []: #Killed
                                    impact += 1
                                else:
                                    killer = None
                                    assist = False
                                    survived = True
                                    for player in round["player_stats"]:
                                        for event in player["kill_events"]:
                                            for assistant in event["assistants"]:
                                                if assistant["assistant_display_name"] == f"{username[0]}#{username[1]}": #Assist
                                                    impact += 1
                                                    assist = True
                                            if assist == True:
                                                break
                                            elif event["victim_display_name"] == f"{username[0]}#{username[1]}":
                                                survived = False
                                                death_time = event["kill_time_in_match"]
                                                break
                                        if assist == True:
                                            break
                                        elif survived == False:
                                            killer = player["player_display_name"]
                                            break
                                    if assist == False:
                                        if survived == True: #Survived
                                            impact += 1
                                        else:
                                            traded = False
                                            for player in round["player_stats"]:
                                                if len(player["kill_events"]) > 0:
                                                    for event in player["kill_events"]:
                                                        if event["victim_display_name"] == killer and (event["kill_time_in_match"]-death_time) <= 3000: #Traded
                                                            traded = True
                                                            impact += 1
                                                            break
                                                    if traded == True:
                                                        break
                                if victim_start == f"{username[0]}#{username[1]}":
                                    FD += 1
                                    if (traded_killer - earliest) <= 3000 and (after - earliest) <= 3000:
                                        TFD += 1
                                elif killer_start == f"{username[0]}#{username[1]}":
                                    FK += 1
                                    if (traded_killer - earliest) <= 3000 and (after - earliest) <= 3000:
                                        TFK += 1
                            KAST = (impact/rounds)*100

                            #MKs
                            TwoK = 0
                            ThreeK = 0
                            FourK = 0
                            FiveK = 0
                            for round in match["rounds"]:
                                num_kills = round["player_stats"][player_id]["kills"]
                                if num_kills == 2:
                                    TwoK += 1
                                elif num_kills == 3:
                                    ThreeK += 1
                                elif num_kills == 4:
                                    FourK += 1
                                elif num_kills > 5:
                                    FiveK += 1

                            #WMK
                            WMK = TwoK*0.75+ThreeK*1.75+FourK*3+FiveK*4
                            try:
                                #Clutchs & Clutch Opportunities
                                Clutch = {"vOne": 0, "vTwo": 0, "vThree": 0, "vFour": 0, "vFive": 0}
                                ClutchOpp = 0

                                team_names = []
                                for player in match["players"][team]:
                                    team_names.append(player["name"])
                                temp_team = team_names
                                temp_team.remove(username[0])
                                
                                coeff = 0
                                for round in range(len(match["rounds"])):
                                    round_deaths = dict()
                                    for player in match["rounds"][round]["player_stats"]:
                                        if player["kill_events"] != []:
                                            for event in player["kill_events"]:
                                                victim = event["victim_display_name"].split("#")[0]
                                                round_deaths[victim] = event["kill_time_in_match"]
                                    if all(elem in list(round_deaths.keys()) for elem in temp_team) and username[0] not in list(round_deaths.keys()):
                                        ClutchOpp += 1
                                        if match["rounds"][round]["winning_team"].lower() == team:
                                            remaining_time = 999999999
                                            for name in round_deaths.copy():
                                                if name in team_names:
                                                    if remaining_time > round_deaths[name]:
                                                        remaining_time = round_deaths[name]
                                                    del round_deaths[name]
                                            for name in round_deaths.copy():
                                                if round_deaths[name] < remaining_time:
                                                    del round_deaths[name]
                                            if team == "blue":
                                                if round <= 12: #Defense
                                                    coeff += 1.029
                                                else: #Attack
                                                    coeff += 0.971
                                            else:
                                                if round <= 12: #Attack
                                                    coeff += 0.971
                                                else: #Defense
                                                    coeff += 1.029
                                            if len(round_deaths) == 1:
                                                Clutch["vOne"] += 1
                                            elif len(round_deaths) == 2:
                                                Clutch["vTwo"] += 1
                                            elif len(round_deaths) == 3:
                                                Clutch["vThree"] += 1
                                            elif len(round_deaths) == 4:
                                                Clutch["vFour"] += 1
                                            elif len(round_deaths) >= 5:
                                                Clutch["vFive"] += 1

                                NCW = (Clutch["vOne"]*0.375+Clutch["vTwo"]*0.75+Clutch["vThree"]*1.75+Clutch["vFour"]*3+Clutch["vFive"]*4)*(coeff/rounds)
                            except:
                                ClutchOpp = 0
                                NCW = 0

                            stats = dict({
                                "kills": kills,
                                "deaths": deaths,
                                "assists": assists,
                                "dmg_dealt": dmg_dealt,
                                "dmg_received": dmg_received,
                                "r": rounds,
                                "ar": ar,
                                "arw": arw,
                                "dr": dr,
                                "drw": drw,
                                "KAST": KAST,
                                "FK": FK,
                                "FD": FD,
                                "TFK": TFK,
                                "TFD": TFD,
                                "HS": HS,
                                "WMK": WMK,
                                "NCW": NCW,
                                "ClutchOpp": ClutchOpp
                            })

                            #Updating the full stats Dict
                            for i in ["Overall", map]:
                                if full_stats["stats"][i]["matches"] == 0:
                                    full_stats["stats"][i]["matches"] = 1
                                    full_stats["stats"][i]["stats"] = stats.copy()
                                    full_stats["stats"][i]["wins"] = win
                                else:
                                    for j in stats:
                                        if j in list(stats.keys()):
                                            full_stats["stats"][i]["stats"][j] += stats[j]
                                        else:
                                            full_stats["stats"][i]["stats"][j] = stats[j]
                                    full_stats["stats"][i]["matches"] += 1
                                    full_stats["stats"][i]["wins"] += win
                                full_stats["stats"][i]["processed"] = False
                                full_stats["agent_pool"]["processed"] = False

                            #Updating raw Stats
                            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}2",
                                                   valueInputOption="USER_ENTERED", body={"values": [[f"{full_stats}"]]}).execute()

                            time.sleep(1)

                            #Saving MatchID
                            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}{head}",
                                                   valueInputOption="USER_ENTERED", body={"values": [[f"{matchID}"]]}).execute()

                            time.sleep(1)
                            head += 1
                            
                            latest = False
                            print(f"Match {match_no} processed!")
                        else:
                            print(f"Match {match_no} skipped!")
                            tail -= 1
                            matchIDs.remove(matchID)

                        match_no = match_no + 1

                    time.sleep(1)
                    head += 1

                    if not(latest):
                        #Updating raw Stats
                        for map in full_stats["stats"]:
                            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}2",
                                                   valueInputOption="USER_ENTERED", body={"values": [[f"{full_stats['stats'][map]}"]]}).execute()
                            time.sleep(1)

                        #Updating Agent Pools
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"Agents!{col[store_pos]}1",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{username[0]}#{username[1]}"]]}).execute()
                        time.sleep(1)

                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"Agents!{col[store_pos]}2",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{full_stats['agent_pool']}"]]}).execute()

                        time.sleep(1)

                        #Updating MatchIDs
                        for map in full_stats["stats"]:
                            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}2",
                                                   valueInputOption="USER_ENTERED", body={"values": [[f"{full_stats['stats'][map]}"]]}).execute()
                            time.sleep(1)

                    no_updated += 1
                    print("Matches Processed!\n")
                    updated = f"{username[0]}#{username[1]}"

            except KeyboardInterrupt as e:
                print(e)
                return
            except Exception as e:
                no_updated = 0
                print(e)
                print("\nError occured! Resuming execution...\n")
                time.sleep(2)
        
        #Updating real stats
        maps_done = 0
        maps = ["Overall", "Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset"]
        latest_map_done = maps[0]
        agent_done = False
        
        while maps_done < 11 and agent_done == False:
            try:
                for map in maps:
                    print(f"\nUpdating stats for {map}...")
                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!A1",
                                           valueInputOption="USER_ENTERED", body={"values": [["Updating..."]]}).execute()
                    if maps.index(map) <= maps.index(latest_map_done) and latest_map_done != maps[0]:
                        continue
                    
                    for username in usernames:
                        get_pos = usernames[username]["stored"]
                        store_pos = usernames[username]["update"]
                        print(f"Updating {username}...")
                        
                        row = 4

                        #stats
                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}2").execute()
                        values = result.get("values")[0][0]
                        stats = ast.literal_eval(values)

                        time.sleep(1)

                        if (stats["matches"] == 0 or stats["processed"] == True):
                            continue

                        if map == "Overall":
                            #rank
                            rank = stats["rank"]
                            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}3",
                                                   valueInputOption="USER_ENTERED", body={"values": [[f"{rank}"]]}).execute()
                            time.sleep(1)

                        #KDR
                        KDR = "{:.2f}".format(stats["stats"]["kills"]/stats["stats"]["deaths"])
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{KDR}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #KDA
                        KDA = "{:.2f}".format((stats["stats"]["kills"]+stats["stats"]["assists"])/stats["stats"]["deaths"])
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{KDA}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #DD
                        DD = "{:.1f}".format((stats["stats"]["dmg_dealt"]-stats["stats"]["dmg_received"])/stats["stats"]["r"])
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{DD}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #ADR
                        ADR = "{:.1f}".format(stats["stats"]["dmg_dealt"]/stats["stats"]["r"])
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{ADR}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #KAST
                        KAST = "{:.1f}".format(stats["stats"]["KAST"]/stats["matches"])
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{KAST}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #FKD
                        FKD = stats["stats"]["FK"]-stats["stats"]["FD"]
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{FKD}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #TFKD
                        TFK = stats["stats"]["TFK"]
                        TFD = stats["stats"]["TFD"]
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{TFK-TFD}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #HS
                        HS = "{:.1f}".format(stats["stats"]["HS"]/stats["matches"])
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{HS}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #WMK
                        WMK = "{:.2f}".format(stats["stats"]["WMK"]/stats["stats"]["r"])
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{WMK}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #CF
                        if stats["stats"]["ClutchOpp"] > 0:
                            CF = "{:.2f}".format(stats["stats"]["NCW"]/stats["stats"]["ClutchOpp"])
                        else:
                            CF = "n/a"
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{CF}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #winrate
                        winrate = "{:.1f}".format((stats["wins"]/stats["matches"])*100)
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{winrate}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #attackrate
                        attackrate = "{:.1f}".format((stats["stats"]["arw"]/stats["stats"]["ar"])*100)
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{attackrate}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #defenserate
                        defenserate = "{:.1f}".format((stats["stats"]["drw"]/stats["stats"]["dr"])*100)
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{defenserate}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        #rating
                        KPR = float(stats["stats"]["kills"]/stats["stats"]["r"])
                        DPR = float(stats["stats"]["deaths"]/stats["stats"]["r"])
                        APR = float(stats["stats"]["assists"]/stats["stats"]["r"])
                        FKPR = float(stats["stats"]["FK"]/stats["stats"]["r"])
                        FDPR = float(stats["stats"]["FD"]/stats["stats"]["r"])
                        rating = 1.2561*KPR - 0.1347*DPR + 0.5516*APR + 0.2488*FKPR - 0.2643*FDPR + 0.0343
                        rating = "{:.2f}".format(rating)
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}{row}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{rating}"]]}).execute()

                        row += 1
                        time.sleep(1)

                        stats["processed"] = True
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{map}!{col[store_pos]}2",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{stats}"]]}).execute()

                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}2").execute()
                        values = result.get("values")[0][0]
                        full_stats = ast.literal_eval(values)
                        full_stats["stats"][map]["processed"] = True
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}2",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{full_stats}"]]}).execute()

                    maps_done += 1
                    latest_map_done = map
                
                #agents
                print("\nUpdating stats for Agents...")
                sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"Agents!A1",
                                       valueInputOption="USER_ENTERED", body={"values": [["Updating..."]]}).execute()                
                for username in usernames:
                    print(f"Updating {username}...")
                    get_pos = usernames[username]["stored"]
                    store_pos = usernames[username]["update"]
                    result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"Agents!{col[store_pos]}2").execute()
                    values = result.get("values")[0][0]
                    stats = ast.literal_eval(values)

                    time.sleep(1)

                    if stats["processed"] == True:
                        continue

                    #agent stats
                    rank = stats["rank"]
                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"Agents!{col[store_pos]}3",
                                           valueInputOption="USER_ENTERED", body={"values": [[f"{rank}"]]}).execute()
                    time.sleep(1)
                    
                    for row in range(len(agents)):
                        if not(agents[row] in stats):
                            winrate = ""
                        else:
                            winrate = (stats[agents[row]]["wins"]/stats[agents[row]]["played"])*100
                            winrate = "{:.1f}".format(winrate)
                        sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"Agents!{col[store_pos]}{row+4}",
                                               valueInputOption="USER_ENTERED", body={"values": [[f"{winrate}"]]}).execute()
                        time.sleep(1)

                    stats["processed"] = True
                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"Agents!{col[store_pos]}2",
                                           valueInputOption="USER_ENTERED", body={"values": [[f"{stats}"]]}).execute()
                    time.sleep(1)

                    result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}2").execute()
                    values = result.get("values")[0][0]
                    full_stats = ast.literal_eval(values)
                    full_stats["agent_pool"]["processed"] = True
                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"MatchIDs!{col[store_pos]}2",
                                           valueInputOption="USER_ENTERED", body={"values": [[f"{full_stats}"]]}).execute()

                agent_done = True

            except KeyboardInterrupt as e:
                print(e)
                return
            except Exception as e:
                print(e)
                print("\nError occured! Resuming execution...\n")
                time.sleep(2)

        print("\nCleaning Statsheet...")
        for table in ["Overall", "Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset", "Agents", "MatchIDs"]:
            print(f"Cleaning {table}...")
            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!A1",
                                   valueInputOption="USER_ENTERED", body={"values": [["Cleaning..."]]}).execute()
            result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"{table}!B1:AE1").execute()
            headers = result.get("values")[0]
            time.sleep(1)

            correct = []
            for username in usernames:
                correct.append(username)

            for repeat in range(len(headers)-len(correct)):
                correct.append("")

            for i in range(len(headers)):
                if correct[i] != headers[i]:
                    i += 1
                    if table == "MatchIDs":
                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}3:{col[i]}999").execute()
                        values = result.get("values")
                        for j in range(2, len(values)+3):
                            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}{j}",
                                           valueInputOption="USER_ENTERED", body={"values": [[""]]}).execute()
                            time.sleep(1)
                    else:
                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}4:{col[i]}999").execute()
                        values = result.get("values")
                        time.sleep(1)
                        if values == None:
                            continue
                        elif table == "Overall" or "Agents":
                            for j in range(3, len(values)+4):
                                sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}{j}",
                                               valueInputOption="USER_ENTERED", body={"values": [[""]]}).execute()
                                time.sleep(1)
                        else:
                            for j in range(4, len(values)+4):
                                sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}{j}",
                                               valueInputOption="USER_ENTERED", body={"values": [[""]]}).execute()
                                time.sleep(1)
                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}1",
                                           valueInputOption="USER_ENTERED", body={"values": [[""]]}).execute()
                    time.sleep(1)
                else:
                    i += 1
                    if table != "Agents" and table != "MatchIDs":
                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}2").execute()
                        values = result.get("values")[0][0]
                        time.sleep(1)
                        stats = ast.literal_eval(values)

                        result = sheets.values().get(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}4").execute()
                        value = result.get("values")
                        time.sleep(1)
                        if stats["matches"] == 0 and value != None:
                            for j in range(3, len(values)+4):
                                    sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!{col[i]}{j}",
                                                   valueInputOption="USER_ENTERED", body={"values": [[""]]}).execute()
                                    time.sleep(1)

            sheets.values().update(spreadsheetId = SPREADSHEET_ID, range=f"{table}!A1",
                                   valueInputOption="USER_ENTERED", body={"values": [["Updated!"]]}).execute()

        print("\nProcess Done!")

    except HttpError as error:
        print(error)

if __name__ == "__main__":
    main()

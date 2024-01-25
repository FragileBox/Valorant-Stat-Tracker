from subprocess import call
import datetime
import requests

def pull_data(url, para):
    resp = requests.get(url=url, params=para)
    return resp.json()

#Getting list of usernames
infile = open("users.txt", "r")
users = [x.split(",")[0].strip() for x in infile.readlines()]
infile.close()
infile = open("users.txt", "r")
games = [int(x.split(",")[1].strip()) for x in infile.readlines()]
infile.close()

while True:
    for username in users:
        print(username)
        username = username.split("#")
        try:
            #puuid & region
            pull_cont = pull_data(f"https://api.henrikdev.xyz/valorant/v1/account/{username[0]}/{username[1]}", dict())
            puuid = pull_cont["data"]["puuid"]
            region = pull_cont["data"]["region"]

            #Start of episode
            episode_start = datetime.datetime(2023,11,1) #Change this when episode changes
            
            pull_cont = pull_data(f"https://api.henrikdev.xyz/valorant/v1/lifetime/matches/{region}/{username[0]}/{username[1]}?mode=competitive&size=999999", dict())
            matchIDs = []
            for match in pull_cont["data"]:
                date = [int(x) for x  in match["meta"]["started_at"][:10].split("-")]
                date = datetime.datetime(date[0], date[1], date[2])
                if date >= episode_start:
                    matchIDs.append(match["meta"]["id"])
            if len(matchIDs) != games[users.index("#".join(username))]:
                games[users.index("#".join(username))] += 1
                call(["python", "main.py"])
                text = ""
                for i in range(len(users)):
                    text += f"{users[i]},{games[i]}\n"
                infile = open("users.txt", "w")
                infile.write(text.strip())
                infile.close()
        except KeyboardInterrupt:
            break
        except Exception:
            continue


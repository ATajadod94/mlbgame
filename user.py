import mlbgame
import sqlite3
from multiprocessing import Pool

def add_games(day, month  ):
    return mlbgame.games(2018, month, day)

conn = sqlite3.connect('games.sqlite')
cur = conn.cursor()
games = mlbgame.games(2018, 6, 1)
league_info = mlbgame.info.league_info()
team_info = mlbgame.info.team_info()

cur.execute('''
CREATE TABLE IF NOT EXISTS TEAMS (NAME TEXT, CLUB_ID TEXT , LEAGUE TEXT, PHOTOSTORE TEXT)''')

cur.execute('''
CREATE TABLE IF NOT EXISTS GAMES (ID TEXT, HOMETEAM TEXT, AWAYTEAM TEXT , SCORE TEXT, HOMETEAM_ID TEXT, AWAYTEAM_ID TEXT)''')

for team in team_info:
    cur.execute('''INSERT INTO TEAMS (NAME, CLUB_ID, LEAGUE, PHOTOSTORE)
            VALUES ( ?, ?, ? ,? )''', (team['aws_club_slug'],team['club_id'], team['league'], team['photostore_url']))

conn.commit()

p = Pool(processes=10)
dates = ((day,month) for day in range(1,31) for month in range(4,12))
all_games = p.starmap(add_games,dates)
p.close()

for gameday in all_games:
    if len(gameday) > 0:
        for game in gameday[0]:
            try:
                cur.execute('''INSERT INTO GAMES (ID, HOMETEAM, AWAYTEAM)
                         VALUES ( ?, ?, ?  )''', (game.game_id, game.home_team, game.away_team))

                cur.execute('SELECT CLUB_ID FROM TEAMS WHERE name = ? ', (game.home_team.replace(" ", "").lower(), ))
                user_id = cur.fetchone()[0]
                cur.execute('''INSERT INTO GAMES (HOMETEAM_ID)
                    VALUES(?) ''', (user_id,))
                cur.execute('SELECT CLUB_ID FROM TEAMS WHERE name = ? ', (game.away_team.replace(" ", "").lower(), ))
                user_id = cur.fetchone()[0]
                cur.execute('''INSERT INTO GAMES (AWAYTEAM_ID)
                  VALUES(?) ''', (user_id,))
                cur.execute('''INSERT INTO GAMES (SCORE)
                         VALUES ( ? )''', (game.w_team))
            except:
                print(game.game_id)
    else:
        print(gameday)


conn.commit()




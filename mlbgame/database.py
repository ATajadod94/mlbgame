import mlbgame
import sqlite3
from multiprocessing import Pool
import difflib

#   Goal :  To write a class which enables users to create a database for all the games in a year

class database(object):


    def __init__(self, database):
        self.conn = sqlite3.connect(database)
        self.cur = self.conn.cursor()

    def teamtable(self):
        """Creates a table with the scheme '' for all the games in the given year
        """
        team_info = mlbgame.info.team_info()
        # Do some setup
        self.cur.executescript('''
        DROP TABLE IF EXISTS TEAMS;
    
        CREATE TABLE IF NOT EXISTS TEAMS (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            NAME TEXT UNIQUE,      
            PHOTOSTORE TEXT);
                ''')
        for team in team_info:
            self.cur.execute('''INSERT INTO TEAMS (NAME, PHOTOSTORE)
                        VALUES ( ?, ?)''', (team['aws_club_slug'],  team['photostore_url']))

        self.conn.commit()

    def gametable(self):

        self.cur.executescript('''
        DROP TABLE IF EXISTS GAMES;

        CREATE TABLE IF NOT EXISTS GAMES (
            id TEXT UNIQUE,
            HOME_TEAM TEXT,   
            AWAY_TEAM TEXT,    
            STATUS TEXT,
            WINNING_TEAM TEXT);
                ''')


        p = Pool(processes=10)
        dates = ((day, month) for day in range(1, 31) for month in range(4, 12))
        all_games = p.starmap(get_game, dates)
        p.close()

        team_info = mlbgame.info.team_info()
        team_ids = [x['aws_club_slug'] for x in team_info]
        for daygames in all_games:
            try:
                for game in daygames[0]:
                    away_team = difflib.get_close_matches(game.away_team, team_ids)[0]
                    home_team = difflib.get_close_matches(game.away_team, team_ids)[0]
                    status = game.game_status

                    if status == 'FINAL':
                        winning_team = difflib.get_close_matches(game.w_team, team_ids)[0]
                    else:
                        winning_team = ''

                    self.cur.execute(''' INSERT INTO GAMES (id, HOME_TEAM, AWAY_TEAM, STATUS, WINNING_TEAM)
                              VALUES ( ?, ?, ? , ? , ? )''', (game.game_id, home_team,away_team,status,winning_team))
            except:
                continue

        self.conn.commit()


def user():
    a = database('games.sqlite')
    #a.teamtable()
    a.gametable()


def get_game(day, month):
    return mlbgame.games(2018, month, day)

user()
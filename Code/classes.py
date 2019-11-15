import pandas as pd
from scipy.stats import gamma
import matplotlib.pyplot as plt
import math
import numpy as np


class League:
    def __init__(self, name, engine):
        """
        Creates instance of League class for a league.

        :param name: name of league schema
        :param engine:
        """
        valid_names = ['premier_league', 'championship', 'league_one', 'league_two']
        if name in valid_names:
            self.name = name
            connection = engine.connect()
            league_table_query = """SELECT * FROM {}.league_table
                                    ORDER BY points DESC, gd DESC, gf DESC""".format(str("`" + self.name + "`"))
            home_table_query = """SELECT * FROM {}.home_league_table
                                    ORDER BY points DESC, gd DESC, gf DESC""".format(str("`" + self.name + "`"))
            away_table_query = """SELECT * FROM {}.away_league_table
                                    ORDER BY points DESC, gd DESC, gf DESC""".format(str("`" + self.name + "`"))
            full_league_table = connection.execute(league_table_query).fetchall()
            home_league_table = connection.execute(home_table_query).fetchall()
            away_league_table = connection.execute(away_table_query).fetchall()
            column_names = ['Team', 'Played', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'P']
            self.data = pd.DataFrame(full_league_table, columns=column_names)
            self.data.set_index([pd.Index([i for i in range(1, len(full_league_table) + 1)], name='#')], inplace=True)
            connection.close()
            self.home_data = pd.DataFrame(home_league_table, columns=column_names)
            self.home_data.set_index([pd.Index([i for i in range(1, len(home_league_table) + 1)], name='#')],
                                     inplace=True)
            self.away_data = pd.DataFrame(away_league_table, columns=column_names)
            self.away_data.set_index([pd.Index([i for i in range(1, len(away_league_table) + 1)], name='#')],
                                     inplace=True)
            self.team_count = self.data.index.values.max()
            self.teams = self.team_list()
            stats_columns = ['aGF', 'aGA']
            self.__stats = pd.DataFrame([[round((self.data.GF / self.data.Played).mean(), 3),
                                          round((self.data.GA / self.data.Played).mean(), 3)]],
                                        columns=stats_columns)
            self.__home_stats = pd.DataFrame([[round((self.home_data.GF / self.home_data.Played).mean(), 3),
                                               round((self.home_data.GA / self.home_data.Played).mean(), 3)]],
                                             columns=stats_columns)
            self.__away_stats = pd.DataFrame([[round((self.away_data.GF / self.away_data.Played).mean(), 3),
                                               round((self.away_data.GA / self.away_data.Played).mean(), 3)]],
                                             columns=stats_columns)

        else:
            self.name = False
            self.data = None
            self.home_data = None
            self.away_data = None
            self.team_count = False
            self.__stats = None
            self.__home_stats = None
            self.__away_stats = None

    def get_stats(self):
        return self.__stats

    def get_home_stats(self):
        return self.__home_stats

    def get_away_stats(self):
        return self.__away_stats

    def team_list(self):
        team_dict = {'premier_league': ["AFC Bournemouth", "Arsenal", "Aston Villa", "Brighton & Hove Albion",
                                        "Burnley", "Chelsea", "Crystal Palace", "Everton", "Leicester City",
                                        "Liverpool", "Manchester City", "Manchester United", "Newcastle United",
                                        "Norwich City", "Sheffield United", "Southampton", "Tottenham Hotspur",
                                        "Watford", "West Ham United", "Wolverhampton Wanderers"],
                     'championship': ["Barnsley", "Birmingham City", "Blackburn Rovers", "Brentford", "Bristol City",
                                      "Cardiff City", "Charlton Athletic", "Derby County", "Fulham",
                                      "Huddersfield Town", "Hull City", "Leeds United", "Luton Town", "Middlesbrough",
                                      "Millwall", "Nottingham Forest", "Preston North End", "Queens Park Rangers",
                                      "Reading", "Sheffield Wednesday", "Stoke City", "Swansea City",
                                      "West Bromwich Albion", "Wigan Athletic"],
                     'league_one': ["Accrington Stanley", "AFC Wimbledon", "Blackpool", "Bolton Wanderers",
                                    "Bristol Rovers", "Burton Albion", "Coventry City", "Doncaster Rovers",
                                    "Fleetwood Town", "Gillingham", "Ipswich Town", "Lincoln City",
                                    "Milton Keynes Dons", "Oxford United", "Peterborough United", "Portsmouth",
                                    "Rochdale", "Rotherham United", "Shrewsbury Town", "Southend United", "Sunderland",
                                    "Tranmere Rovers", "Wycombe Wanderers"],
                     'league_two': ["Bradford City", "Cambridge United", "Carlisle United", "Cheltenham Town",
                                    "Colchester United", "Crawley Town", "Crewe Alexandra", "Exeter City",
                                    "Forest Green Rovers", "Grimsby Town", "Leyton Orient", "Macclesfield Town",
                                    "Mansfield Town", "Morecambe", "Newport County", "Northampton Town",
                                    "Oldham Athletic", "Plymouth Argyle", "Port Vale", "Salford City",
                                    "Scunthorpe United", "Stevenage", "Swindon Town", "Walsall"]}
        return team_dict[self.name]


class Team:
    def __init__(self, name, engine, league, venue):

        if venue in ['HOME', 'AWAY']:
            self.venue = venue
        else:
            self.venue = 'HOME'
        self.engine = engine
        self.league = league
        self.name = name
        self.__match_data = []
        self.__stats = []
        self._set_match_data()
        self.position = self._set_position()
        self.zone = self._set_zone()

    def _set_match_data(self):
        connection = self.engine.connect()
        sql_query = "SELECT * FROM {}".format(str("`" + self.name + " " + self.venue + "`"))
        data = connection.execute(sql_query).fetchall()
        connection.close()
        match_columns = ['Date', 'Opponent', 'GF', 'GA', 'TG', 'W', 'D', 'L']
        self.__match_data = pd.DataFrame(data, columns=match_columns)
        self.set_stats()

    def get_match_data(self):
        return self.__match_data

    def _set_position(self):
        if self.league.data is not None:
            return self.league.data.where(self.league.data.Team == self.name).first_valid_index()
        else:
            return False

    def _set_zone(self):
        """
        Sets the zone that an instance belongs to within their league

        :return: -1 if instance in bottom 3; +1 if instance in top 3; +2 if instance in top 6; 0 otherwise
        """
        if self.league.team_count:
            difference = self.league.team_count - self.position
            if difference <= 2:
                return -1
            elif difference >= self.league.team_count - 3:
                return 1
            elif difference >= self.league.team_count - 6:
                return 2
            else:
                return 0
        else:
            return False

    def set_stats(self):
        stats_cols = ['GF', 'GA', 'TG', 'Games', 'aGF', 'aGA', 'aTG', 'O/U', 'W', 'D', 'L', 'ATT', 'DEF', 'xGF']
        self.__stats = pd.DataFrame([[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]],
                                    columns=stats_cols)
        self.__stats.Games = len(self.__match_data.Opponent.values)
        for i in ['GF', 'GA', 'TG']:
            self.__stats[i] = self.__match_data[i].values.sum()
            self.__stats['a' + i] = round(self.__stats[i] / self.__stats.Games, 3)
        for i in ['W', 'D', 'L']:
            self.__stats[i] = self.__match_data[i].values.sum()
        self.__stats['O/U'] = (self.__match_data.TG > 2.5).sum()
        if self.venue == 'HOME':
            self.__stats.ATT = self.__stats.aGF.values[0] / self.league.get_home_stats().aGF.values[0]
            self.__stats.DEF = self.__stats.aGA.values[0] / self.league.get_home_stats().aGA.values[0]
        elif self.venue == 'AWAY':
            self.__stats.ATT = self.__stats.aGF.values[0] / self.league.get_away_stats().aGF.values[0]
            self.__stats.DEF = self.__stats.aGA.values[0] / self.league.get_away_stats().aGA.values[0]
        self.__stats.ATT = round(self.__stats.ATT, 3)
        self.__stats.DEF = round(self.__stats.DEF, 3)

    def get_stats(self):
        return self.__stats

    def expected_scored(self, opp_def):
        if self.venue == 'HOME':
            print("\n" + self.name + " xGF")
            self.__stats.xGF = round(self.__stats.ATT * opp_def * self.league.get_home_stats().aGF, 3)
        elif self.venue == 'AWAY':
            print("\n" + self.name + " xGF")
            self.__stats.xGF = round(self.__stats.ATT * opp_def * self.league.get_away_stats().aGF, 3)
        return self.__stats.xGF.values[0]

    def plot_xgf(self):
        fig, ax = plt.subplots(1, 1)
        ax.set_title(self.name + " xGF", fontsize=20)
        self._plot_helper(self.__stats.xGF, ax)

    def display_name(self):
        print("\n\t\t\t*** {0} ({1}) ***\n".format(self.name, self.venue))

    def plot_gf(self):
        fig, ax = plt.subplots(1, 1)
        ax.set_title(self.name + " GF", fontsize=20)
        self._plot_helper(self.__stats.aGF, ax)

    def plot_ga(self):
        fig, ax = plt.subplots(1, 1)
        ax.set_title(self.name + " GA", fontsize=20)
        self._plot_helper(self.__stats.aGA, ax)

    @staticmethod
    def _plot_helper(goals, ax):
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.grid(True)
        ax.set_ylabel("Probability", fontsize=14)
        ax.set_xlabel("Goals", fontsize=14)
        ax.set_xlim([0, math.ceil(goals + 5)])
        xs = np.linspace(gamma.ppf(0.0001, goals + 1), gamma.ppf(0.9999, goals + 1), 1000)
        ax.plot(xs, gamma.pdf(xs, goals + 1), 'r-', lw=2, alpha=1)
        x = np.linspace(0, math.ceil(goals) + 5, math.ceil(goals) + 6)
        ax.bar(x, [round((math.pow(goals, xi) * math.exp(-goals)) / (math.factorial(xi)), 3) for xi in x], alpha=0.75,
               align='edge')
        ax.plot([goals, goals], [0, max(gamma.pdf(xs, goals + 1))], 'k--', lw=2,
                label=r'$\mu$ = ' + str(goals.values[0]))
        ax.legend(loc='upper right', framealpha=1, shadow=True, fontsize=14)
        plt.show()

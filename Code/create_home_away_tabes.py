from db_connection import connect_to_db
from classes import Team, League
import pandas as pd

pd.options.display.max_rows = 999
pd.options.display.max_columns = 999


def main(league):
    """
    Populates home and away league tables. Uses classes League() and Team().

    :param league: league with home and away tables to be populated
    """
    engine = connect_to_db(league)
    if engine:
        league = League(league, engine)
        for team in league.teams:
            print()
            for venue in ['HOME', 'AWAY']:
                print(team, venue)
                team_instance = Team(team, engine, league, venue)
                stats = team_instance.get_stats()
                cols = ['team', 'mp', 'w', 'd', 'l', 'gf', 'ga', 'gd', 'points']
                df = pd.DataFrame([[team, stats.Games.values.sum(), stats.W.values.sum(), stats.D.values.sum(),
                                    stats.L.values.sum(), stats.GF.values.sum(), stats.GA.values.sum(),
                                    (stats.GF.values.sum() - stats.GA.values.sum()),
                                    (3 * stats.W.values.sum() + stats.D.values.sum())]],
                                  columns=cols)
                print(df)
                table_name = venue.lower() + "_league_table"
                connection = engine.connect()
                df.to_sql(con=connection, name=table_name, if_exists='append', index=False)
                connection.close()


if __name__ == "__main__":
    leagues = ['premier_league', 'championship', 'league_one', 'league_two']
    for l in leagues:
        main(l)

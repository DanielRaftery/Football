from db_connection import connect_to_db
import re
import feedparser
from sqlalchemy import exc
import pandas as pd
import numpy as np

pd.options.display.max_columns = 999


def write_to_db(team_identifier, team_df, engine):
    """
    Writes results to a team's table
    :param team_identifier: table to write to (e.g. Liverpool AWAY)
    :param team_df: pandas dataframe containing data to be written
    :param engine: sql connection engine
    :return:
    """
    try:
        connection = engine.connect()
        team_df.to_sql(str(team_identifier), con=connection, if_exists="append", index=False)
        connection.close()
    except exc.IntegrityError:
        print("Attempted duplicate entry for table", team_identifier)


def read_from_db(team_identifier, engine):
    """

    :param team_identifier: team table to read from, e.g. "Liverpool AWAY"
    :param engine: engine to use as connection
    :return: list of tuples in the form (date, opponent, goals for, goals against, total goals, win, draw, lose)
    """
    connection = engine.connect()
    sql = "SELECT * FROM {}".format(str("`" + team_identifier + "`"))
    team_info = connection.execute(sql).fetchall()
    connection.close()
    return team_info


def get_match_result(home_goals, away_goals):
    """
    Obtain match result based on score
    :param home_goals: goals scored by home team
    :param away_goals: goals scored by away team
    :return: list of integer booleans for result
    """
    home_win = home_loss = draw = away_win = away_loss = 0
    if home_goals > away_goals:
        [home_win, away_loss] = [1, 1]
    elif home_goals == away_goals:
        draw = 1
    else:
        [home_loss, away_win] = [1, 1]

    return [home_win, home_loss, draw, away_win, away_loss]


def prepare_df(data, entry):
    """
    Parse data from RSS feed into format for database
    :param data: data to be parsed
    :param entry: which result from the RSS feed to be considered
    :return: list containing data and dataframes
    """
    date = data.entries[entry].published
    home_team = data.entries[entry].title.split(" v ")[0]
    away_team = data.entries[entry].title.split(" v ")[1]
    home_team_identifier = home_team + " HOME"
    away_team_identifier = away_team + " AWAY"
    result = re.split("<.+?>", data.entries[entry].summary_detail.value)[1].replace(str(home_team + " "), "").replace(
        str(" " + away_team), "")
    home_goals = int(result.split(" - ")[0])
    away_goals = int(result.split(" - ")[1])
    total_goals = int(home_goals + away_goals)
    [home_win, home_loss, draw, away_win, away_loss] = get_match_result(home_goals, away_goals)
    column_names = ["date", "opponent", "goals_for", "goals_against", "total_goals", "win", "draw", "loss"]
    home_data = [date, away_team, home_goals, away_goals, total_goals, home_win, draw, home_loss]
    away_data = [date, home_team, away_goals, home_goals, total_goals, away_win, draw, away_loss]
    home_df = pd.DataFrame(np.array([home_data]), columns=column_names)
    away_df = pd.DataFrame(np.array([away_data]), columns=column_names)
    return [date, home_team, home_df, home_team_identifier, away_team, away_df, away_team_identifier]


def _table_write(tid, data, table, conn):
    """
    Helper function to write to league tables

    :param tid: team name
    :param data: data to be written to table
    :param table: table to be updated (home_league_table, away_league_table, league_table)
    :param conn: connection to be used
    """
    sql_query = ""
    if int(data.at[0, 'win']) == 1:
        sql_query = """
            UPDATE {0}
            SET w=w+1
            WHERE team='{1}'""".format(table, tid)
    elif int(data.at[0, 'draw']) == 1:
        sql_query = """
            UPDATE {0}
            SET d=d+1
            WHERE team='{1}'""".format(table, tid)
    elif int(data.at[0, 'loss']) == 1:
        sql_query = """
            UPDATE {0}
            SET l=l+1
            WHERE team='{1}'""".format(table, tid)
    sql = """
        UPDATE {0}
        SET mp=mp+1, gf=gf+{1}, ga=ga+{2}
        WHERE team='{3}'""".format(table, int(data.at[0, 'goals_for']), int(data.at[0, 'goals_against']), tid)
    conn.execute(sql)
    conn.execute(sql_query)


def write_to_league_table(team_id, team_data, engine):
    """
    Updates team's record in the league table, using the method _table_write()

    :param team_id: team name
    :param team_data: data to be written to table
    :param engine: database engine to connect to
    """
    connection = engine.connect()
    _table_write(team_id, team_data, 'league_table', connection)
    connection.close()


def update_home_table(team_id, team_data, engine):
    """
    Updates team's records in home league table, using the method _table_write()

    :param team_id: team name
    :param team_data: data to be written to table
    :param engine: database engine to connect to
    """
    connection = engine.connect()
    _table_write(team_id, team_data, 'home_league_table', connection)
    connection.close()


def update_away_table(team_id, team_data, engine):
    """
    Updates team's records in the away league table, using the method _table_write()

    :param team_id: team name
    :param team_data: data to be written to table
    :param engine: database engine to connect to
    """
    connection = engine.connect()
    _table_write(team_id, team_data, 'away_league_table', connection)
    connection.close()


def parse_data():
    """
    Writes data to league table and to individual team table.
    User is asked to enter which league first, then given a list of valid dates to choose from.
    :return:
    """
    url_dict = {"PL": "https://www.soccerstats247.com/CompetitionFeed.aspx?langId=1&leagueId=1204",
                "C": "https://www.soccerstats247.com/CompetitionFeed.aspx?langId=1&leagueId=1205",
                "L1": "https://www.soccerstats247.com/CompetitionFeed.aspx?langId=1&leagueId=1206",
                "L2": "https://www.soccerstats247.com/CompetitionFeed.aspx?langId=1&leagueId=1197"}

    schema_dict = {"PL": "premier_league",
                   "C": "championship",
                   "L1": "league_one",
                   "L2": "league_two"}

    league = str(input("What league to you want to add games for (PL / C / L1 / L2)? "))

    while league not in url_dict.keys():
        print("Invalid input detected.")
        league = str(input(" Please select a league (PL / C / L1 / L2). "))

    url = url_dict[league]
    schema = schema_dict[league]
    engine = connect_to_db(schema)
    data = feedparser.parse(url)
    valid_dates = []

    for i in range(len(data.entries)):
        d = data.entries[i].published
        if d not in valid_dates:
            valid_dates.append(d)

    print("\nValid dates:", [d for d in valid_dates])
    date_list = []
    requested_date = str(input("What date do you want to add games for (in MM/DD/YYYY format)? "))

    while len(date_list) < len(valid_dates):
        if requested_date.lower() == "all":
            print("Are you sure? You may double up on league data! Enter \"Y\" to continue.")
            is_sure = str(input())
            if is_sure.lower() == "y":
                date_list = valid_dates
                print("Added all valid dates!")
            break
        elif requested_date.lower() == "q":
            break
        elif requested_date not in valid_dates:
            print("No matches found for this date!")
        elif requested_date not in date_list:
            date_list.append(requested_date)
        requested_date = str(input("Add more dates (in MM/DD/YYYY format)? \"Q\" to quit. "))

    print("\nDate selection complete. Selected dates:", [d for d in date_list])
    proceed = str(input("Do you wish to continue (Y/N)? "))

    if proceed.lower() == 'y':
        if date_list:
            print("\nWriting data...")

        for i in range(len(data.entries)):
            [date, home_team, home_team_df, home_team_id, away_team, away_team_df, away_team_id] = prepare_df(data,
                                                                                                              i)
            if date in date_list:
                print("\nWriting data to...\n{}".format(home_team_df))
                write_to_db(home_team_id, home_team_df, engine)
                print("\nWriting data to...\n{}".format(away_team_df))
                write_to_db(away_team_id, away_team_df, engine)
                write_to_league_table(home_team, home_team_df, engine)
                update_home_table(home_team, home_team_df, engine)
                write_to_league_table(away_team, away_team_df, engine)
                update_away_table(away_team, away_team_df, engine)

        print("\nComplete!")
    else:
        print("\nData input aborted.")


if __name__ == "__main__":
    parse_data()

from db_connection import connect_to_db
from sqlalchemy import exc
import pandas as pd
import numpy as np


def add_to_db(df, identifier, engine):
    """
    Writes to a team's AWAY or HOME table.
    :param df: data to be written, pandas dataframe
    :param identifier: the table to be written to
    :param engine: sql engine connecting to the schema containing the table
    :return:
    """
    print("\nWriting to database...")
    try:
        connection = engine.connect()
        df.to_sql(str(identifier), con=connection, if_exists="append", index=False)
        print("Added data to table", str(identifier) + "!")
        connection.close()
    except exc.IntegrityError:
        print("Failed! Attempted duplicate entry for table", str(identifier) + ".")


def parse_date(input_date):
    """
    Ensures input date is delimited by '/' and has exactly 3 items (DD MM and YYYY).
    :param input_date: date to be parse
    :return: parsed date
    """
    index_error = True
    date = ""

    while index_error:
        try:
            correct_length = len(input_date.split('/')) == 3
            # make sure only 3 items in date (DD MM YYYY)
            while not correct_length:
                input_date = str(input("Invalid format entered! Enter a date in DD/MM/YYYY format: "))
                correct_length = len(input_date.split('/')) == 3

            date = '/'.join([input_date.split('/')[1], input_date.split('/')[0], input_date.split('/')[2]])
            index_error = False
        except IndexError:
            input_date = str(input("Invalid format entered! Enter a date in DD/MM/YYYY format: "))
            continue
    return date


def validate_date(input_date):
    """
    Checks that the entered date contains only integers as entries for DD/MM/YYYY.
    :param input_date: date in MM/DD/YYYY format
    :return: integer date in MM/DD/YYYY format
    """
    input_date = parse_date(input_date)
    value_error = True
    mm = dd = yyyy = 0

    while value_error:
        try:
            mm, dd, yyyy = map(int, input_date.split('/'))
            value_error = False
        except ValueError:
            input_date = str(input("Invalid type entered for dates! Please enter integers in DD/MM/YYYY format! "))
            input_date = parse_date(input_date)

    mm, dd, yyyy = input_date.split('/')
    mm, dd, yyyy = check_range(mm, 'MM'), check_range(dd, 'DD'), check_range(yyyy, 'YYYY')

    date = '/'.join([str(mm), str(dd), str(yyyy)])
    return date


def check_length(integer, length, date_type):
    """
    Checks if input is the correct length for the date type selected.
    :param integer: input number
    :param length: valid length for the number
    :param date_type: DD, MM or YYYY
    :return: integer of correct length
    """
    length_error = not len(integer) == length

    if len(integer) == 1 and str(integer) != "0":
        integer = "0" + str(integer)
    else:
        while length_error:
            print("Invalid", date_type, "entered.")
            try:
                integer = str(input("Enter an integer for the {}: ".format(date_type)))
                length_error = not len(integer) == length
                integer = int(integer)
            except ValueError:
                print("Invalid type entered!")
    return integer


def check_range(integer, date_type):
    """
    Checks that an input is valid for the date type selected.
    If:
        date_type == 'DD', integer must be in [1, 31]
        date_type == 'MM', integer must be in [1, 12]
        date_type == 'YYYY', integer must be in [2019, 2020]
    :param integer: input integer
    :param date_type: DD, MM or YYYY
    :return: integer in correct range
    """
    valid_range = []
    valid_length = 0

    if date_type == 'DD':
        valid_range = range(1, 32)
        valid_length = 2
    elif date_type == 'MM':
        valid_range = range(1, 13)
        valid_length = 2
    elif date_type == 'YYYY':
        valid_range = [2019, 2020]
        valid_length = 4

    integer = check_length(integer, valid_length, date_type)

    while int(integer) not in valid_range:
        print("Invalid input for input", date_type)
        integer = str(input("Enter an integer in the range " + str(valid_range) + " "))
        integer = check_length(integer, valid_length, date_type)

    return integer


def check_date(input_date):
    input_date = validate_date(input_date)
    return input_date


def get_match_data(team, league_dict, league):
    """
    Get user input for match data
    :param team: home team
    :param league_dict: home team league dictionary
    :param league: home team league
    :return: [dataframe to write to home table, home table identifier,
            dataframe to write to away table, away table identifier]
    """
    raw_date = str(input("Enter date (DD/MM/YYYY format): "))
    date = check_date(raw_date)

    opponent = str(input("Opponent? "))
    while opponent not in league_dict[league] or opponent == team:
        if opponent == team:
            print(team, "cannot play against themselves!")
        else:
            print("Invalid team selected for the league.")
        opponent = str(input("What team? "))

    team_identifier = team + " HOME"
    opponent_identifier = opponent + " AWAY"
    result = str(input("Match result (home score - away score)? "))
    home_goals, away_goals = result.split('-')[0], result.split('-')[1]
    home_win = home_loss = draw = 0
    total_goals = int(home_goals) + int(away_goals)

    if home_goals > away_goals:
        home_win = 1
    elif home_goals == away_goals:
        draw = 1
    else:
        home_loss = 1

    data = [date, opponent, home_goals, away_goals, total_goals, home_win, draw, home_loss]
    opponent_data = [date, team, away_goals, home_goals, total_goals, home_loss, draw, home_win]

    column_names = ["date", "opponent", "goals_for", "goals_against", "total_goals", "win", "draw", "loss"]
    team_df = pd.DataFrame(np.array([data]), columns=column_names)
    away_df = pd.DataFrame(np.array([opponent_data]), columns=column_names)
    pd.options.display.max_rows = 999
    pd.options.display.max_columns = 999

    print("\nYou have entered:\n",
          '/'.join([date.split('/')[1], date.split('/')[0], date.split('/')[2]]),
          team, home_goals, "-", away_goals, opponent)

    return [team_df, team_identifier, away_df, opponent_identifier]


def manual_write():
    """
    Manually input data into a team's HOME or AWAY table.
    :return:
    """
    pl_teams = ["AFC Bournemouth", "Arsenal", "Aston Villa", "Brighton & Hove Albion", "Burnley", "Chelsea",
                "Crystal Palace",
                "Everton", "Leicester City", "Liverpool", "Manchester City", "Manchester United", "Newcastle United",
                "Norwich City", "Sheffield United", "Southampton", "Tottenham Hotspur", "Watford", "West Ham United",
                "Wolverhampton Wanderers"]

    championship_teams = ["Barnsley", "Birmingham City", "Blackburn Rovers", "Brentford", "Bristol City",
                          "Cardiff City",
                          "Charlton Athletic", "Derby County", "Fulham", "Huddersfield Town", "Hull City",
                          "Leeds United",
                          "Luton Town", "Middlesbrough", "Millwall", "Nottingham Forest", "Preston North End",
                          "Queens Park Rangers", "Reading", "Sheffield Wednesday", "Stoke City", "Swansea City",
                          "West Bromwich Albion", "Wigan Athletic"]

    league_one_teams = ["Accrington Stanley", "AFC Wimbledon", "Blackpool", "Bolton Wanderers", "Bristol Rovers",
                        "Burton Albion", "Coventry City", "Doncaster Rovers", "Fleetwood Town", "Gillingham",
                        "Ipswich Town", "Lincoln City", "Milton Keyes Dons", "Oxford United", "Peterborough United",
                        "Portsmouth", "Rochdale", "Rotherham United", "Shrewsbury Town", "Southend United",
                        "Sunderland",
                        "Tranmere Rovers", "Wycombe Wanderers"]

    league_two_teams = ["Bradford City", "Cambridge United", "Carlisle United", "Cheltenham Town", "Colchester United",
                        "Crawley Town", "Crewe Alexandra", "Exeter City", "Forest Green Rovers", "Grimsby Town",
                        "Leyton Orient", "Macclesfield Town", "Mansfield Town", "Morecambe", "Newport County",
                        "Northampton Town", "Oldham Athletic", "Plymouth Argyle", "Port Vale", "Salford City",
                        "Scunthorpe United", "Stevenage", "Swindon Town", "Walsall"]

    league = str(input("What league do you want to add to (PL / C / L1 / L2)? "))

    league_dict = {'PL': pl_teams,
                   'C': championship_teams,
                   'L1': league_one_teams,
                   'L2': league_two_teams}

    schema_dict = {"PL": "premier_league",
                   "C": "championship",
                   "L1": "league_one",
                   "L2": "league_two"}

    while league not in schema_dict.keys():
        print("Invalid input detected.")
        league = str(input("Please select a league (PL / C / L1 / L2). "))

    schema = schema_dict[league]
    engine = connect_to_db(schema)

    team = str(input("Who is the home team? "))

    while team not in league_dict[league]:
        print("Invalid team selected for the league.")
        team = str(input("What team? "))

    more_teams = True

    while more_teams:
        [team_df, team_identifier, away_df, opponent_identifier] = get_match_data(team, league_dict, league)
        is_sure = str(input("Is this correct? \"Y\" to continue. "))
        if is_sure.lower() == "y":
            add_to_db(team_df, team_identifier, engine)
            add_to_db(away_df, opponent_identifier, engine)
        else:
            print("You have quit.")
        temp = str(
            input("\nAdd more entries? \"N\" to quit. \"C\" to change home team. Press any other key to continue. "))
        if temp.lower() == "n":
            more_teams = False
        elif temp.lower() == "c":
            team = str(input("Who is the home team? "))
            while team not in league_dict[league]:
                print("Invalid team selected for the league.")
                team = str(input("What team? "))
        print()


if __name__ == "__main__":
    manual_write()
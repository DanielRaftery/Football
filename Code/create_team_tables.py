from db_connection import connect_to_db
from sqlalchemy import Column, Integer, String, VARCHAR, exc
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

Base = declarative_base()
# engine = connect_to_db()

pl_teams = ["AFC Bournemouth", "Arsenal", "Aston Villa", "Brighton & Hove Albion", "Burnley", "Chelsea",
            "Crystal Palace",
            "Everton", "Leicester City", "Liverpool", "Manchester City", "Manchester United", "Newcastle United",
            "Norwich City", "Sheffield United", "Southampton", "Tottenham Hotspur", "Watford", "West Ham United",
            "Wolverhampton Wanderers"]

championship_teams = ["Barnsley", "Birmingham City", "Blackburn Rovers", "Brentford", "Bristol City", "Cardiff City",
                      "Charlton Athletic", "Derby County", "Fulham", "Huddersfield Town", "Hull City", "Leeds United",
                      "Luton Town", "Middlesbrough", "Millwall", "Nottingham Forest", "Preston North End",
                      "Queens Park Rangers", "Reading", "Sheffield Wednesday", "Stoke City", "Swansea City",
                      "West Bromwich Albion", "Wigan Athletic"]

league_one_teams = ["Accrington Stanley", "AFC Wimbledon", "Blackpool", "Bolton Wanderers", "Bristol Rovers",
                    "Burton Albion", "Coventry City", "Doncaster Rovers", "Fleetwood Town", "Gillingham",
                    "Ipswich Town", "Lincoln City", "Milton Keynes Dons", "Oxford United", "Peterborough United",
                    "Portsmouth", "Rochdale", "Rotherham United", "Shrewsbury Town", "Southend United", "Sunderland",
                    "Tranmere Rovers", "Wycombe Wanderers"]

league_two_teams = ["Bradford City", "Cambridge United", "Carlisle United", "Cheltenham Town", "Colchester United",
                    "Crawley Town", "Crewe Alexandra", "Exeter City", "Forest Green Rovers", "Grimsby Town",
                    "Leyton Orient", "Macclesfield Town", "Mansfield Town", "Morecambe", "Newport County",
                    "Northampton Town", "Oldham Athletic", "Plymouth Argyle", "Port Vale", "Salford City",
                    "Scunthorpe United", "Stevenage", "Swindon Town", "Walsall"]


def table_generator(team, conn):
    """
    Creates two tables for each team - an AWAY table and a HOME table
    :param team: the team for which tables are to be created (string)
    :param conn: database connection
    :return: tables
    """

    class HomeTable(Base):
        __tablename__ = str(team + " HOME")
        date = Column(VARCHAR(45), primary_key=True)
        opponent = Column(VARCHAR(45))
        goals_for = Column(Integer)
        goals_against = Column(Integer)
        total_goals = Column(Integer)
        win = Column(Integer)
        draw = Column(Integer)
        loss = Column(Integer)

    class AwayTable(Base):
        __tablename__ = str(team + " AWAY")
        date = Column(VARCHAR(45), primary_key=True)
        opponent = Column(VARCHAR(45))
        goals_for = Column(Integer)
        goals_against = Column(Integer)
        total_goals = Column(Integer)
        win = Column(Integer)
        draw = Column(Integer)
        loss = Column(Integer)

    Base.metadata.create_all(bind=conn)


def generator(league, team_array):
    """
    Generates the tables for each team in a given league using TableGenerator
    :param league: name of schema to create (string)
    :param team_array: array of teams to create tables for inside the schema
    :return: schema in database with empty tables
    """
    engine = connect_to_db(str(league))
    connection = engine.connect()
    for team in team_array:
        # table_generator(team, connection)
        sql = """
            INSERT INTO league_table (team)
            VALUES ({})""".format("`" + str(team) + "`")
        connection.execute(sql)
    connection.close()


# generator("championship", championship_teams)
# generator("league_one", league_one_teams)
# generator("league_two", league_two_teams)

def dataframe_generator(league, teams):
    engine = connect_to_db(str(league))
    connection = engine.connect()
    data = []
    for team in teams:
        if team == 'Bolton Wanderers':
            # Bolton have a -12 point deduction for the 2019/2020 season
            data.append([team, 0, 0, 0, 0, 0, 0, 0, -12])
        else:
            data.append([team, 0, 0, 0, 0, 0, 0, 0, 0])
    dataframe = pd.DataFrame(data, columns=['team', 'mp', 'w', 'd', 'l', 'gf', 'ga', 'gd', 'points'])
    try:
        dataframe.to_sql(str("league_table"), con=connection, if_exists="append", index=False)
        connection.close()
    except exc.IntegrityError:
        print("Attempted duplicate entry.")
        connection.close()

# dataframe_generator("championship", championship_teams)
# dataframe_generator("league_one", league_one_teams)
# dataframe_generator("league_two", league_two_teams)
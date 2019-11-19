import pandas as pd
from db_connection import connect_to_db
import requests
from bs4 import BeautifulSoup, SoupStrainer
from classes import League, Team


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


def get_result_prob(home_team, away_team):
    team_xgf = home_team.xgf()
    opp_xgf = away_team.xgf()
    home_wins = {}
    draw = {}
    away_wins = {}
    prob_under = 0

    for i, probability_i in enumerate(team_xgf):
        for j, probability_j in enumerate(opp_xgf):
            result_probability = round(probability_i * probability_j, 5)
            if i + j < 3:
                prob_under += result_probability
            # print("Result {}-{}: {}".format(i, j, result_probability))
            if i < j:
                away_wins["{}-{}".format(i, j)] = result_probability
            elif i == j:
                draw["{}-{}".format(i, j)] = result_probability
            elif i > j:
                home_wins["{}-{}".format(i, j)] = result_probability

    # for key, item in home_wins.items():
    #     print("{}: {}".format(key, item))
    # print("\n{} - {}".format(home_team.name, away_team.name))
    # print("Home win: {}%".format(round(100 * sum(home_wins.values()), 5)))
    # print("Draw: {}%".format(round(100 * sum(draw.values()), 5)))
    # print("Away win: {}%".format(round(100 * sum(away_wins.values()), 5)))
    # print("Probability O2.5 goals: {}%".format(round(100 * (1 - prob_under), 5)))
    results_dict = {"home win": round(100 * sum(home_wins.values()), 5),
                    "draw": round(100 * sum(draw.values()), 5),
                    "away win": round(100 * sum(away_wins.values()), 5),
                    "over": round(100 * (1 - prob_under), 5)}
    return results_dict


def prob_predictions(lid, tid, tid_venue, oid, oid_venue):
    if tid_venue == oid_venue:
        raise ValueError("Team venue and opponent venue cannot be the same!")
    engine = connect_to_db(lid)
    if engine:
        league = League(lid, engine)
        team = Team(tid, engine, league, tid_venue)
        opponent = Team(oid, engine, league, oid_venue)
        team.set_opposition(opponent)
        opponent.set_opposition(team)
        # team.plot_xgf()
        # opponent.plot_xgf()
        results_dict = get_result_prob(team, opponent)
        return results_dict


def read_fixtures(url, date):
    pl_url = "https://ie.soccerway.com/national/england/premier-league/20192020/regular-season/r53145/matches/"
    championship_url = "https://ie.soccerway.com/national/england/championship/20192020/regular-season/r53782/matches/"
    league_one_url = "https://ie.soccerway.com/national/england/league-one/20192020/regular-season/r53677/matches/"
    league_two_url = "https://ie.soccerway.com/national/england/league-two/20192020/regular-season/r53874/matches/"

    url_dict = {"premier_league": pl_url,
                "championship": championship_url,
                "league_one": league_one_url,
                "league_two": league_two_url}

    page = requests.get(url_dict[url]).text
    only_td = SoupStrainer("td")
    soup = BeautifulSoup(page, 'html.parser', parse_only=only_td)
    dates = soup.find_all(class_='date no-repetition')
    input_date = date
    match_data = []
    for d in dates:
        if input_date == d.text:
            score_time_status = d.find_next("td", class_="score-time status").find_next("a")
            # don't read postponed games
            if not score_time_status.text.strip() == "-":
                home_team = d.find_next("td", class_="team team-a").find_next("a").get('title')
                away_team = d.find_next("td", class_="team team-b").find_next("a").get('title')
                # print(d.text, d.find_next("td", class_="team team-a").find_next("a").get('title'), "-",
                #       d.find_next("td", class_="team team-b").find_next("a").get('title'))
                results_dict = prob_predictions(url, home_team, 'HOME', away_team, 'AWAY')
                match_data.append([home_team, away_team, results_dict])
    return match_data


def match_predict(home_identifier, away_identifier, league_name):
    """
    Predict goals scored in match using Poisson distribution based on historical data for current season

    :param home_identifier: home team
    :param away_identifier: away team
    :param league_name: league containing home and away team
    :return: predicted goals in match
    """
    print()
    engine = connect_to_db(str(league_name))

    league = League(league_name, engine)
    home_team = " ".join(home_identifier.split(" ")[:-1])
    away_team = " ".join(away_identifier.split(" ")[:-1])

    home = Team(home_team, engine, league, 'HOME')
    away = Team(away_team, engine, league, 'AWAY')

    home.plot_gf()
    home.plot_ga()
    print(home.get_stats())
    away.plot_gf()
    away.plot_ga()
    print(away.get_stats())
    # home_info = read_from_db(str(home_identifier), engine)
    # away_info = read_from_db(str(away_identifier), engine)
    # column_names = ['Team', 'GF', 'GA', 'TG', 'Games', 'aGF', 'aGA', 'aTG', 'o2.5']
    #
    # match_df = pd.DataFrame([[' '.join(home_identifier.split(' ')[:-1]), 0, 0, 0, 0, 0, 0, 0, 0],
    #                          [' '.join(away_identifier.split(' ')[:-1]), 0, 0, 0, 0, 0, 0, 0, 0]],
    #                         columns=column_names)
    #
    # for i in home_info:
    #     for j in range(1, 4):
    #         if j == 3 and i[j + 1] > 2.5:
    #             match_df.iloc[0, 8] += 1
    #         match_df.iloc[0, j] += i[j + 1]
    #     match_df.iloc[0, 4] += 1
    #
    # for i in away_info:
    #     for j in range(1, 4):
    #         if j == 3 and i[j + 1] > 2.5:
    #             match_df.iloc[1, 8] += 1
    #         match_df.iloc[1, j] += i[j + 1]
    #     match_df.iloc[1, 4] += 1
    #
    # match_df.aGF = match_df.GF / match_df.Games
    # match_df.aGA = match_df.GA / match_df.Games
    # match_df.aTG = match_df.TG / match_df.Games
    #
    # average_total = match_df.aTG.sum() / 2
    #
    # [c1, c2] = match_df.aTG / match_df.Games > 0.5
    # goals = (average_total + match_df.iloc[0, 5]) / 2
    #
    # if c1:
    #     goals += 0.5
    # if c2:
    #     goals += 0.5
    #
    # if goals > 1.9:
    #     bet = "OVER"
    # else:
    #     bet = "UNDER"
    #
    # home = Team(" ".join(home_identifier.split(" ")[:-1]), match_df.iloc[0, :])
    # print(home.data)
    # print("\n *** Details for {} - {} ***".format(' '.join(home_identifier.split(' ')[:-1]),
    #                                               ' '.join(away_identifier.split(' ')[:-1])))
    # print(match_df)
    # print("\nPredicted result: {} {} - {} {}".format(' '.join(home_identifier.split(' ')[:-1]),
    #                                                  math.ceil((match_df.iloc[0, 5] + match_df.iloc[1, 6]) / 2),
    #                                                  ' '.join(away_identifier.split(' ')[:-1]),
    #                                                  math.ceil((match_df.iloc[1, 5] + match_df.iloc[0, 6]) / 2)))
    # print("Predicted match goals: {} ({})".format(math.ceil(goals), goals))
    # print("Bet {} 2.5 match goals!".format(bet))
    # gf_mu = round(match_df.aGF, 2)
    # ga_mu = round(match_df.aGA, 2)
    # total_goals = math.ceil(max(max(match_df.aGF), max(match_df.aGA))) + 5
    # x_vals = np.linspace(0, total_goals, total_goals + 1)
    # gf_vals = [[custom_poisson(gf_mu[0], x) for x in x_vals], [custom_poisson(gf_mu[1], x) for x in x_vals]]
    # ga_vals = [[custom_poisson(ga_mu[0], x) for x in x_vals], [custom_poisson(ga_mu[1], x) for x in x_vals]]
    # fig, axes = plt.subplots(2, 2, figsize=(10, 10))
    # ax_plotter(' '.join(home_identifier.split(' ')[:-1]) + str(" GF"), x_vals, gf_vals[0], axes[0, 0], gf_mu[0])
    # ax_plotter(' '.join(home_identifier.split(' ')[:-1]) + str(" GA"), x_vals, ga_vals[0], axes[0, 1], ga_mu[0])
    # ax_plotter(' '.join(away_identifier.split(' ')[:-1]) + str(" GF"), x_vals, gf_vals[1], axes[1, 0], gf_mu[1])
    # ax_plotter(' '.join(away_identifier.split(' ')[:-1]) + str(" GA"), x_vals, ga_vals[1], axes[1, 1], ga_mu[1])
    # plt.show()
    # return bet


#
# home_teams = ['Exeter City', 'Colchester United', 'Crawley Town', 'Leyton Orient', 'Northampton Town', 'Port Vale',
#               'Salford City', 'Stevenage', 'Walsall']
# away_teams = ['Plymouth Argyle', 'Newport County', 'Swindon Town', 'Carlisle United', 'Cambridge United',
#               'Oldham Athletic', 'Scunthorpe United', 'Morecambe', 'Mansfield Town']
# bets = []
#
# for i, team in enumerate(home_teams):
#     bets.append(match_predict("{} HOME".format(team), "{} AWAY".format(away_teams[i]), 'league_two'))

# match_predict("{} HOME".format(home_teams[0]), "{} AWAY".format(away_teams[0]), "league_one")
# print(bets)

if __name__ == "__main__":
    # engine = connect_to_db('premier_league')
    # league = read_table('premier_league', engine)
    # print("League Data:\n", league.data)
    # print("\n\nTeam Count:", league.team_count)
    # match_predict('Sheffield United HOME', 'Manchester United AWAY', 'premier_league')
    prob_predictions('premier_league', 'Sheffield United', 'HOME', 'Manchester United', 'AWAY')

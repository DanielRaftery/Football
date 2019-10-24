import pandas as pd
from db_connection import connect_to_db
import math
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import gamma


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


def custom_poisson(mu, x):
    return round((math.pow(mu, x) * math.exp(-mu)) / (math.factorial(x)), 3)


def ax_plotter(title, x, y, ax, mu):
    ax.grid(True)
    ax.set_title(title, fontsize=20)
    ax.set_ylabel("Probability", fontsize=14)
    ax.set_xlabel("Goals", fontsize=14)
    ax.set_xlim([0, math.ceil(mu + 5)])
    a = mu + 1
    xs = np.linspace(gamma.ppf(0.0001, a), gamma.ppf(0.999, a), 1000)
    ax.plot(xs, gamma.pdf(xs, a), 'r-', lw=2, alpha=1)
    ax.bar(x, y, alpha=0.75, align='edge')
    ax.plot([mu, mu], [0, max(gamma.pdf(xs, a))], 'k--', lw=2, label=r'$\mu$ = ' + str(mu))
    ax.legend(loc='upper right', framealpha=1, shadow=True, fontsize=14)
    return ax


def match_predict(home_identifier, away_identifier, league):
    print()
    engine = connect_to_db(str(league))
    home_info = read_from_db(str(home_identifier), engine)
    away_info = read_from_db(str(away_identifier), engine)
    column_names = ['Team', 'GF', 'GA', 'TG', 'Games', 'aGF', 'aGA', 'aTG', 'o2.5']

    match_df = pd.DataFrame([[' '.join(home_identifier.split(' ')[:-1]), 0, 0, 0, 0, 0, 0, 0, 0],
                             [' '.join(away_identifier.split(' ')[:-1]), 0, 0, 0, 0, 0, 0, 0, 0]],
                            columns=column_names)

    for i in home_info:
        for j in range(1, 4):
            if j == 3 and i[j + 1] > 2.5:
                match_df.iloc[0, 8] += 1
            match_df.iloc[0, j] += i[j + 1]
        match_df.iloc[0, 4] += 1

    for i in away_info:
        for j in range(1, 4):
            if j == 3 and i[j + 1] > 2.5:
                match_df.iloc[1, 8] += 1
            match_df.iloc[1, j] += i[j + 1]
        match_df.iloc[1, 4] += 1

    match_df.aGF = match_df.GF / match_df.Games
    match_df.aGA = match_df.GA / match_df.Games
    match_df.aTG = match_df.TG / match_df.Games

    average_total = match_df.aTG.sum() / 2

    [c1, c2] = match_df.aTG / match_df.Games > 0.5
    goals = (average_total + match_df.iloc[0, 5]) / 2

    if c1:
        goals += 0.5
    if c2:
        goals += 0.5

    if goals > 1.9:
        bet = "OVER"
    else:
        bet = "UNDER"

    print("\n *** Details for {} - {} ***".format(' '.join(home_identifier.split(' ')[:-1]),
                                                  ' '.join(away_identifier.split(' ')[:-1])))
    print(match_df)
    print("\nPredicted result: {} {} - {} {}".format(' '.join(home_identifier.split(' ')[:-1]),
                                                     math.ceil((match_df.iloc[0, 5] + match_df.iloc[1, 6]) / 2),
                                                     ' '.join(away_identifier.split(' ')[:-1]),
                                                     math.ceil((match_df.iloc[1, 5] + match_df.iloc[0, 6]) / 2)))
    print("Predicted match goals: {} ({})".format(math.ceil(goals), goals))
    print("Bet {} 2.5 match goals!".format(bet))
    gf_mu = round(match_df.aGF, 2)
    ga_mu = round(match_df.aGA, 2)
    total_goals = math.ceil(max(max(match_df.aGF), max(match_df.aGA))) + 5
    x_vals = np.linspace(0, total_goals, total_goals + 1)
    gf_vals = [[custom_poisson(gf_mu[0], x) for x in x_vals], [custom_poisson(gf_mu[1], x) for x in x_vals]]
    ga_vals = [[custom_poisson(ga_mu[0], x) for x in x_vals], [custom_poisson(ga_mu[1], x) for x in x_vals]]
    fig, axes = plt.subplots(2, 2, figsize=(10, 10))
    ax_plotter(' '.join(home_identifier.split(' ')[:-1]) + str(" GF"), x_vals, gf_vals[0], axes[0, 0], gf_mu[0])
    ax_plotter(' '.join(home_identifier.split(' ')[:-1]) + str(" GA"), x_vals, ga_vals[0], axes[0, 1], ga_mu[0])
    ax_plotter(' '.join(away_identifier.split(' ')[:-1]) + str(" GF"), x_vals, gf_vals[1], axes[1, 0], gf_mu[1])
    ax_plotter(' '.join(away_identifier.split(' ')[:-1]) + str(" GA"), x_vals, ga_vals[1], axes[1, 1], ga_mu[1])
    plt.show()
    return bet


home_teams = ['Bristol City', 'Derby County', 'Fulham', 'Huddersfield Town', 'Nottingham Forest']
away_teams = ['Charlton Athletic', 'Wigan Athletic', 'Luton Town', 'Middlesbrough', 'Hull City']
bets = []

for i, team in enumerate(home_teams):
    bets.append(match_predict("{} HOME".format(team), "{} AWAY".format(away_teams[i]), "championship"))

# match_predict("{} HOME".format(home_teams[0]), "{} AWAY".format(away_teams[0]), "league_one")
print(bets)

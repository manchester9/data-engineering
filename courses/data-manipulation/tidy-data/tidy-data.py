# %matplotlib inline 
import os
import numpy as np 
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

if int(os.environ.get("MODERN_PANDAS_EPUB", 0)):
    import prep # noqa

pd.options.display.max_rows = 10
sns.set(style='ticks', context='talk')


# Reading in dataset
fp = 'data/nba.csv'

if not os.path.exists(fp):
    tables = pd.read_html("http://www.basketball-reference.com/leagues/NBA_2016_games.html")
    games = tables[0]
    games.to_csv(fp)
else:
    games = pd.read_csv(fp)
print(games.head())


# Indicating columns 
column_names = {'Date': 'date', 'Start (ET)': 'start',
                'Unamed: 2': 'box', 'Visitor/Neutral': 'away_team', 
                'PTS': 'away_points', 'Home/Neutral': 'home_team',
                'PTS.1': 'home_points', 'Unamed: 7': 'n_ot'}

games = games.rename(columns=column_names).dropna(thresh=4).assign(date=lambda x: pd.to_datetime(x['date'], format='%a, %b %d, %Y'))
# [['date', 'away_team', 'away_points', 'home_team', 'home_points']].set_index('date', append=True).rename_axis(["game_id", "date"]).sort_index()
print(games.head())



# How many days of rest did each team get between each game?
# tidy = pd.melt(games.reset_index(),
#                id_vars=['game_id', 'date'], value_vars=['away_team', 'home_team'],
#                value_name='team')
# print(tidy.head())















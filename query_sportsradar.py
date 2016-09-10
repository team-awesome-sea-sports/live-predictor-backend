"""Query data from Sports Radar and emit with necessary fields."""

import os
import sys
import json
import requests

KEY = os.environ['SPORTSRADAR_API_KEY']
ACCESS_LEVEL = 't'
VERSION = '1'
FORMAT = 'json'

BASE_NFL_URL = 'http://api.sportradar.us/nfl-{}{}'.format(ACCESS_LEVEL, VERSION)

SCHEDULE_ROUTE = "{year}/{season}/schedule.{format}"
GAME_ROUTE = "{year}/{season}/{week}/{away_team}/{home_team}/pbp.{format}"


DEFAULT_GAME_INFO = {
    'year': '2016',
    'season': 'PRE',
    'week': '2',
    'away_team': 'MIN',
    'home_team': 'SEA',
    'format': FORMAT,
}

DEFAULT_SEASON_INFO = {
    'year': '2016',
    'season': 'REG',
    'format': FORMAT,
}

OUPUT = 'game_pbp.json'


def main(*args):
    """Run requests against the SportsRadar API."""
    params = {'api_key': KEY}
    game_info = DEFAULT_GAME_INFO
    season_info = DEFAULT_SEASON_INFO
    response = get_game_pbp(game_info, params)
    # response = get_season(season_info, params)
    with open(OUPUT, 'w') as txtfile:
        json.dump(response.json(), txtfile)
    # import pdb;pdb.set_trace()


def get_game_pbp(game_info, params):
    """Get json for a given game ID."""
    game_route = GAME_ROUTE.format(**game_info)
    url = '/'.join((BASE_NFL_URL, game_route))
    print(url)
    return requests.get(url, params=params)


def get_season(season_info, params):
    schedule_route = SCHEDULE_ROUTE.format(**season_info)
    url = '/'.join((BASE_NFL_URL, schedule_route))
    print(url)
    return requests.get(url, params=params)


if __name__ == '__main__':
    args = sys.argv

    main(*args[1:])

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


# Query database
# Is there a new play?
# If so:
#   parse a result object for updating player score
#   parse a situation object for displaying on UI


# json["quarters"][num]["pbp"]

def main(*args):
    """Run requests against the SportsRadar API."""
    params = {'api_key': KEY}
    game_info = DEFAULT_GAME_INFO

    latest_play = get_latest_play(game_info, params)
    print(latest_play)
    import pdb;pdb.set_trace()


def get_latest_play(game_info, params):
    """Get json information of most recent play."""

    response = get_game_pbp(game_info, params)
    current_quarter_idx = 0
    current_pbp_len = 0
    current_items_len = 0
    current_plays_len = 0
    latest_play = None

    j = response.json()
    quarters = j["quarters"]

    if len(quarters) > current_quarter_idx:

        # Update any end-of-quarter stuff

        current_quarter_idx = len(quarters) - 1

    quarter = quarters[current_quarter_idx]
    pbp = quarter["pbp"]
    if len(pbp) > current_pbp_len:
        current_pbp_len = len(pbp)
        latest_drive_or_event = pbp.pop()

        try:
            plays = latest_drive_or_event["actions"]
            # import pdb;pdb.set_trace()
        except:
            pass
            # ???
        else:
            if len(plays) > current_plays_len:
                latest_play = plays.pop()
    return latest_play


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

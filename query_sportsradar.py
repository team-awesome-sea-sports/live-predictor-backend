"""Query data from Sports Radar and emit with necessary fields."""

import os
import sys
import requests

KEY = os.environ['SPORTSRADAR_API_KEY']
ACCESS_LEVEL = 't'
VERSION = '1'
FORMAT = 'json'

BASE_NFL_URL = 'https://api.sportradar.us/nfl-{}{}/games'.format(ACCESS_LEVEL, VERSION)

GAME_ROUTE = '{game_id}/pbp.{format}'


def main(game_id, *args):
    """Run requests against the SportsRadar API."""

    params = {'api_key': KEY}
    game_pbp_response = get_game_pbp(game_id, params)
    print(game_pbp_response.json)


def get_game_pbp(game_id, params):
    """Get json for a given game ID."""
    game_route = GAME_ROUTE.format(game_id=game_id, format=FORMAT)
    url = '/'.join((BASE_NFL_URL, game_route))
    return requests.get(url, params=params)


if __name__ == '__main__':
    args = sys.argv

    main(*args[1:])

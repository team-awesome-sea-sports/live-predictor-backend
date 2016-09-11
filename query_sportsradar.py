"""Query data from Sports Radar and emit with necessary fields."""

import os
import re
import sys
import json
import requests
from functools import partial
from operator import add, sub

# Edge cases
# turnover
#   returned for TD
#   overturned
# fumble recovered by own team
# touchdown
#   overturned
#   kickoff or punt returned for TD
# blocked punt or FG
#   returned for TD

# squence 153

KEY = os.environ['SPORTSRADAR_API_KEY']
ACCESS_LEVEL = 't'
VERSION = '1'
FORMAT = 'json'

BASE_NFL_URL = 'http://api.sportradar.us/nfl-{}{}'.format(ACCESS_LEVEL, VERSION)

SCHEDULE_ROUTE = "{year}/{season}/schedule.{format}"
GAME_ROUTE = "{year}/{season}/{week}/{away_team}/{home_team}/pbp.{format}"

TEAMS = ['MIN', 'SEA']
SIDE_STR = r'(?P<side>' + r'|'.join(TEAMS) + r')'
YARD_GAIN_STR = r'for\s\-?\d{1,3}\syard(s)?'
NEW_YARD_LINE_STR = r'(to|at)(\sthe)?\s' + SIDE_STR + r'\s\d{1,2}'

CULPRIT_STR = r'Penalty\son\s(?P<team>' + r'|'.join(TEAMS) + r')\s\d{1,2}'
LOSS_STR = r'\s(?P<loss>\d{1,2})\syards,\senforced\sat\s' + SIDE_STR + r'\s(?P<yard_line>\d{1,2})'
PENALTY_STR = CULPRIT_STR + r'.*' + LOSS_STR

YARD_GAIN_PAT = re.compile(YARD_GAIN_STR)
NEW_YARD_LINE_PAT = re.compile(NEW_YARD_LINE_STR)

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

    # Will need to get this separately as a one-off
    # response = get_game_pbp(game_info, params)
    # game_data = response.json()
    # latest_play = get_latest_play(game_data, game_info, params)
    # print(latest_play)


def parse_number_from_summary(summary, pattern):
    """Parse the new yard line from a given play summary."""
    match = re.search(pattern, summary)
    try:
        match_string = match.group()
        number_match = re.search(r'\-?\d+', match_string).group()
        return int(number_match)
    except AttributeError:
        raise ValueError(
            'Summary does not contain number pattern: {}'.format(summary)
        )


def parse_pass_or_rush(play):
    """Calculate situation for pass or run play."""
    # if touchdown in summary...
    # if fumble, recovered by other team?

    summary = play["summary"]
    try:
        new_yard_line = parse_number_from_summary(summary, NEW_YARD_LINE_STR)
    except ValueError:
        new_yard_line = play['yard_line']

    if 'Penalty' in summary and 'declined' not in summary:
        return parse_penalty(play)

    if 'INTERCEPTED' in summary:
        return {
            'down': 1,
            'distance': 10,
            'yard_line': new_yard_line,
            'team_on_offense': play['team_on_defense'],
        }

    try:
        yards_gained = parse_number_from_summary(summary, YARD_GAIN_PAT)
    except ValueError:
        yards_gained = 0
    if yards_gained >= play['yfd']:
        new_down = 1
        new_distance = 10
    else:
        new_down = (play['down'] + 1) % 5
        # turnover on downs -- report change in possession
        if new_down:
            new_distance = play['yfd'] - yards_gained
        else:
            new_down = 1
            new_distance = 10

    return {
        'down': new_down,
        'distance': new_distance,
        'yard_line': new_yard_line,
        'team_on_offense': play['team_on_offense'],
    }


def parse_kick_or_punt(play, touchback_yard_line=20):
    """Parse situation results from a punt play."""
    summary = play["summary"]
    if 'Penalty' in summary and 'declined' not in summary:
        result = parse_penalty(play)
    elif 'touchback' in summary:
        result = {'yard_line': touchback_yard_line}
    else:
        result = {'yard_line': parse_number_from_summary(summary, NEW_YARD_LINE_STR)}

    result.update({'down': 1, 'distance': 10})
    return result


parse_rush = parse_pass_or_rush
parse_pass = parse_pass_or_rush
parse_punt = partial(parse_kick_or_punt, touchback_yard_line=20)
parse_kick = partial(parse_kick_or_punt, touchback_yard_line=25)


def parse_penalty(play):
    """Get penalty info from play."""
    summary = play["summary"]
    match = re.search(PENALTY_STR, summary)
    try:
        team = match.groupdict()['team']
        loss = int(match.groupdict()['loss'])
        side = match.groupdict()['side']
        enforced_at = int(match.groupdict()['yard_line'])

        yard_line_func = sub if team == side else add
        distance_func = sub if team == play['team_on_defense'] else add

        distance = distance_func(play['yfd'], + loss)
        if distance <= 0:
            down = 1
            distance = 10
        else:
            down = play['down']

        return {
            'yard_line': yard_line_func(enforced_at, loss),
            'down': down,
            'distance': distance,
        }
    except (AttributeError):
        pass
        import pdb;pdb.set_trace()


def parse_play(new_yard_line_pat, play):
    """Return data for the result of the play and the situation for next."""
    # get the time at start of last play
    # get score and quarter, easy enough

    play_type = play['play_type']
    method = globals()['parse_' + play_type]

    new_data = method(play)

    # Check for turnover
    new_data['clock'] = play['clock']
    new_data['score'] = play['score']
    new_data['quarter'] = play['quarter']

    return play, new_data


def get_latest_play(game_data, game_info, params):
    """Get json information of most recent play."""
    current_quarter_idx = 0
    current_pbp_len = 0
    current_drive_len = 0
    latest_play = {}
    quarters = game_data["quarters"]

    if len(quarters) > current_quarter_idx:

        # Update any end-of-quarter stuff

        current_quarter_idx = len(quarters) - 1

    quarter = quarters[current_quarter_idx]
    pbp = quarter["pbp"]
    if len(pbp) > current_pbp_len:
        current_pbp_len = len(pbp)
        latest_drive = pbp.pop()

        try:
            drive = latest_drive["actions"][:-3]
        except:
            pass
            # It is the coin toss or some other non-drive item
        else:
            if len(drive) > current_drive_len:
                latest_play = drive.pop()
    home_team = game_data['home_team']
    away_team = game_data['away_team']

    latest_play['score'] = {
        home_team['id']: home_team['points'],
        away_team['id']: away_team['points']
    }
    latest_play['quarter'] = quarter['number']
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

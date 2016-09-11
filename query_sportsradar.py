"""Query data from Sports Radar and emit with necessary fields."""

import os
import re
import sys
import json
import time
import boto3
import requests
from itertools import count
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
ACCESS_KEY = os.environ['GAMESTREAM_ACCESS_KEY']
SECRET_KEY = os.environ['GAMESTREAM_SECRET_KEY']
SITUATION_ARN = os.environ['SNS_ARN']
RESULTS_ARN = os.environ['SQS_ARN']

ACCESS_LEVEL = 't'
VERSION = '1'
FORMAT = 'json'

BASE_NFL_URL = 'http://api.sportradar.us/nfl-{}{}'.format(ACCESS_LEVEL, VERSION)

SCHEDULE_ROUTE = "{year}/{season}/schedule.{format}"
GAME_ROUTE = "{year}/{season}/{week}/{away_team}/{home_team}/pbp.{format}"

TEAMS = ['MIN', 'SEA', 'TB', 'ATL', 'MIA', 'NE', 'ARI']
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

TEST_GAME_INFO = {
    'year': '2016',
    'season': 'REG',
    'week': '1',
    'away_team': 'TB',
    'home_team': 'ATL',
    'format': FORMAT,
}

DEFAULT_SEASON_INFO = {
    'year': '2016',
    'season': 'REG',
    'format': FORMAT,
}

OUPUT = 'game_pbp.json'
DELAY = 5

# Query database
# Is there a new play?
# If so:
#   parse a result object for updating player score
#   parse a situation object for displaying on UI


def main(*args):
    """Run requests against the SportsRadar API."""
    game_info = TEST_GAME_INFO
    params = {'api_key': KEY}

    unique = count()

    latest_play_id = ''

    # while True:
    for _ in range(3):
        response = get_game_pbp(game_info, params)
        game_data = response.json()
        game_id = game_data['id']
        latest_play = get_latest_play(game_data, game_info)
        if latest_play and latest_play['id'] != latest_play_id:

            current_sequence = next(unique)
            result, new_sit = parse_play(latest_play)

            result['gameID'] = game_id
            new_sit['gameID'] = game_id
            result['situationID'] = '-'.join((game_id, str(current_sequence)))
            new_sit['situationID'] = '-'.join((game_id, str(current_sequence + 1)))

            latest_play_id = latest_play['id']
            print('Play result:')
            print(latest_play['summary'])
            print('New situation: ')
            print(new_sit)

            sns_client = get_sns_client()
            put_situation_in_sns(new_sit, sns_client)
            put_result_in_sns(result, sns_client)

        time.sleep(30)


def get_sns_client():
    """Set up a boto client for SNS."""
    return boto3.client(
        'sns',
        region_name='us-west-2',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        use_ssl=True,
    )


def put_situation_in_sns(situation, client):
    """Send a situation to the SNS."""
    data = json.dumps(situation)
    return client.publish(
        TopicArn=SITUATION_ARN,
        Message=json.dumps({'default': data}),
        MessageStructure='json'
    )


def put_result_in_sns(result, client):
    """Send a result to the SNS."""
    data = json.dumps(result)
    return client.publish(
        TopicArn=RESULTS_ARN,
        Message=json.dumps({'default': data}),
        MessageStructure='json'
    )


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
    # if fumble, recovered by other team?

    summary = play["summary"]
    team_on_offense = play['team_on_offense']
    try:
        new_yard_line = parse_number_from_summary(summary, NEW_YARD_LINE_STR)
    except ValueError:
        new_yard_line = play['yard_line']

    if 'Penalty' in summary and 'declined' not in summary:
        return parse_penalty(play)

    elif 'INTERCEPTED' in summary:
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
        if new_down:
            new_distance = play['yfd'] - yards_gained
        else:
            new_down = 1
            new_distance = 10
            team_on_offense = play['team_on_defense']

    if team_on_offense != play['side']:
        new_distance = min(new_distance, new_yard_line)

    return {
        'down': new_down,
        'distance': new_distance,
        'yard_line': new_yard_line,
        'team_on_offense': team_on_offense,
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


def parse_extrapoint(play):
    """Return results of extra point/two-point conversion play."""
    return {
        'yard_line': 35,
        'down': 'Kickoff',
        'distance': 'Kickoff',
    }


def parse_fieldgoal(play):
    """Return results of field goal play."""
    # handle penalty on field goal
    # handle blocked/missed field goal
    if "No Good" in play['summary']:
        return {
            'yard_line': play['yard_line'],
            'down': 1,
            'distance': 10,
        }
    return {
        'yard_line': 35,
        'down': 'Kickoff',
        'distance': 'Kickoff',
    }


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
        new_yard_line = yard_line_func(enforced_at, loss)
        if new_yard_line > 50:
            new_yard_line = 50 - (new_yard_line - 50)

        distance_func = sub if team == play['team_on_defense'] else add
        distance = distance_func(play['yfd'], + loss)
        if distance <= 0:
            down = 1
            distance = 10
        else:
            down = play['down']

        return {
            'yard_line': new_yard_line,
            'down': down,
            'distance': distance,
        }
    except (AttributeError):
        pass
        print('AttributeError in parse_penalty')
        # import pdb;pdb.set_trace()
        return {}


def touchdown(play):
    """Return data resulting from touchdown."""
    return {
        'yard_line': 'Extra Point Conversion',
        'down': 'Extra Point Conversion',
        'distance': 'Extra Point Conversion',
    }


def parse_play(play):
    """Return data for the result of the play and the situation for next."""
    # get the time at start of last play
    # get score and quarter, easy enough

    if 'touchdown' in play['summary']:
        new_data = touchdown(play)
    else:
        play_type = play['play_type']
        method = globals()['parse_' + play_type]
        new_data = method(play)

    # Check for turnover
    new_data['clock'] = play['clock']
    new_data['score'] = play['score']
    new_data['quarter'] = play['quarter']
    new_data['side'] = play['side']
    return play, new_data


def get_latest_play(game_data, params):
    """Get json information of most recent play."""
    quarters = game_data["quarters"]

    try:
        current_quarter = quarters.pop()
        pbp = current_quarter["pbp"]
        latest_drive = pbp.pop()
    except (IndexError, KeyError):
        return

    try:
        drive_plays = latest_drive["actions"]
    except KeyError:
        pass
        # It is the coin toss or some other non-drive item
        return
    else:
        play = drive_plays.pop()
        # Might be an event e.g. TV time out
        if play['type'] != 'play':
            return
    home_team = game_data['home_team']
    away_team = game_data['away_team']
    play['team_on_offense'] = latest_drive['team']

    if latest_drive['team'] == home_team['id']:
        play['team_on_defense'] = away_team['id']
    else:
        play['team_on_defense'] = home_team['id']
    play['score'] = {
        home_team['id']: home_team['points'],
        away_team['id']: away_team['points'],
    }
    play['quarter'] = current_quarter['number']
    return play


def get_game_pbp(game_info, params):
    """Get json for a given game ID."""
    game_route = GAME_ROUTE.format(**game_info)
    url = '/'.join((BASE_NFL_URL, game_route))
    return requests.get(url, params=params)


# def get_season(season_info, params):
#     schedule_route = SCHEDULE_ROUTE.format(**season_info)
#     url = '/'.join((BASE_NFL_URL, schedule_route))
#     print(url)
#     return requests.get(url, params=params)


if __name__ == '__main__':
    args = sys.argv

    main(*args[1:])

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
# fumble for turnover
# 3-R.Wilson FUMBLES (Aborted) at SEA 44. 47-K.Alonso recovers at the SEA 38. 47-K.Alonso to SEA 35 for 3 yards (79-G.Gilliam).
#   returned for TD
#   overturned
# fumble recovered by own team
# touchdown
#   overturned
#   kickoff or punt returned for TD
# blocked punt or FG
#   returned for TD

API_KEY = os.environ['SPORTSRADAR_API_KEY']
ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
SECRET_KEY = os.environ['AWS_SECRET_KEY']
SNS_ARN = os.environ['SNS_ARN']
SQS_URL = os.environ['SQS_URL']

ACCESS_LEVEL = 't'
VERSION = '1'
FORMAT = 'json'

BASE_NFL_URL = 'http://api.sportradar.us/nfl-{}{}'.format(ACCESS_LEVEL, VERSION)
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

TEST_GAME_INFO = {
    'year': '2016',
    'season': 'PRE',
    'week': '2',
    'away_team': 'MIN',
    'home_team': 'SEA',
    'format': FORMAT,
}

LIVE_GAME_INFO = {
    'year': '2016',
    'season': 'REG',
    'week': '1',
    'away_team': 'NE',
    'home_team': 'ARI',
    'format': FORMAT,
}

DEFAULT_GAME_INFO = {
    'year': '2016',
    'season': 'REG',
    'format': FORMAT,
}

OUPUT = 'game_pbp.json'
DELAY = 2


def main(game_info=LIVE_GAME_INFO):
    """Run requests against the SportsRadar API."""
    sns_client = get_boto_client('sns')
    print('sns client created')
    sqs_client = get_boto_client('sqs')
    print('sqs client created')
    params = {'api_key': API_KEY}

    unique = count()

    latest_play_id = ''

    while True:
        response = get_game_pbp(game_info, params)
        game_data = response.json()
        if game_data.get('status') == 'closed':
            break
        game_id = game_data['id']
        try:
            latest_play = get_latest_play(game_data, game_info)
        except Exception as e:
            print('Error trying to get latest play:')
            print(e)
            latest_play = {}

        if not latest_play or latest_play['id'] == latest_play_id:
            print('No latest play.')
            time.sleep(DELAY)
            continue

        try:
            result, new_sit = parse_play(latest_play)
        except Exception as e:
            print('Error trying to parse latest play:')
            print(e)
            time.sleep(DELAY)
            continue

        current_sequence = next(unique)
        result['gameID'] = game_id
        new_sit['gameID'] = game_id
        result['situationID'] = '-'.join((game_id, str(current_sequence)))
        new_sit['situationID'] = '-'.join((game_id, str(current_sequence + 1)))

        latest_play_id = latest_play['id']
        print('Play result:')
        print(latest_play['summary'])
        print('New situation: ')
        print(new_sit)

        send_sns_data(new_sit, sns_client)
        send_sqs_data(result, sqs_client)

        time.sleep(DELAY)


def get_boto_client(service):
    """Set up a boto client for SNS."""
    return boto3.client(
        service,
        region_name='us-west-2',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        use_ssl=True,
    )


def send_sns_data(data, client):
    """Send a situation to the SNS."""
    data = json.dumps(data)
    return client.publish(
        TopicArn=SNS_ARN,
        Message=json.dumps({'default': data}),
        MessageStructure='json'
    )


def send_sqs_data(data, client):
    """Send a situation to the SNS."""
    data = json.dumps(data)
    return client.send_message(
        QueueUrl=SQS_URL,
        MessageBody=data,
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
            'yfd': 10,
            'yard_line': new_yard_line,
            'team_on_offense': play['team_on_defense'],
        }

    try:
        yards_gained = parse_number_from_summary(summary, YARD_GAIN_PAT)
    except ValueError:
        yards_gained = 0

    if yards_gained >= play['yfd']:
        new_down = 1
        new_yfd = 10
    else:
        new_down = (play['down'] + 1) % 5
        if new_down:
            new_yfd = play['yfd'] - yards_gained
        else:
            new_down = 1
            new_yfd = 10
            team_on_offense = play['team_on_defense']

    if team_on_offense != play['side']:
        new_yfd = min(new_yfd, new_yard_line)

    return {
        'down': new_down,
        'yfd': new_yfd,
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

    result.update({'down': 1, 'yfd': 10})
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
        'yfd': 'Kickoff',
    }


def parse_fieldgoal(play):
    """Return results of field goal play."""
    # handle penalty on field goal
    # handle blocked/missed field goal
    if "No Good" in play['summary']:
        return {
            'yard_line': play['yard_line'],
            'down': 1,
            'yfd': 10,
        }
    return {
        'yard_line': 35,
        'down': 'Kickoff',
        'yfd': 'Kickoff',
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

        yfd_func = sub if team == play['team_on_defense'] else add
        yfd = yfd_func(play['yfd'], + loss)
        if yfd <= 0:
            down = 1
            yfd = 10
        else:
            down = play['down']

        return {
            'yard_line': new_yard_line,
            'down': down,
            'yfd': yfd,
        }
    except (AttributeError):
        print('AttributeError in parse_penalty')
        return {}


def touchdown(play):
    """Return data resulting from touchdown."""
    return {
        'yard_line': 'Extra Point Conversion',
        'down': 'Extra Point Conversion',
        'yfd': 'Extra Point Conversion',
    }


def parse_play(play):
    """Return data for the result of the play and the situation for next."""
    # get the time at start of last play

    play.update({'touchdown': False, 'turnover': False})

    if 'touchdown' in play['summary']:
        new_data = touchdown(play)
        play['touchdown'] = True
    else:
        method = globals()['parse_' + play['play_type']]
        new_data = method(play)

    # Check for turnover
    new_data['clock'] = play['clock']
    new_data['score'] = play['score']
    new_data['quarter'] = play['quarter']
    new_data['side'] = play['side']

    if play['play_type'] == 'rush' and play.get('distance') is None:
        try:
            summary = play['summary']
            yards_gained = parse_number_from_summary(summary, YARD_GAIN_PAT)
            play['distance'] = 'Long' if yards_gained > 11 else 'Short'
        except ValueError:
            pass
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


if __name__ == '__main__':
    args = sys.argv[1:]
    try:
        week, away_team, home_team = args
    except ValueError:
        print(
            'Usage:\n'
            'python query_sportsradar.py <week> <away_team> <home_team>\n'
            '<week> must be a number. <away_team> and <home_team> must be '
            'NFL standard 2 or 3 uppercase letter abbreviations of teams.'
        )
        sys.exit()
    game_info = DEFAULT_GAME_INFO.copy()
    game_info.update(dict(week=week, away_team=away_team, home_team=home_team))

    main(game_info)

"""Query data from Sports Radar and emit with necessary fields."""

import os
import sys
import requests

KEY = os.environ['SPORTSRADAR_API_KEY']


def main(*args):
    """Run requests against the SportsRadar API."""

    requests.get()


if __name__ == '__main__':
    args = sys.argv

    main(*args[1:])
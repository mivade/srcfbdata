"""Get CFB data from sports-reference.com."""

import os
import os.path as osp
import time
import hashlib
import functools
from datetime import datetime
import argparse
import requests
import bs4
import pandas as pd
try:
    import colorama
    colorama.init(autoreset=True)
    RED = colorama.Fore.RED
except ImportError:
    RED = ''

# A dict of urls to get data from. The keys should be the same as in the
# metadata dict.
urls = {
    'schools': 'http://www.sports-reference.com/cfb/schools/',
    'schedule': 'http://www.sports-reference.com/cfb/years/{year:d}-schedule.html'
}

# Metadata for data tables.
metadata = {
    'schools': {
        'columns': [
            'school', 'start_year', 'last_year', 'years',
            'games', 'wins', 'losses', 'ties', 'win_percentage',
            'bowl_games', 'bowl_wins', 'bowl_losses', 'bowl_ties', 'bowl_percentage',
            'srs', 'sos', 'ap', 'conference_championships', 'notes'
        ]
    },
    'schedule': {
        'columns': [
            'week', 'date', 'time', 'day',
            'winner', 'pts_winner', 'atsign', 'loser', 'pts_loser',
            'tv', 'notes'
        ],
        'reordered_columns': [
            'timestamp', 'home', 'pts_home', 'away', 'pts_away',
            'tv', 'notes'
        ],
        'renamed_columns': {
            'winner': 'away', 'pts_winner': 'pts_away',
            'loser': 'home', 'pts_loser': 'pts_home'
        }
    }
}


def get_table(dtype, year, args):
    """Get and process the table at the specified URL.

    Note that this is not generic for *any* table yet and will only work
    for pages with only one table.

    """
    assert dtype in urls
    assert isinstance(year, int)

    if dtype in ['schedule']:
        url = urls[dtype].format(year=year)
    else:
        url = urls[dtype]

    filename = None
    if args.debug:
        try:
            os.mkdir('.cache')
        except FileExistsError:
            pass
        finally:
            filename = osp.join(
                '.cache', hashlib.md5(url.encode()).hexdigest() + '.html')

    print('[{}]'.format(time.ctime()))
    if args.debug and filename and osp.exists(filename):
        print('Using cached file for ' + url)
        with open(filename) as fp:
            html = fp.read()
    else:
        print('GET ' + url)
        html = requests.get(url).content
        if args.debug:
            with open(filename, 'w') as fp:
                fp.write(html.decode())

    print('Parsing...')
    soup = bs4.BeautifulSoup(html, 'lxml')
    table = soup.find_all('table')[0].tbody
    rows = table.find_all('tr', class_=lambda cs: cs in ['', 'ranked'])
    print('Finished: [{}]'.format(time.ctime()))

    df = pd.read_html(
        '<table>' + ''.join([str(row) for row in rows]) + '</table>',
        index_col=0)[0]
    if len(df.columns) != len(metadata[dtype]['columns']):
        cols = [col for col in metadata[dtype]['columns'] if col != 'tv']
    else:
        cols = metadata[dtype]['columns']
    df.columns = cols

    if args.debug:
        print(RED + 'get_table finished')
        print(df.head())

    if dtype == 'schedule':
        df = process_schedule(df, args)

    return df


def process_schedule(df, args):
    """Do additional processing of schedule data."""
    # Combine all date and time columns into a single datetime column.
    dates, times = df.date, df.time
    afternoon = [t.split()[1] for t in times]
    afternoon = [12 if 'PM' in t else 0 for t in afternoon]
    start = pd.DataFrame(
        [(int(t.split(':')[0]), int(t.split(':')[1].split()[0])) for t in times],
        columns=('hour', 'minute'))
    start.hour += afternoon
    start.hour[start.hour == 24] = 12  # Because afternoon above doesn't work right for noon!

    dt = pd.to_datetime(dates)
    start['year'] = dt.dt.year.values
    start['month'] = dt.dt.month.values
    start['day'] = dt.dt.day.values

    df['timestamp'] = [pd.datetime(t.year, t.month, t.day, t.hour, t.minute) for _, t in start.iterrows()]
    if args.debug:
        print(RED + 'timestamp added')
        print(df.head())

    # Make winner -> home and loser -> away
    idx = df.atsign == '@'
    df.loc[idx, 'winner'], df.loc[idx, 'loser'] = df.loc[idx, 'loser'], df.loc[idx, 'winner']
    df.loc[idx, 'pts_winner'], df.loc[idx, 'pts_loser'] = df.loc[idx, 'pts_loser'], df.loc[idx, 'pts_winner']
    if args.debug:
        print(RED + 'winner -> home, loser -> away')
        print(df.head())

    # Fix column names
    df.rename(columns=metadata['schedule']['renamed_columns'], inplace=True)
    if args.debug:
        print(RED + 'renamed')
        print(df.head())

    # Drop unneeded columns: week, date, time, day, atsign
    df.drop(['week', 'date', 'time', 'day', 'atsign'], 1, inplace=True)
    if args.debug:
        print(RED + 'dropped')
        print(df.head())

    # Reorder columns
    df = df.reindex(columns=metadata['schedule']['reordered_columns'])
    if args.debug:
        print(RED + 'reordered')
        print(df.head())

    # Convert points to numeric data types
    df.pts_home = df.pts_home.convert_objects(convert_numeric=True)
    df.pts_away = df.pts_away.convert_objects(convert_numeric=True)

    # Remove rankings from team names and strip remaining spaces
    regex = r'\([0-9]+\)'
    df.home = df.home.str.replace(regex, '').str.lstrip()
    df.away = df.away.str.replace(regex, '').str.lstrip()

    return df


def main(defaults=None):
    parser = argparse.ArgumentParser(
        description="Retrieve data for a given year")
    parser.add_argument(
        '-y', '--year', default=datetime.now().year,
        help='Year to get data for (default: this year)')
    parser.add_argument(
        '-d', '--debug', default=False, action='store_true',
        help='Enable debug mode')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--schedule', action='store_true',
        help='Download a schedule')
    group.add_argument(
        '--schools', action='store_true',
        help="Download schools' all-time data.")
    if defaults:
        args = parser.parse_args(defaults)
    else:
        args = parser.parse_args()

    get_table_ = functools.partial(get_table, args=args)

    if args.schedule:
        df = get_table_('schedule', int(args.year))
        df.to_csv(
            osp.join('data', 'schedule_{:}.csv'.format(args.year)), index=False)
    if args.schools:
        df = get_table_('schools', int(args.year))
        df.to_csv(osp.join('data', 'schools.csv'), index=False)

if __name__ == "__main__":
    main()

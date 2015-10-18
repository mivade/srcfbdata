"""Get CFB data from sports-reference.com."""

import time
import requests
import bs4
import pandas as pd

# A dict of urls to get data from. The keys should be the same as in the
# metadata dict.
urls = {
    'schools': 'http://www.sports-reference.com/cfb/schools/',
    'schedule': 'http://www.sports-reference.com/cfb/years/{:d}-schedule.html'
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
        ]
    }
}


def get_table(dtype, year=None):
    """Get and process the table at the specified URL.

    Note that this is not generic for *any* table yet and will only work
    for pages with only one table.

    """
    assert dtype in urls
    assert isinstance(year, int) or year is None
    if year:
        url = urls[dtype].format(year)
    else:
        url = urls[dtype]
    print('[{}]'.format(time.ctime()))
    print('GET ' + url)
    html = requests.get(url).content

    print('Parsing...')
    soup = bs4.BeautifulSoup(html, 'lxml')
    table = soup.find_all('table')[0].tbody
    rows = table.find_all('tr', class_='')
    print('Finished: [{}]'.format(time.ctime()))

    df = pd.read_html(
        '<table>' + ''.join([str(row) for row in rows]) + '</table>',
        index_col=0)[0]
    df.columns = metadata[dtype]['columns']

    if dtype == 'schedule':
        df = process_schedule(df)

    return df


def process_schedule(df):
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

    # Remove rankings from team names and strip remaining spaces
    regex = r'\([0-9]+\)'
    df.home = df.home.str.replace(regex, '').str.lstrip()
    df.away = df.away.str.replace(regex, '').str.lstrip()

    # Make winner -> home and loser -> away
    idx = df.atsign == '@'
    df.loc[idx, 'winner'], df.loc[idx, 'loser'] = df.loc[idx, 'loser'], df.loc[idx, 'winner']
    df.loc[idx, 'pts_winner'], df.loc[idx, 'pts_loser'] = df.loc[idx, 'pts_loser'], df.loc[idx, 'pts_winner']

    # Convert points to numeric data types
    df.pts_home = df.pts_home.convert_objects(convert_numeric=True)
    df.pts_away = df.pts_away.convert_objects(convert_numeric=True)

    # Drop unneeded columns: week, date, time, day, atsign
    df.drop(['week', 'date', 'time', 'day', 'atsign'], 1, inplace=True)

    # Reorder columns
    df = df.reindex(columns=metadata['schedule']['reordered_columns'])

    return df

if __name__ == "__main__":
    for key in urls:
        df = get_table(key)
        df.to_csv('data/{}.csv'.format(key), index=False)

"""Scrape CFB data from sports-reference.com."""

from __future__ import print_function
import logging
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, ioloop
import pandas as pd

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__.split('.')[0])

def _schedule_url(year):
    """Build the URL for the schedule for the desired year."""
    return "http://www.sports-reference.com/cfb/years/{:d}-schedule.html".format(year)

@gen.coroutine
def download_schedule(year):
    """Get the full schedule from the specified year."""
    assert isinstance(year, int)
    url = _schedule_url(year)
    logger.info('Getting schedule data for {:d}...'.format(year))
    logger.debug('URL: {:s}'.format(url))
    response = yield AsyncHTTPClient().fetch(url)
    schedule = pd.read_html(response.body.decode('utf-8'), attrs={'id': 'schedule'})[0]

    # Get rid of rows that are just repeating the header (which is
    # useful for viewing on an HTML page, but not useful as a table
    # for manipulation!).
    schedule = schedule[~schedule.Rk.str.match('Rk')]

    # Rename columns. This is mostly cosmetic for purposes of making
    # it more obvious what we're doing to the table later.
    schedule.rename(
        columns={
            'Wk': 'Week',
            'Winner/Tie': 'Winner',
            'Pts': 'WinnerPoints',
            'Unnamed: 7': 'HomeOrAway',
            'Loser/Tie': 'Loser',
            'Pts.1': 'LoserPoints',
            'Unnamed: 12': 'Empty'},
        inplace=True)

    # Swap team columns so that away is always on the left and home is
    # always on the right and rename the columns appropriately.
    home_won = schedule.HomeOrAway.isnull()
    schedule.Winner[home_won], schedule.Loser[home_won] = schedule.Loser[home_won], schedule.Winner[home_won]
    schedule.WinnerPoints[home_won], schedule.LoserPoints[home_won] = schedule.LoserPoints[home_won], schedule.WinnerPoints[home_won]

    # Rename Winner/Loser to Away/Home
    schedule.rename(
        columns={
            'Winner': 'Away',
            'Loser': 'Home',
            'WinnerPoints': 'AwayPoints',
            'LoserPoints': 'HomePoints'},
        inplace=True)

    # TODO: remove unplayed games from the schedule

    # Remove unnecessary columns.
    schedule.drop(
        ['Rk', 'Week', 'Day', 'HomeOrAway', 'Empty', 'TV', 'Notes'],
        axis=1, inplace=True)
    
    raise gen.Return(schedule)

@gen.coroutine
def main():
    """Main function."""
    schedule = yield download_schedule(2014)
    schedule.to_csv('schedule_2014.csv', encoding='utf-8', index=False)

if __name__ == "__main__":
    if True:
        ioloop.IOLoop.instance().run_sync(main)
    else:
        schedule = pd.read_html(_schedule_url(2014), attrs={'id': 'schedule'})[0]
    
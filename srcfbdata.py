"""Scrape CFB data from sports-reference.com."""

from __future__ import print_function
import logging
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, ioloop
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__.split('.')[0])

def _schedule_url(year):
    """Build the URL for the schedule for the desired year."""
    return "http://www.sports-reference.com/cfb/years/{:d}-schedule.html".format(year)

@gen.coroutine
def download_schedule(year):
    """Get the full schedule from the specified year."""
    assert isinstance(year, int)
    url = _schedule_url(year)
    logger.info('Getting data for {:d}'.format(year))
    logger.debug('URL: {:s}'.format(url))
    response = yield AsyncHTTPClient().fetch(url)
    schedule = pd.read_html(response.body, attrs={'id': 'schedule'})[0]

    # Get rid of rows that are just repeating the header (which is
    # useful for viewing on an HTML page, but not useful as a table
    # for manipulation!).
    bad_rows = schedule[schedule.Rk.str.match('Rk')]
    schedule = schedule.drop(bad_rows)

    # Rename columns. This is mostly cosmetic for purposes of making
    # it more obvious what we're doing to the table later.
    schedule.rename(
        columns={
            'Wk': 'Week',
            'Winner/Tie': 'Winner',
            'Pts': 'WinnerPoints',
            'Unnamed: 7': 'HomeOrAway',
            'Lower/Tie': 'Loser',
            'Pts.1': 'LoserPoints',
            'Unnamed: 12': 'Empty'},
        inplace=True)

    # TODO: mask '@' in HomeOrAway so that columsn can be reordered
    # with away always first and home always second.

    # TODO: remove unplayed games from the schedule

    # Remove unnecessary columns.
    schedule.drop('Week')
    schedule.drop('Day', 1)
    schedule.drop('Empty')
    
    raise gen.Return(schedule)

@gen.coroutine
def main():
    """Main function."""
    schedule = yield download_schedule(2014)
    schedule.to_csv('schedule_2014.csv')

if __name__ == "__main__":
    if False:
        ioloop.IOLoop.instance().run_sync(main)
    else:
        schedule = pd.read_html(_schedule_url(2014), attrs={'id': 'schedule'})[0]
    
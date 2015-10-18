"""Get CFB data from sports-reference.com."""

import time
import requests
import bs4
import pandas as pd

# A dict of urls to get data from. The keys should be the same as in the
# metadata dict.
urls = {
    'schools': 'http://www.sports-reference.com/cfb/schools/'
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
    }
}


def get_table(dtype):
    """Get and process the table at the specified URL.

    Note that this is not generic for *any* table yet and will only work
    for pages with only one table.

    """
    assert dtype in urls
    print('[{}]'.format(time.ctime()))
    print('Getting ' + urls[dtype] + '...')
    html = requests.get(urls[dtype]).content

    print('Parsing...')
    soup = bs4.BeautifulSoup(html, 'lxml')
    table = soup.find_all('table')[0].tbody
    rows = table.find_all('tr', class_='')
    print('Finished: [{}]'.format(time.ctime()))

    df = pd.read_html(
        '<table>' + ''.join([str(row) for row in rows]) + '</table>',
        index_col=0)[0]
    df.columns = metadata[dtype]['columns']
    return df

if __name__ == "__main__":
    for key in urls:
        df = get_table(key)
        df.to_csv('data/{}.csv'.format(key), index=False)

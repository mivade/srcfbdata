Sports-Reference.com CFB Data Extractor
=======================================

The kind folks at
[sports-reference.com](http://www.sports-reference.com) have quite a
bit of sports data available in nice HTML tables (which they are even
awesome enough to convert to CSV for you!). This repository simple
automates the process of downloading and manipulating the tables of
college football data.

Currently, the following data extraction is implemented:

* Schools: Overview of all "major" schools for their entire history.
  This includes things like wins/losses.

Requirements
------------

* [Pandas][]
* [Requests][]
* [Beautiful Soup][] and [lxml][]

[Tornado]: tornadoweb.org
[Pandas]: http://pandas.pydata.org/
[Requests]: http://docs.python-requests.org/en/latest/
[Beautiful Soup]: http://www.crummy.com/software/BeautifulSoup/
[lxml]: http://lxml.de/

License
-------

The source files in this repository are freely available under the
terms of the GNU GPL version 3.

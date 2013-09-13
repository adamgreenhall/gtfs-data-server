import os
from psycopg2 import connect
import urlparse
import pandas as pd

if os.environ.get('IS_HEROKU', False):
    # if Heroku 
    # https://devcenter.heroku.com/articles/heroku-postgresql#connecting-in-python
    urlparse.uses_netloc.append("postgres")
    url = urlparse.urlparse(os.environ["DATABASE_URL"])

    con = connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    config = dict(
        debug=False,
        host='0.0.0.0',
        port=int(os.environ['PORT']),
        db=os.environ["DATABASE_URL"],
        url_update="http://www.bart.gov/dev/gtrtfs/tripupdate.aspx"
        )
    print 'config={}'.format(config)
else:
    # is local
    con = connect("postgresql://postgres@localhost/bart-gtfs")
    config = dict(
        debug=True,
        port=5000)


def service_type(timestamp):
    '''get BART service_id'''
    dayofweek = pd.Timestamp(timestamp).dayofweek # monday is 0, ..., sat is 5, sun is 6
    if dayofweek == 5:
        return 'SAT'
    elif dayofweek == 6:
        return 'SUN'
    else:
        return 'WKDY'
import os
from psycopg2 import connect
import urlparse
from json import load
import subprocess

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
        debug=True,
        host='0.0.0.0',
        port=int(os.environ['PORT']),
        db=os.environ["DATABASE_URL"],
        url_update="http://www.bart.gov/dev/gtrtfs/tripupdate.aspx"
        )
    
    
    # heroku needs to run the gtfsrdb script to get updates
    subprocess.Popen(
        'python gtfsrdb/gtfsrdb.py -t {url_update} \
            -d {db} --create-tables --wait {t}'.format(
            t=60 * 5, # check for new data every t sec
            url_update=config['url_update'],
            db=config['db']
        ),
        shell=True)
    
    
else:
    # is local
    con = connect("postgresql://postgres@localhost/bart-gtfs")
    config = dict(
        debug=True,
        port=5000)
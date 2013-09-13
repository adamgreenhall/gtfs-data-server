import os
from psycopg2 import connect
import urlparse

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
        port=int(os.environ.get('PORT', 33507)))

else:
    # is local
    con = connect("postgresql://postgres@localhost/bart-gtfs")
    config = dict(
        debug=True,
        port=5000)

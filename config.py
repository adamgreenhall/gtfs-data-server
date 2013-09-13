import os
from psycopg2 import connect
from flask.ext.heroku import Heroku

def configure(app):
    if os.environ.get('IS_HEROKU', False):
        # if Heroku 
        Heroku(app)
        return connect(app.config['SQLALCHEMY_DATABASE_URI'])
    else:
        # is local
        return connect("postgresql://postgres@localhost/bart-gtfs")

from flask import Flask, jsonify, request
import pandas as pd
from config import con, config
import subprocess
# from ipdb import set_trace

app = Flask(__name__)

query_template = """
SELECT trips.route_id, trips.trip_id, trips.trip_headsign, 
    stop_time_updates.stop_id, stop_time_updates.arrival_delay,
    stop_times.arrival_time, trip_updates.timestamp
FROM trip_updates, stop_time_updates, trips, stop_times
WHERE 
    trips.trip_id::text = trip_updates.trip_id::text AND 
    trip_updates.oid = stop_time_updates.trip_update_id AND
    stop_time_updates.stop_id = stop_times.stop_id AND 
    trips.trip_id::text = stop_times.trip_id::text
"""

if not config['debug']:
    subprocess.Popen(
        'python gtfsrdb/gtfsrdb.py -t #{url_update} -d #{db} --create-tables --wait 30'.format(
        url_update=config['url_update'],
        db=config['db']
        ),
        shell=True)

@app.route('/schedule')
def get_schedule():
    route_id = request.args.get('route_id', None)
    date = request.args.get('date', None)
    query = str(query_template)
    if route_id is not None:
        query += " AND trips.route_id = '{}'".format(route_id)
    if date is not None:
        date = pd.Timestamp(date).date()
        query += " and trip_updates.timestamp:date = {}".format(date)
    # TODO - select by date
    df = pd.read_sql(query, con)
    
    placeholder = '__df__'
    response = jsonify(data=placeholder)
    response.data = response.data.replace(
        '"{}"'.format(placeholder), df.to_json(orient='records'))
    return response

@app.route('/test-db')
def test_db():
    cur = con.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    result = cur.fetchall()
    return jsonify(database_tables=result)

@app.route('/test')
def test():
    return jsonify(app_ok=True)

if __name__ == '__main__':
    app.run(**config)
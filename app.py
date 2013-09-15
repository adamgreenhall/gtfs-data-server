from flask import Flask, jsonify, request
import pandas as pd
from config import con, config, service_type
import subprocess
from datetime import timedelta
from json import load

if config['debug']:
    from ipdb import set_trace


app = Flask(__name__)

if not config['debug']:
    subprocess.Popen(
        'python gtfsrdb/gtfsrdb.py -t {url_update} \
            -d {db} --create-tables --wait 30'.format(
            url_update=config['url_update'],
            db=config['db']
        ),
        shell=True)

    # heroku needs to run get_stop_distances first
    from get_stop_distances import get_stop_dists
    stop_dists = get_stop_dists()
    print stop_dists
else:
    with open('route-distances.json', 'r') as f:
        stop_dists = load(f)


@app.route('/schedule')
def get_schedule():
    date = request.args.get('date', "2013-09-13")
    route_id = request.args.get('route_id', "01")
    json = create_schedule(date, route_id)
    return json


def create_schedule(date, route_id):
    # take the params and create a properly formatted schedule df
    date = pd.Timestamp(date)
    # first get the trips and stops for the route
    query_schedule = """
      select 
          trips.trip_id, trips.direction_id, 
          st.stop_id, st.stop_sequence, st.arrival_time, st.departure_time
      from trips, stop_times st
      where trips.trip_id = st.trip_id    
    """
    query_updates = """
        select 
            tu.trip_id, tu.timestamp,
            stu.stop_id, stu.arrival_delay, stu.departure_delay
          from trip_updates tu, stop_time_updates stu
          where tu.oid = stu.trip_update_id
    """
    
    query_schedule += """
        and trips.service_id='{service}' 
        and trips.route_id=%s""".format(service=service_type(date))
    query_updates += " and tu.timestamp between '{d}' and '{nd}'".format(
        d=date.date(), nd=(date + pd.DateOffset(days=1)).date())
    
    query = """
    select 
        s.trip_id, s.direction_id,
        s.stop_id, s.stop_sequence, s.arrival_time, s.departure_time,
        u.arrival_delay, u.departure_delay, u.timestamp
    from ({s}) s   left join ({u}) u
    on (u.trip_id = s.trip_id and u.stop_id = s.stop_id)
    order by s.trip_id, s.stop_sequence
    """.format(s=query_schedule, u=query_updates)
    
    df = pd.read_sql(query, con, params=[route_id])
    
    trips = []
    trip_stops = {}
    for (id_trip, trip_direction), stops in \
        df.groupby(['trip_id', 'direction_id']):
        stops['time_arrival'] = pd.to_datetime(
            str(date.date()) + ' ' + stops.arrival_time
            ) #  + stops.departure_delay.fillna(0).shift(1)

        if (stops.departure_delay > 0).any():
            # departure delay = arrival delay at next stop
            stops['time_arrival'] += stops.departure_delay.shift(1).fillna(0)\
                .apply(lambda t: timedelta(seconds=t))
        
        col_names=dict(
            stop_id='id_stop',
            time_arrival='time_arrival',
            )
        stops = stops[col_names.keys()].rename(columns=col_names)
        stops['count'] = 0
        stops['count_boarding'] = 0
        stops['count_exiting'] = 0
        
        trip_stops[id_trip] = stops.to_json(orient='records')
        trips.append(dict(
            id_trip=id_trip,
            trip_direction=trip_direction,
            stops="__trip_stops__{}".format(id_trip),
            ))

    # placeholder_stop_locs = "__stop_locs__"
    response = jsonify(data=dict(
        date=str(date.date()),        
        stop_locations=stop_dists[route_id],
        id_route=route_id,
        trips=trips,
        ))

    for id_trip, stops_json in trip_stops.iteritems():
        response.data = response.data.replace(
                '"__trip_stops__{}"'.format(id_trip),
                stops_json)
    return response

@app.route('/test-db')
def test_db():
    cur = con.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables \
        WHERE table_schema = 'public';")
    result = cur.fetchall()
    return jsonify(database_tables=result)

@app.route('/test')
def test():
    return jsonify(app_ok=True)

if __name__ == '__main__':
    app.run(**config)
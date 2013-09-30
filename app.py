from flask import Flask, jsonify, request
from flask_utils import crossdomain
from utils import pd, service_type, prints, parse_date_times, timedelta, set_trace
from datetime import datetime
from config import con, config, launch_gtfs_updates

# heroku needs to run get_stop_distances first
from get_stop_distances import get_stop_dists
stop_dists = get_stop_dists()


app = Flask(__name__)

counts = {nm: pd.read_csv('passenger_counts/{}.csv'.format(nm), index_col=0) 
    for nm in [
        'exits-WKDY', 'entries-WKDY', 
        'exits-SAT', 'entries-SAT',
        'exits-SUN', 'entries-SUN'
        ]}
        
launch_gtfs_updates(config)

# TODO - rate limit via: flask.pocoo.org/snippets/70/
@app.route('/schedule')
@crossdomain(origin='*')
def get_schedule():
    default_date = pd.Timestamp(datetime.now()) - pd.DateOffset(days=3)
    date = request.args.get('date', default_date)
    route_id = request.args.get('route_id', "01")
    try: 
        response = create_schedule(date, route_id)
    except Exception, e:
        prints(e)
        if config['debug']:
            raise
        else:
            response = jsonify(error=str(e))
    return response

def get_trip_stops(date, stops):
    direction = stops['direction_id'].iloc[0]
    stops['time_arrival'] = parse_date_times(date, stops.arrival_time)

    if (stops.departure_delay > 0).any():
        # departure delay = arrival delay at next stop
        stops['time_arrival'] += stops.departure_delay.shift(1)\
                .fillna(0).apply(lambda t: timedelta(seconds=t))
        
    col_names=dict(
        stop_id='id_stop',
        time_arrival='time_arrival',
        )
    stops = stops[col_names.keys()].rename(columns=col_names)
    
    hour = stops.time_arrival.iloc[0].hour
    service = service_type(date)
    stops = stops.set_index('id_stop')
    stops['count_exiting'] = counts['exits-{}'.format(service)].ix[hour][stops.index]
    stops['count_boarding'] = counts['entries-{}'.format(service)].ix[hour][stops.index]
    # hack 
    stops['count'] = stops['count_boarding']
    return stops.reset_index(), direction

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
    for i, (id_trip, stops_data) in enumerate(df.groupby('trip_id')):
        stops, direction = get_trip_stops(date, stops_data)            
        trip_stops[id_trip] = stops.to_json(orient='records')
        trips.append(dict(
            id_trip=id_trip,
            trip_direction=direction,
            time_start= str(stops.time_arrival.min()),
            stops="__trip_stops__{}".format(id_trip),
            ))
    
    response = jsonify(data=dict(
        date=str(date.date()),        
        stop_locations= stop_dists.get(route_id, []),
        id_route=route_id,
        trips=sorted(trips, key=lambda t: t['time_start']),
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
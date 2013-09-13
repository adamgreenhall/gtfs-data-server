from flask import Flask, jsonify, request
import pandas as pd
from config import con, config, service_type
import subprocess
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
    query = """
    select 
        t.trip_id, t.direction_id, 
        st.stop_id, st.stop_sequence, st.arrival_time, st.departure_time,
        stu.arrival_delay, stu.departure_delay
    from 
        trips t, stop_times st, 
        trip_updates tu, stop_time_updates stu
    where 
        t.trip_id = st.trip_id and t.trip_id = tu.trip_id and
        tu.oid = stu.trip_update_id and 
        stu.stop_id = st.stop_id    
    """
    
    query += """
        and t.service_id='{service}' 
        and t.route_id=%s""".format(service=service_type(date))
    # query += " and tu.timestamp between '{d}' and '{nd}'".format(
    #     d=date.date(), nd=(date + pd.DateOffset(days=1)).date())        
    df = pd.read_sql(query, con, params=[route_id])
    
    
    trips = []
    for (id_trip, trip_direction), stops in \
        df.groupby(['trip_id', 'direction_id']):
        
        set_trace()
        trips.append(dict(
            id_trip=id_trip,
            trip_direction=trip_direction,
            stops=stops,
            ))
    
    # "trips": [
    #     {
    #         "id_trip": "758",
    #         "trip_direction": 1,
    #         "stops": [
    #             {
    #                 "count": 0,
    #                 "time_arrival": 1349077922,
    #                 "id_stop": 3893,
    #                 "count_boarding": 0,
    #                 "count_exiting": 0
    #             }, ...
    #         ]}, 
    # ... ]

    data = dict(
        date=str(date.date()),
        stop_locations={}, # list of dict(distance, direction, id_stop)
        id_route=route_id,
        trips=trips,
        )
        
    
    response = jsonify(data=data)
    # response.data = response.data.replace(
    #     '"{}"'.format(placeholder), df.to_json(orient='records'))
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
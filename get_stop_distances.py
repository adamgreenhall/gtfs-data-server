"""
get the distances between stops
"""
import numpy as np
import math
import pandas as pd
from json import dumps


def get_linear_dist(df):
    '''convert lat long to a linear distance travelled since start'''
    LL = ['latitude', 'longitude']
    latlng = df.reset_index()[LL].fillna(0).copy()
    latlng['distance'] = 0.0
    for i, coords in latlng.iterrows():
        if i == 0: continue
        latlng.ix[i, 'distance'] = haversine_distance(
            latlng.ix[i - 1][LL].values,
            coords[LL].values)

    dists = latlng.distance.replace(0, np.nan)
    dists.ix[0] = 0

    if dists.isnull().any():
        print('interpolating missing stop distances')
        return dists.cumsum().interpolate().values

    return dists.cumsum().values


def haversine_distance(origin, destination):
    # Haversine formula for great circle distance
    # platoscave.net/blog/2009/oct/5/calculate-distance-latitude-longitude-python/
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1-a))

def get_dists(trip_id, con):
    print 'trip_id: ', trip_id
    query = """
    select 
        s.stop_id, s.stop_lat, s.stop_lon, st.stop_sequence 
    from stop_times st inner join stops s 
    on st.stop_id = s.stop_id
    where trip_id = '{}'    
    order by stop_sequence
    """.format(trip_id)
    
    df = pd.read_sql(query, con)

    dists = pd.DataFrame(dict(
        stop_id=df.stop_id,
        distance=get_linear_dist(df.rename(columns=dict(
            stop_lat='latitude', stop_lon='longitude')))
        ))
    dists.index.name = 'stop_number'
    return dists
    
if __name__ == "__main__":
    from config import con
    trip_ids = pd.read_sql('select distinct(trip_id) from trips', con).trip_id
    jsons = dumps({tid: "__dists_{}__".format(tid) for tid in trip_ids})
    
    # TODO - get per route distances based on longest trip
    # not just per-trip distances
    
    for tid in trip_ids:
        jsons = jsons.replace('"__dists_{}__"'.format(tid), 
            get_dists(tid, con).to_json(orient='records'))
    
    with open('trip-distances.json', 'w+') as f:
        f.write(jsons)
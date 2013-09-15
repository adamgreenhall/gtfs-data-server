"""
get the distances between stops
"""
import numpy as np
import math
import pandas as pd
from json import dumps
from config import con

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
    query = """
    select 
        s.stop_id, s.stop_lat, s.stop_lon, st.stop_sequence 
    from stop_times st inner join stops s 
    on st.stop_id = s.stop_id
    where trip_id = '{}'    
    order by stop_sequence
    """.format(trip_id)
    
    df = pd.read_sql(query, con)

    dists = pd.Series(
        get_linear_dist(df.rename(columns=dict(
            stop_lat='latitude', stop_lon='longitude'))),
        index=df.stop_id
        )
    dists.name = 'distance'
    dists.index.name = 'stop_id'
    return dists
    
def merge_dists(dists_direction):
    if len(dists_direction) == 1:
        dists = dists_direction[dists_direction.keys()[0]]
    else:
        inbound = dists_direction[1]
        outbound = dists_direction[0]
        dists = pd.DataFrame(dict(
            inbound=inbound,
            outbound=outbound.max() - outbound
            )).mean(axis=1)

    dists.index.name = 'id_stop'
    dists.name = 'distance'
    dists = dists.reset_index().sort('distance').reset_index(drop=True)
    dists['direction'] = -1 # all stops are in both trip directions

    if len(dists_direction) > 1:
        inbound_only = dists.id_stop.isin(inbound.index) & \
            (dists.id_stop.isin(outbound.index) == False)
        outbound_only = dists.id_stop.isin(outbound.index) & \
            (dists.id_stop.isin(inbound.index) == False)
        dists.ix[inbound_only, 'direction'] = 1
        dists.ix[outbound_only, 'direction'] = 0
    return dists
    
    
def get_route_dists():
    query = """
    select 
        t.route_id, t.trip_id, t.direction_id, 
        count(st.stop_id) as stop_count
    from trips t, stop_times st
    where t.trip_id = st.trip_id
    group by t.trip_id    
    """
    df = pd.read_sql(query, con)
    df['direction_id'] = df.direction_id.fillna(-1)
    trip_max = pd.DataFrame(dict(indexer=df.groupby(['route_id', 'direction_id']).stop_count.idxmax()))
    trip_max['trip_id'] = df.trip_id.ix[trip_max.indexer].values
    route_ids = trip_max.index.levels[0]
    
    jsons = dumps({rid: "__dists_{}__".format(rid) for rid in route_ids})
    
    # get per route distances based on longest trip (in either direction)
    # not just per-trip distances
    
    for rid in route_ids:
        print rid
        dists = merge_dists({direction: get_dists(tid, con) 
            for direction, tid in trip_max.ix[rid].trip_id.iteritems()})
        
        jsons = jsons.replace('"__dists_{}__"'.format(rid), 
            dists.to_json(orient='records', double_precision=2))
    
    with open('route-distances.json', 'w+') as f:
        f.write(jsons)        

if __name__ == "__main__":
    get_route_dists()
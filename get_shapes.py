'''
parser for shapefiles (stops and routes)
not required for running the server.
'''

import os
import pandas as pd
from ujson import dumps as dump_json
import json
from os.path import join as joindir
from config import con

def feature_collection(list):
    return dict(type="FeatureCollection", features = list)


def feature(properties, coordinates, geom_type='LineString'):
    return dict(
        type='Feature', 
        properties=properties,
        geometry=dict(type=geom_type, coordinates=coordinates)
        )


def shape_to_geojson(shape_id, shape):
    '''
    converts an individual shape (like a route) listing to geojson format
    shape format from: developers.google.com/transit/gtfs/reference#shapes_fields
    ''' 
    
    return feature(
        properties=dict(shape_id=shape_id),
        coordinates=
            shape.sort('shape_pt_sequence')[['shape_pt_lat', 'shape_pt_lon']]\
                .values.tolist(),
        geom_type="LineString")


def create_topojson(dict, name, directory='shapes-topojson'):
    # dict is in geojson format
    tmp_fnm = joindir(directory, '{}.geojson'.format(name))
    with open(tmp_fnm, 'w+') as f:
        f.write(dump_json(dict))
    # need to have topojson installed
    print('creating {}'.format(name))
    os.system("topojson {tmp} -o {fnm} --properties".format(
        tmp=tmp_fnm, fnm=joindir(directory, name + '.topojson')))
    # os.remove(tmp_fnm)
    

# normally you would do this - but BART doesn't supply a complete shapefile
# for all routes as of Sept 2013
# https://groups.google.com/forum/#!topic/bart-developers/LQCHnZUqQ_8
def parse_shapes():
    q = 'select shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence from shapes'
    shapes = pd.read_sql(q, con)
    geojson = feature_collection([
        shape_to_geojson(sid, shape)
        for sid, shape in shapes.groupby('shape_id')
        ])
    create_topojson(geojson, 'routes')

def parse_shapes_hack():
    os.system('cd shapes-topojson && make bart.geojson')
    with open('shapes-topojson/bart.geojson', 'r') as f:
        geo = json.load(f)

    q = "select route_id, route_long_name, route_color from routes where agency_id='BART'"
    routes = pd.read_sql(q, con)
    
    # filter to just the lines
    lines = filter(
        lambda f: f['geometry']['type'] == 'GeometryCollection', 
        geo['features'])
    
    for ln in lines:
        # get the route id
        rid = "%02d" % int(ln['properties']['name'].split('ROUTE ')[1].split('/')[0])
        ln['properties'] = routes[routes.route_id == rid].iloc[0].to_dict()
    
    geo['features'] = lines
    create_topojson(geo, 'routes')
    

def parse_stops():
    
    q = 'select stop_id, stop_name, stop_lat, stop_lon from stops'
    stops = pd.read_sql(q, con)

    geojson = feature_collection([
        feature(
            properties=row.drop(['stop_lat', 'stop_lon']).dropna().to_dict(), 
            coordinates=row[['stop_lon', 'stop_lat']].values.tolist(),
            geom_type='Point')
        for i, row in stops.iterrows()
        ])
    create_topojson(geojson, 'stops')
    

if __name__ == '__main__':
    parse_stops()
    parse_shapes_hack()
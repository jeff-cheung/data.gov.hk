import networkx as nx
import pandas as pd
from pathlib import Path
from datetime import date, datetime
from shapely.geometry import Point
import matplotlib.pyplot as plt
import re

def retrieve_data_last_upd_date(filename='DATA_LAST_UPDATED_DATE.csv'):
    data_folder = Path('gtfs_data')
    path = data_folder / filename
    #print(path.resolve())
    with open(path.resolve(), 'r') as f:
        f.readline()
        last_upd_dt = f.readline()
        #print(last_upd_dt)
        return datetime.strptime(last_upd_dt, "%Y-%m-%d").date()

def read_gtfs_data(data_type):
    data_file = data_type + '.txt'
    data_folder = Path('gtfs_data')
    path = data_folder / data_file
    raw_data = pd.read_csv(path.resolve())#.fillna('')
    last_upd_dt = retrieve_data_last_upd_date()
    raw_data['as_of_date'] = last_upd_dt

    return refine_data(raw_data, data_type)

def refine_data(raw_data, data_type):
    if data_type == 'routes':
        raw_data.set_index('route_id')

    if data_type == 'stops':
        geom = [Point(xy) for xy in zip(raw_data.stop_lon, raw_data.stop_lat)]
        raw_data['stop_lat_lon'] = geom
        raw_data = raw_data.drop(['stop_lat', 'stop_lon'], axis = 1)

        #crs = {'init': 'epsg:4326'}
        #gdf = GeoDataFrame(raw_data, crs=crs, stop_lat_lon=geom)
    
    if data_type == 'stop_times':
        raw_data['arrival_hr'] = pd.to_numeric(raw_data['arrival_time'].str.split(':').str[0])
        raw_data['departure_hr'] = pd.to_numeric(raw_data['departure_time'].str.split(':').str[0])

        raw_data.loc[raw_data['arrival_hr'] >= 24, 'arrival_time'] = \
            (raw_data['arrival_hr'] - 24).astype('str').str.split('.').str[0] + \
            ':' + raw_data['arrival_time'].str.split(':').str[1] + ':' + raw_data['arrival_time'].str.split(':').str[2]

        raw_data.loc[raw_data['departure_hr'] >= 24, 'departure_time'] = \
            (raw_data['departure_hr'] - 24).astype('str').str.split('.').str[0] + \
            ':' + raw_data['departure_time'].str.split(':').str[1] + ':' + raw_data['departure_time'].str.split(':').str[2]

        raw_data.drop(['arrival_hr', 'departure_hr'], axis=1, inplace=True)

    if data_type == 'calendar':
        raw_data['start_date'] = pd.to_datetime(raw_data['start_date'], format='%Y%m%d')
        raw_data['end_date'] = pd.to_datetime(raw_data['end_date'], format='%Y%m%d')

    if data_type == 'calendar_dates':
        raw_data['date'] = pd.to_datetime(raw_data['date'], format='%Y%m%d')   

    if data_type == 'frequencies':
        raw_data['start_hr'] = pd.to_numeric(raw_data['start_time'].str.split(':').str[0])
        raw_data['end_hr'] = pd.to_numeric(raw_data['end_time'].str.split(':').str[0])

        raw_data.loc[raw_data['start_hr'] >= 24, 'start_time'] = \
            (raw_data['start_hr'] - 24).astype('str').str.split('.').str[0] + \
            ':' + raw_data['start_time'].str.split(':').str[1] + ':' + raw_data['start_time'].str.split(':').str[2]

        raw_data.loc[raw_data['end_hr'] >= 24, 'end_time'] = \
            (raw_data['end_hr'] - 24).astype('str').str.split('.').str[0] + \
            ':' + raw_data['end_time'].str.split(':').str[1] + ':' + raw_data['end_time'].str.split(':').str[2]

        raw_data.drop(['start_hr', 'end_hr'], axis=1, inplace=True)

    return raw_data

def generate_edge(df):
    for index in range(len(df)-1, 0, -1):
        yield(df[index-1], df[index])

def generate_stops(route_no):
    routes_data = read_gtfs_data('routes')
    trips_data = read_gtfs_data('trips')
    stops_data = read_gtfs_data('stops')

    selected_route_id = routes_data[routes_data.route_short_name == route_no].route_id.values[0]
    selected_agency_id = routes_data[routes_data.route_short_name == route_no].agency_id.values[0]

    selected_trip_id = trips_data[trips_data.route_id == selected_route_id].trip_id.values[0]

    stop_times_data = read_gtfs_data('stop_times')
    stop_times_data = stop_times_data[stop_times_data.trip_id == selected_trip_id]

    pd_merge = pd.merge(stop_times_data, stops_data, on='stop_id')

    stop_name_dict = pd_merge[['stop_id', 'stop_name']].set_index('stop_id').to_dict()['stop_name']

    dict = {}
    re_exp = re.compile('\[(?P<agency>...*)\] (?P<stop_name>...*)')
    for id, name in iter(stop_name_dict.items()):
        #dict[id] = name.split('|')
        for n in name.split('|'):
            if (id, re_exp.match(n).groupdict()['agency']) in dict.keys():
                dict[(id, re_exp.match(n).groupdict()['agency'])] = dict[(id, re_exp.match(n).groupdict()['agency'])] + \
                    ' / ' + re_exp.match(n).groupdict()['stop_name']
            else:
                dict[(id, re_exp.match(n).groupdict()['agency'])] = re_exp.match(n).groupdict()['stop_name']
    
    for k in stop_name_dict.keys():
        stop_name_dict[k] = dict[(k, selected_agency_id)]
    
    return stop_name_dict


if __name__ == '__main__':

    stop_name_dict = generate_stops('962X')

    print(list(stop_name_dict))

    G = nx.DiGraph()
    G.add_edges_from(generate_edge(list(stop_name_dict)))

    print(G.nodes.data())

    nx.relabel_nodes(G, stop_name_dict, copy=False)
    nx.draw(G, with_labels=True, font_size=6)
    plt.show()
    

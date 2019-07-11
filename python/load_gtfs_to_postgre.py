import psycopg2
from configparser import ConfigParser
from pathlib import Path
from datetime import date, datetime
import pandas as pd
from sqlalchemy import engine_from_config, create_engine
import urllib.request
import traceback
#from geopandas import GeoDataFrame
from shapely.geometry import Point
from psycopg2.extensions import adapt, register_adapter, AsIs
import numpy as np

'''
class Point(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y
'''

def adapt_point(point: Point):
    x = adapt(point.x).getquoted().decode()
    y = adapt(point.y).getquoted().decode()
    return AsIs("'(%s, %s)'" % (x, y))

def config(filename='database.ini', section='python-postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
 
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
 
    return db

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
   # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 

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
    raw_data = pd.read_csv(path.resolve())
    last_upd_dt = retrieve_data_last_upd_date()
    raw_data['as_of_date'] = last_upd_dt

    return refine_data(raw_data, data_type)

def refine_data(raw_data, data_type):
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

def write_gtfs_data_to_postgre(data, data_type):
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        #cur = conn.cursor()
        
        engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost/gtfs")
        #engine = engine_from_config(params)
        data.to_sql(name=data_type, con=engine, if_exists='append', index=False, chunksize=10000)
       
       # close the communication with the PostgreSQL
        # cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        #traceback.print_exc()
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def check_latest_update_dte():
    with urllib.request.urlopen('http://static.data.gov.hk/td/pt-headway-en/DATA_LAST_UPDATED_DATE.csv') as f:
        f.readline()
        #return datetime.strptime(str(f.readline()), "%Y-%m-%d").date()
        return f.readline()

if __name__ == '__main__':
    register_adapter(Point, adapt_point)

    latest_update_dte = check_latest_update_dte()
    print(latest_update_dte)
    #connect()
    #last_upd_dt = retrieve_data_last_upd_date()
    #print(last_upd_dt)
    
    #agency_data = read_gtfs_data('agency')
    #write_gtfs_data_to_postgre(agency_data, 'agency')

    #stops_data = read_gtfs_data('stops')
    #print(stops_data.head(5))
    #write_gtfs_data_to_postgre(stops_data, 'stops')

    #routes_data = read_gtfs_data('routes')
    #print(routes_data.head(5))
    #print(routes_data['route_short_name'][routes_data['route_short_name'].apply(str).apply(len).argmax()])
    '''
    for r in routes_data['route_short_name']:
        if str(r).find('suspen') > -1:
            print(r)
    '''
    #write_gtfs_data_to_postgre(routes_data, 'routes')

    #trips_data = read_gtfs_data('trips')
    #print(trips_data.head(5))
    #write_gtfs_data_to_postgre(trips_data, 'trips')

    #stop_times_data = read_gtfs_data('stop_times')
    #print(stop_times_data.head(5))
    #write_gtfs_data_to_postgre(stop_times_data, 'stop_times')
    
    #calendar_data = read_gtfs_data('calendar')
    #print(calendar_data.head(5))
    #write_gtfs_data_to_postgre(calendar_data, 'calendar')

    #calendar_dates_data = read_gtfs_data('calendar_dates')
    #print(calendar_dates_data.head(5))
    #write_gtfs_data_to_postgre(calendar_dates_data, 'calendar_dates')

    #frequencies_data = read_gtfs_data('frequencies')
    #print(frequencies_data.head(5))
    #write_gtfs_data_to_postgre(frequencies_data, 'frequencies')

    #fare_attributes_data = read_gtfs_data('fare_attributes')
    #print(fare_attributes_data.head(5))
    #write_gtfs_data_to_postgre(fare_attributes_data, 'fare_attributes')

    fare_rules_data = read_gtfs_data('fare_rules')
    print(fare_rules_data.head(5))
    write_gtfs_data_to_postgre(fare_rules_data, 'fare_rules')


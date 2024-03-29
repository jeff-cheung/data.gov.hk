import avro.schema
from pathlib import Path
from datetime import date, datetime
import pandas as pd
from shapely.geometry import Point
from psycopg2.extensions import adapt, register_adapter, AsIs

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

def adapt_point(point: Point):
    x = adapt(point.x).getquoted().decode()
    y = adapt(point.y).getquoted().decode()
    return AsIs("'(%s, %s)'" % (x, y))

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
    raw_data = pd.read_csv(path.resolve()).fillna('')
    last_upd_dt = retrieve_data_last_upd_date()
    raw_data['as_of_date'] = last_upd_dt.toordinal() - date(1970, 1, 1).toordinal()

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

def write_data_to_avro(raw_data, data_type):
    
    data_folder = Path('avro')
    avro_file = data_type + '.avro'
    avro_file_path = data_folder / avro_file
    avsc_file = data_type + '.avsc'
    avsc_file_path = data_folder / avsc_file

    schema = avro.schema.Parse(open(avsc_file_path.resolve(), "rb").read())

    writer = DataFileWriter(open(avro_file_path.resolve(), "wb"), DatumWriter(), schema)

    for _ , record in raw_data.iterrows():
        dict = record.to_dict()

        if data_type == "stops":
            dict['stop_lat_lon'] = {'stop_lon': dict['stop_lat_lon'].x, 'stop_lat': dict['stop_lat_lon'].y}
        '''
        if data_type == "stop_times":
            #del dict['arrival_time']
            #del dict['departure_time']
            del dict['stop_id']
            del dict['stop_sequence']
            del dict['pickup_type']
            del dict['drop_off_type']
            del dict['timepoint']
        '''
        writer.append(dict)
    
    writer.close()
    '''
    reader = DataFileReader(open(path.resolve(), "rb"), DatumReader())
    for agency in reader:
        print(agency)

    reader.close()
    '''

if __name__ == '__main__':
    register_adapter(Point, adapt_point)

    #agency_data = read_gtfs_data('agency')
    #write_data_to_avro(agency_data, 'agency')

    #stops_data = read_gtfs_data('stops')
    #write_data_to_avro(stops_data, 'stops')

    #routes_data = read_gtfs_data('routes')
    #write_data_to_avro(routes_data, 'routes')

    #stop_times_data = read_gtfs_data('stop_times')
    #write_data_to_avro(stop_times_data, 'stop_times')

    trips_data = read_gtfs_data('trips')
    write_data_to_avro(trips_data, 'trips')


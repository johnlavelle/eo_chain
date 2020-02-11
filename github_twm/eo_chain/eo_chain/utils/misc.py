from os.path import basename
import dateutil.parser
from datetime import timedelta


def parse(fname):
    fname = basename(fname)
    names = ['satellite', 'data_source', 'processing_level', 'data_type_id',
             'start_time', 'stop_time', 'creation_date', 'duration', 'cycle_number',
             'relative_orbit_number', 'tile_id1', 'tile_id2', 'frame_along_track_coordinate',
             'tile_id', 'platform', 'timeliness', 'baseline_collection_or_data_usage']

    values = [n for n in fname.split('_') if n]  # get parts of filename
    values[-1] = values[-1].split('.')[0]  # handle extension
    return {name: value for name, value in zip(names, values)}


def get_info(fname):
    info = parse(fname)
    info['start_time'] = dateutil.parser.parse(info['start_time'])
    info['stop_time'] = dateutil.parser.parse(info['stop_time'])
    info['creation_date'] = dateutil.parser.parse(info['creation_date'])
    return info



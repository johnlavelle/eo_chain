"""
Coastval timeseries to database

Usage:
  coast_val_to_mongodb <input_directory>
  coast_val_to_mongodb (-h | --help)

Options:
  -h --help     Show this screen.

"""


import os
import re
import tilt_cleaner
from datetime import datetime
import numpy as np
from glob import glob
from os.path import dirname, basename, join
from pymongo import MongoClient, ASCENDING
from docopt import docopt
import stats


headers = {}

path_parent = dirname(os.getcwd())


def read_headers():
    headers_filename = join(path_parent, "data/coastval_data_keys.txt")
    with open(headers_filename, 'r') as headers_file:
        for line in headers_file:
            if ';' not in line:
                continue
            stripped = line.strip()
            names = stripped.split(';')[:-1]
            data_type = names[0].strip()
            field_names = [re.sub(r'\(.+?\)', '', n) for n in names[1:]]

            if data_type == 'OPTICAL':
                headers[data_type] = [f.strip().replace(' ', '_') for f in field_names]
            else:
                headers[data_type] = field_names[0].strip().replace(' ', '').split(',')


def read_data(file):
    data = {'OPTICAL': {}, 'TILT': [], 'SBE-39': [], 'HYDROCAT-EP': []}
    discarded = 0
    total = 0
    with open(file, 'r') as data_file:
        for line in data_file:

            if not line or line.strip() == '':
                continue

            total = total + 1
            fields = line.strip().split(';')
            data_type = fields.pop(0).strip()
            if data_type == 'OPTICAL':
                sample = dict(zip(headers[data_type], fields))
                try:
                    """
                    for idx, v in enumerate(vals):
                        print v
                        if ' ' in v:
                            print 'found'
                            print idx, v
                    """

                    light_data = []

                    for element in sample['light_data'].strip(', ').split(',') :

                        element = float(element.replace(' ', ''))
                        light_data.append(element)

                    sample['light_data'] = light_data

                except KeyError as e:
                    discarded = discarded + 1
                    continue
                if sample['sample_header'] not in data[data_type]:
                    data[data_type][sample['sample_header']] = []
                data[data_type][sample['sample_header']].append(sample)
            else:
                if data_type == 'HYDROCAT-EP':
                    try:
                        timestamp = fields.pop()
                        hydrocat_data = fields[0].split(',')
                        filtered = [hydrocat_data[i] for i in [1, 2, 3, 4, 5, 6, 7, 8, 9, 12]]
                        filtered.append(timestamp)
                        fields[0] = ",".join(filtered)

                    except Exception as e:
                        discarded = discarded + 1
                        continue

                for field in fields:

                    try:
                        subfields = []

                        for element in field.strip().split(','):
                            element = float(element)
                            subfields.append(element)

                    except ValueError as e:
                        discarded = discarded + 1
                        continue
                    try:
                        subsample = dict(zip(headers[data_type], subfields))
                    except KeyError as e:
                        discarded = discarded + 1
                        continue
                    data[data_type].append(subsample)
    return (total, discarded, data)


def optical_to_list(data_dict):
    for instrument_name, instrument_dict_list in data_dict.items():
        for instrument_dict in instrument_dict_list:
            instrument_dict['instrument'] = instrument_name
            yield instrument_dict


def insert(input_directory):
    opened = 0
    processed = 0

    for x in sorted(glob(join(input_directory, '*/*/*.csv'))):
        idx = 0
        instruments = []

        opened += 1
        try:
            total, discarded, data = read_data(x)

            data['SBE39'] = data.pop('SBE-39')
            data['HYDROCAT'] = data.pop('HYDROCAT-EP')

            print(str(discarded) + " lines discarded out of " + str(total) + " in file : " + x)

            filename = x.strip('.csv')

            fname_out = 'Mat/output/' + filename + '.mat'
            basedir = os.path.dirname(fname_out)
            try:
                os.makedirs(basedir)
            except OSError:
                pass
            # sio.savemat(fname_out, {'coastval_data': data})  # careful, no

            data_dict = {}
            # timestamp = datetime.fromtimestamp(int(basename(x).replace('.csv', '')))
            timestamp = int(basename(x).replace('.csv', ''))

            data_optical = data.pop('OPTICAL')
            data_optical = optical_to_list(data_optical)
            data_optical = sorted(data_optical, key=lambda d: d['start_time'])

            for row in data_optical:
                for k in row.keys():
                    if k not in ['start_time', 'end_time', 'end_time', 'integration_time', 'dark_samples',
                                 'dark_average', 'spectrometer_temp', 'timer']:
                        continue
                    if isinstance(row[k], str):
                        if "." in row[k]:
                            row[k] = np.float(row[k].replace("+", "").replace("-", ""))
                        else:
                            row[k] = np.int(row[k])
                row['start_time'] = datetime.fromtimestamp(row['start_time'])
                row['end_time'] = datetime.fromtimestamp(row['end_time'])
                row['timestamp_file'] = timestamp

                try:
                    diff = row['start_time'] - start_time_old
                    if diff.total_seconds() < 0:
                        # raise ValueError
                        pass
                    if (diff.total_seconds() >= 1) or (row['instrument'] in instruments):
                        idx += 1
                        instruments = []
                except NameError:
                    pass
                finally:
                    start_time_old = row['start_time']
                    instruments.append(row['instrument'])
                row['idx'] = idx

                post_id = coll_radiances.insert_one(row).inserted_id
            for i, k in enumerate(data.keys()):
                for row in data[k]:
                    row['timestamp'] = datetime.fromtimestamp(row['timestamp'])
                    row['timestamp_file'] = timestamp
                    row['idx'] = i
                    post_id = collections[k].insert_one(row).inserted_id
            processed += 1

        except Exception as e:
            print("Error with file :" + x)
            print(str(e))
            # raise
    return opened, processed


if __name__ == '__main__':
    args = docopt(__doc__)
    read_headers()
    tilt_cleaner.process_files()  # cleans up TILT data

    client = MongoClient('localhost', 27017)
    db = client['coastval_data_analysis-db']
    # db.radiances.drop()
    # db.tilt.drop()
    # db.enviroment.drop()
    # db.environment.drop()
    # db.tilt_stats.drop()
    coll_tilt_stats = db.tilt_stats

    collection = db['coastval_data_analysis']
    coll_radiances = db.radiances
    coll_tilt = db.tilt
    coll_environment = db.environment
    coll_hydrocat = db.hydrocat

    collections = {'TILT': coll_tilt, 'SBE39': coll_environment, 'HYDROCAT': coll_hydrocat}

    opened, processed = insert(args['<input_directory>'])

    print(str(opened) + ' files opened ' + str(processed) + ' files processed successfully')

    print('Creating Indices')
    start_time_idx = coll_radiances.create_index([("timestamp_file", ASCENDING),
                                                  ("idx", ASCENDING)], name='start_time_idx')
    db.tilt.create_index([("timestamp", ASCENDING)], name='ts_index')
    db.environment.create_index([("timestamp", ASCENDING)], name='ts_index')

    print('Computing stats for tilt')
    for t in stats.get_tilt_stats(db):
        post_id = db.tilt_stats.insert_one(t).inserted_id

    print('Computing stats for environment')
    for t in stats.get_environment_stats(db):
        post_id = db.environment_stats.insert_one(t).inserted_id

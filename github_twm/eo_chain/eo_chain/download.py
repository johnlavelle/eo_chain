#!/usr/bin/env python3

"""

Usage:
    process.py <startdate> <stopdate>
    process.py (--last_days=<days>)
"""

from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
from itertools import groupby
from os.path import join
from glob import glob
from os.path import basename
from docopt import docopt
import os
from zipfile import ZipFile
from datetime import timedelta
from datetime import datetime
from utils.readwrite import AwsS3


api = SentinelAPI('jlavelle', 'j724IhXqmYah', 'https://coda.eumetsat.int/')


def find_products(area_path, start, stop, producttype):
    footprint = geojson_to_wkt(read_geojson(area_path))

    products = api.query(footprint,
                         date=(start, stop),
                         platformname='Sentinel-3',
                         producttype=producttype,
                         limit=None)
    print(f'{len(products.keys())} products found')
    return products


def products_by_date(area_path, start, stop, producttype):
    products = find_products(area_path, start, stop, producttype)
    print(set(i['producttype'] for p, i in products.items()))
    for start_date, prods in groupby(products.items(), key=lambda x: x[1]['beginposition']):
        save_path = join(save_dir_base,
                         str(start_date.year),
                         str(start_date.month).zfill(2),
                         str(start_date.day).zfill(2))
        # Exclude swaths that we already have
        # local_files = [basename(fn) for fn in glob(join(save_path, '*.*'))]
        s3_files = list(s3.get_swath_filenames())
        products_remote = {prod_id: info for prod_id, info in prods if info['filename'] not in s3_files}
        if products_remote:
            yield save_path, products_remote


def uncompress(zip_dir):
    # Create a ZipFile Object and load sample.zip in it
    print('uncompressing')
    for fn in glob(join(zip_dir, '*.zip')):
        with ZipFile(fn, 'r') as zipObj:
            zipObj.extractall(zip_dir)
        os.remove(fn)


area_path = './data/areas/dublin_bay.geojson'
save_dir_base = '/tmp/sentinel3/'



def main(start, stop, producttype):
    for save_path, products_remote in products_by_date(area_path, start, stop, producttype):
        try:
            os.makedirs(save_path)
        except OSError:
            pass
        api.download_all(products_remote, directory_path=save_path)
        uncompress(save_path)
        s3.upload_directory(save_path, 'source')


if __name__ == '__main__':
    args = docopt(__doc__)
    if args['--last_days']:
        last_days = int(args['--last_days'])
    stop_d = datetime.now()
    start_d = datetime.now() - timedelta(days=last_days)
    s3 = AwsS3('olci-oceancolour', start_d, stop_d)
    for p_type in ['OL_2_WFR___', 'OL_1_EFR___']:
        pass
        main(start_d, stop_d, p_type)

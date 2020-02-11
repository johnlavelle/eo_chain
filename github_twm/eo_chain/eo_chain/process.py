#!/usr/bin/env python3

"""
Usage:
    process.py [<startdate> <stopdate>]
    process.py (--last_days=<days>)
    process.py (--all)
    process.py (--date=<date>)
"""


from satpy import Scene, find_files_and_readers
import xarray as xr
from glob import glob
# import utils.filename_info as filename_info
from utils.misc import get_info
# from . import utils
from os.path import join
from docopt import docopt
from datetime import datetime, timedelta
from datetime import date
from os.path import exists, dirname
import os
import zarr
import s3fs
from utils.readwrite import AwsS3
from itertools import groupby
import io
import re

def save_dataset(scene, composite, filename, format='netcdf'):
    ds = scene.to_xarray_dataset()
    ds_oc = ds[composite].to_dataset()

    for t in ['start_time', 'end_time']:
        ds_oc[composite].attrs[t] = ds_oc[composite].attrs[t].timestamp()
    for k, v in ds_oc[composite].attrs.items():
        if v is None:
            ds_oc[composite].attrs[k] = ''
    ds_oc[composite].attrs['area'] = ds_oc[composite].attrs['area'].name
    del ds_oc[composite].attrs['prerequisites']
    #= str(ds_oc.crs.values)
    # del ds_oc['crs']
    ds_oc.attrs = ds_oc[composite].attrs
    ds_oc[composite].attrs = {}

    lons, lats = scene[composite].attrs['area'].get_lonlats()
    lons, lats = xr.DataArray(lons, dims=('y', 'x')), xr.DataArray(lats, dims=('y', 'x'))
    lons.attrs = {'long_name': 'longitude', 'standard_name': 'longitude', 'units': 'degrees_east'}
    lats.attrs = {'long_name': 'latitude', 'standard_name': 'latitude', 'units': 'degrees_north'}
    ds_oc = ds_oc.assign_coords({'lon': lons, 'lats': lats})

    filename = filename.format(name=composite)
    if format == 'netcdf':
        ds_oc.to_netcdf(filename)
    elif format == 'zarr':
        ds_oc = ds_oc.expand_dims(dim='time')
        ds_oc['time'] = [ds_oc.start_time]
        ds_oc = ds_oc.assign_coords(time=[ds_oc.start_time])
        fname = join('/', join(*filename.split('/')[:-2]), filename.split('__')[-1])
        print(f'Saving to {fname}')
        # s3 = s3fs.S3FileSystem()
        # store = s3fs.S3Map(root='s3://olci-oceancolour/cog_S3A_OL_1_EFR____20200121T112913_20200121T113135_20200121T142625_0142_054_080_1980_MAR_O_NR_002__true_color_marine_clean.tif', s3=s3, check=False)
        store = None

        if not exists(fname):
            compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)
            encodings = {v: {'compressor': compressor} for v in
                         list(set(ds_oc.data_vars.keys())) + list(ds_oc._coord_names)}
            ds_oc.to_zarr(fname, encoding=encodings)
        else:
            ds_oc.to_zarr(fname, mode='a', append_dim='time',
                          consolidated=False, compute=True)


def get_unique_times(source_directory):
    file_infos = [get_info(fname) for fname in glob(join(source_directory, '*.SEN3'))]
    file_infos = sorted(file_infos, key=lambda x: x['start_time'])  # sort by start time
    times = [(info['start_time'], info['stop_time']) for info in file_infos]  # get all the start times
    return list(set(times))  # uniques times


def process():
    for timestamp, local_direc in s3.swaths('ocean_color'):
        # swaths = list(swaths)  # this will transfer the files to local machine
        files = find_files_and_readers(base_dir=local_direc,
                                       reader=['olci_l1b', 'olci_l2'])
        # fname_out = files[list(files.keys())[0]][0].split('/')[-2].replace('.SEN3', '__{name}.tif')

        # del files['olci_l1b']
        scn = Scene(filenames=files)
        # composites = ['true_color', 'ocean_color', 'true_color_marine_clean', 'true_color_marine_tropical',
        #               'true_color_raw']
        composites = ['true_color_marine_clean', ]
        try:
            scn.load(composites + ['wqsf'])
            # scn.load(['mask'])
            print('Using mask')
        except KeyError:
            print('Mask not found')
            scn.load(composites)
        # scn.show(composite)
        newscn = scn.resample('euron1')
        del scn
        # try:
        #     for composite in composites:
        #         # newscn[composite] = newscn[composite].apply_mask(['CLOUD', 'CLOUD_AMBIGUOUS', 'CLOUD_MARGIN'])
        #         save_dataset(newscn, composite, filename=s3.product_name.replace('.tif', '.zarr'), format='zarr')
        # except KeyError:
        #     raise
        #     pass
        file_out = io.BytesIO()
        newscn.save_datasets(writer='geotiff', filename=file_out)
        file_out.seek(0)
        s3.upload_file(file_out, location='product')


base_dir = '/data/sentinel3/download/'
# base_dir = '/data/sentinel3/s3a_network/'

if __name__ == '__main__':
    args = docopt(__doc__)

    if args['--all']:
        fnames = sorted(fn.split('/')[-1] for fn in glob(join(base_dir, '*')))
        start_d = datetime.strptime(fnames[0], '%Y%m%d')
        stop_d = datetime.strptime(fnames[-1], '%Y%m%d')
    elif args['--date']:
        start_d = datetime.strptime(args['--date'], '%Y%m%d')
        stop_d = start_d
    elif args['<startdate>'] and args['<stopdate>']:
        pass  # TODO: parse dates
    elif args['--last_days']:
        last_days = int(args['--last_days'])
        stop_d = datetime.combine(date.today(), datetime.min.time())
        start_d = stop_d - timedelta(days=last_days)
    else:
        stop_d = datetime.combine(date.today(), datetime.min.time())
        start_d = stop_d - timedelta(days=1)
    s3 = AwsS3('olci-oceancolour', 'quick', start_d, stop_d)
    process()

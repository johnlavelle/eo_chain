"""
Subset Sentinel 3 data and save in Zarr format

Usage:
  subset_s3 <input_directory> <save_directory>
  subset_s3 (-h | --help)

Options:
  -h --help     Show this screen.

"""

from os.path import join
import read_sentinel3
from docopt import docopt


lon_cv, lat_cv = -6.07, 53.293
buffer = 0.05


def main(source_directory, save_directory):
    for ds in read_sentinel3.dataset(join(source_directory, '*.SEN3')):

        ds_reduce = ds.where((ds.lon > lon_cv - buffer) &
                             (ds.lon < lon_cv + buffer) &
                             (ds.lat > lat_cv - buffer) &
                             (ds.lat < lat_cv + buffer))

        ds_reduce = ds_reduce.dropna(dim='rows', how='all').dropna(dim='columns', how='all')
        ds_reduce.to_zarr(join(save_directory, ds.attrs['product_name'] + '.zarr'))


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args['<input_directory>'], args['<save_directory>'])

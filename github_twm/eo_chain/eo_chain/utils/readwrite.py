from os.path import join
from itertools import groupby
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
from functools import lru_cache
from utils.misc import get_info
from datetime import timedelta
from glob import glob
import shutil
import re


class IO:
    def __init__(self, bucketname, product_level='source'):
        raise NotImplemented

    def get_unprocessed_swaths(self, start_date, end_date, suffix):
        raise NotImplemented


class AwsS3(IO):
    def __init__(self, bucketname, name, start_date, end_date, product_level='source'):
        self.bucketname = bucketname
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.client_resource = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self._product_level = product_level
        self.product_name, self.source_name = None, None
        self.base_direc = '/tmp'


    def get_timestamp(self, direc):
        pattern = '.*(\d{8}T\d{4}).*'
        return re.match(pattern, direc).group(1)

    def add_prefix(self, swath_date, product_level):
        prefix = join(product_level,
                      str(swath_date.year),
                      str(swath_date.month).zfill(2),
                      str(swath_date.day).zfill(2))
        return prefix

    def get_product_name(self, source_direc, timestamp):
        return join(source_direc.replace('source', 'product'), f"{self.bucketname}_{self.name}_{timestamp.strftime('%Y%m%dT%H%M')}.tif")


    def get_swath_filenames(self):
        for swath_date in self.date_range(self.start_date, self.end_date, False):
            prefix = self.add_prefix(swath_date, 'source')
            keys = (obj.key for obj in
                    self.resource.Bucket('olci-oceancolour').objects.filter(Prefix=prefix).all())
            for timestamp, keys_iter in groupby(keys, self.get_timestamp):
                yield datetime.strptime(timestamp, '%Y%m%dT%H%M%S'), keys_iter

    def swaths(self, suffix):
        """Get swath names for which no product exists"""
        for timestamp, keys in self.get_swath_filenames():
            source_direc = self.add_prefix(timestamp, 'source')
            local_direc = join(self.base_direc, source_direc)
            self.product_name = self.get_product_name(source_direc, timestamp)

            product_direc = os.path.dirname(self.product_name)
            existing_prods = self.list_products(product_direc)
            if self.product_name not in existing_prods:
                # transfer to local file system
                # Remove existing directories otherwise satpy would process
                # these
                for dirname in glob('/tmp/source/*/*/*'):
                    shutil.rmtree(dirname, ignore_errors=False, onerror=None)

                os.makedirs(local_direc)

                for k in keys:
                    local_name = join(self.base_direc, k)
                    self.download(k, local_name)
                yield timestamp, local_direc

    def download(self, objectname, filename):
        if not os.path.exists(filename):
            print(self.bucketname, objectname, filename)
            if not os.path.isdir(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            try:
                self.client.download_file(self.bucketname, objectname, filename)
            except FileNotFoundError:
                print(f'Cannot download: {objectname} to {filename}')

    @lru_cache(maxsize=1)
    def list_products(self, prod_prefix):
        return list(self.resource.Bucket(self.bucketname).objects.filter(
               Prefix=prod_prefix).all())

    def list(self):
        response = self.client.list_buckets()
        for bucket in response['Buckets']:
            yield bucket["Name"]

    def upload_file(self, local_fname, location):
        upload_paths = {'product': self.product_name,
                        'source': self.source_name}
        try:
            self.client.upload_file(local_fname,
                                    self.bucketname,
                                    upload_paths[location])
        except ClientError as e:
            print(e)
            # logging.error(e)
            return False

    def upload_directory(self, direc, location):
        print('uploading')
        for root, dirs, files in os.walk(direc):
            for file in files:
                obj_dir = join(self._product_level, '/'.join(root.split('/')[3:]))
                self.source_name = join(obj_dir, file)
                self.upload_file(join(root, file), location)

    @staticmethod
    def date_range(start_date, end_date, return_string=True):
        date_out = start_date
        while date_out <= end_date:
            print(date_out)
            if return_string:
                yield date_out.strftime('%Y%m%d')
            else:
                yield date_out
            date_out += timedelta(days=1)


class Local(IO):
    pass


if __name__ == '__main__':
    s3 = AwsS3('olci-oceancolour')
    print(next(s3.get_unprocessed_swaths_grouped(datetime(2020, 2, 5), datetime(2020, 2, 6), '_ocean_colour1.tif')))
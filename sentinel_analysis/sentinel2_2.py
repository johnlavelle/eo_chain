#!/usr/bin/env python
# coding: utf-8

# In[144]:


import boto3
from botocore.client import Config
from itertools import groupby
import pandas as pd
import pylab as plt
import datetime

from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.cloud import bigquery


# In[140]:


kwargs = dict(region_name="europe-west2",
              endpoint_url="https://storage.googleapis.com",
              aws_access_key_id='GOOG1EIW767D4F2XW5CY3A6MBP7MY6WQ2YPYYILOWFCHAXCDBF72JOJU7SSXI',
              aws_secret_access_key='hbXjofmyTTW5b9+kU8Kx+N5a1gexGDddm0kpxtz2',
              config=Config(signature_version='s3v4'))
bucketname = 'gcp-public-data-sentinel-2'
resource = boto3.resource('s3', **kwargs)
client = boto3.client('s3', **kwargs)


# In[141]:



# Construct a BigQuery client object.
client = bigquery.Client()

def get_object(tile, cloud_cover, start_day, end_day):
    query = f"""
    WITH files AS (
      SELECT
      base_url AS product, mgrs_tile, CAST(SUBSTR(sensing_time, 1, 10) AS date) AS day, CAST(cloud_cover AS float64) AS cloud_cover
      FROM
        bigquery-public-data.cloud_storage_geo_index.sentinel_2_index
    )
    SELECT 
      *    
    FROM
      files
    WHERE
      cloud_cover >= {cloud_cover}
    AND 
      day >= CAST('{start_day}' AS date)
    AND 
      day <= CAST('{end_day}' AS date)
    AND
     mgrs_tile = "{tile}"
    ORDER BY
     day
    LIMIT
      10000
    """
    query_job = client.query(query)  # Make an API request.
    for row in query_job:
        # Row values can be accessed by field name or index.
        prod = row['product'].split('gs://gcp-public-data-sentinel-2/')[1]
        yield {'name': prod, 'clouds': row['cloud_cover'], 'day': row['day']}


# In[142]:


prods = list(get_object('29UPV', 0, '2015-01-01', '2021-01-01'))
df = pd.DataFrame(prods)


# In[143]:


df['clouds'].plot.hist(bins=30,normed=True, cumulative=False)
plt.grid()
plt.show()
df['clouds'].plot.hist(bins=30,normed=True, cumulative=True)
plt.grid()


# In[135]:


res = df['day'] - df['day'].shift(1)
res = res.dropna().dt.days
res[res < 10].plot.hist(bins=range(10))


# In[147]:


def download(objectname, filename):        
    if not (os.path.exists(filename) or '$folder$' in objectname):
        # print(self.bucketname, objectname, filename)
        if not os.path.isdir(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        try:
            client.download_file(bucket, objectname, filename)
        except FileNotFoundError:
            print(f'Cannot download: {objectname} to {filename}')


bb = resource.Bucket('gcp-public-data-sentinel-2').objects.filter(Prefix='tiles/29/U/PV/S2B_MSIL1C_20190310T115359_N0207_R023_T29UPV_20190310T184948.SAFE')
for b in bb.all():
    print(b)
    
    
bucket = 'gcp-public-data-sentinel-2'
for prod in prods():
    bb = resource.Bucket(bucket).objects.filter(Prefix=prod['name']).all()
    for b in bb:
        download(b.key, join('/tmp', b.key))


# In[ ]:





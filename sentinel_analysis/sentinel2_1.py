#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery
import boto3
from os.path import join
import os
import satpy
from satpy import Scene, find_files_and_readers


# In[8]:


# TODO(developer): Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the destination table.
# table_id = "your-project.your_dataset.your_table_name"

sql = """
WITH sent2 as
  (SELECT 
    base_url, mgrs_tile, PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E6S", SUBSTR(sensing_time, 1, 26)) AS time, cloud_cover
  FROM 
    bigquery-public-data.cloud_storage_geo_index.sentinel_2_index)
SELECT * from sent2
WHERE 
  mgrs_tile = '29UPV'
AND
  time > TIMESTAMP('2017-11-05 00:00:00') 
LIMIT 3
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql)  # Make an API request.
query_job.result()  # Wait for the job to complete.


# In[9]:


kwargs = dict(region_name="europe-west2",
              endpoint_url="https://storage.googleapis.com",
              aws_access_key_id='GOOG1EIW767D4F2XW5CY3A6MBP7MY6WQ2YPYYILOWFCHAXCDBF72JOJU7SSXI',
              aws_secret_access_key='hbXjofmyTTW5b9+kU8Kx+N5a1gexGDddm0kpxtz2')
resource = boto3.resource('s3', **kwargs)
client = boto3.client('s3', **kwargs)
bucket = 'gcp-public-data-sentinel-2'


# In[2]:


def download(objectname, filename):        
    if not (os.path.exists(filename) or '$folder$' in objectname):
        # print(self.bucketname, objectname, filename)
        if not os.path.isdir(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        try:
            client.download_file(bucket, objectname, filename)
        except FileNotFoundError:
            print(f'Cannot download: {objectname} to {filename}')


# In[11]:


def tiles():
    for r in query_job.result():
        tile = r['base_url'].split('gs://gcp-public-data-sentinel-2/')[1]
        yield tile


# In[12]:


next(tiles())


# In[55]:


for tile in tiles():
    bb = resource.Bucket(bucket).objects.filter(Prefix=tile).all()
    for b in bb:
        download(b.key, join('/tmp', b.key))


# In[49]:


conda install -c conda-forge satpy


# In[3]:


files = find_files_and_readers(base_dir='/tmp/tiles/29/U/PV/20180506/', reader='msi_safe')
scn = Scene(filenames=files)


# In[4]:


scn


# In[5]:


composite = 'true_color'
scn.load([composite])
scn.save_datasets(datasets=[composite,],writer='geotiff', filename='test.tif')
#scn.show(composite)


# In[17]:


scn.crop()


# In[20]:


import sen2cor


# In[1]:


get_ipython().system('ls -lh')


# In[ ]:





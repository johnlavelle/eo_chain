import pandas as pd
from pyproj import Transformer
import geopandas as gpd
import tools
import numpy as np


info = tools.get_info()

df = pd.read_csv("/data/seaweed/sbir_seaweed_survey_20210114/gnss_raw.csv", header=None, names=['short_name', 'x', 'y', 'q'])
df.drop(df.tail(1).index, inplace=True)  # Drop last row
df = df.iloc[::-1]
del df['q']

exclude_rows = np.concatenate([df[df['short_name'] == c].index.values for c in info["exclude_coordinate"]])
df.drop(exclude_rows, inplace=True)

df['species'] = df['short_name'].apply(lambda name: tools.get_species_name(name, info)[1])
df['comment'] = df['short_name'].apply(lambda k: info['comments'].get(k, ""))

fname_shp = '/data/seaweed/sbir_seaweed_survey_20210114/shapefile/seaweed.shp'

transformer = Transformer.from_crs(2157, 4326)
lat, lon = transformer.transform(df.x.values, df.y.values)
df['lon'], df['lat'] = lon, lat

df = df[['short_name', 'species', 'x', 'y', 'lon', 'lat', 'comment']]

names = pd.DataFrame(info['names']).T

gdf = gpd.GeoDataFrame(
    df, geometry=gpd.points_from_xy(lon, lat))
gdf.to_file(fname_shp)

del df['geometry']
with pd.ExcelWriter(fname_shp.replace('shapefile/seaweed.shp', 'gnss.xlsx')) as writer:
    df.to_excel(writer, sheet_name='coordinates', index=False)
    names.to_excel(writer, sheet_name='names', index=True)
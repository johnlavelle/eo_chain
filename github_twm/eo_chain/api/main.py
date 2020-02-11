import json
from fastapi import FastAPI
import xarray as xr

json.encoder.FLOAT_REPR = lambda o: format(o, '.2f')

app = FastAPI()

ds = xr.open_zarr('/data/sentinel3/out/true_color_marine_clean.zarr')


def get_vals(variable, band, x, y):
    return json.dumps(list(ds[variable].sel(bands=band).sel(x=x, y=y,
                                                                     method='nearest').values))


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/data/variable={variable}%x={x}%y={y}")
def read_item(variable: str, x: float, y: float):
    return {band: get_vals(variable, band, x, y) for band in ['R', 'G', 'B']}

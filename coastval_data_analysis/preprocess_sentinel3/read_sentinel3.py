
import xarray as xr
from glob import glob
from os.path import join, exists
import scipy.io as sio
import re


data_sent_srf = sio.loadmat('../data/Sentinel3_SRF.mat')
wavelengths_centre = data_sent_srf['SRFF'].ravel()


def dataset(direc):
    for d in glob(direc):
        try:
            fs = glob(join(d, '*_reflectance.nc'))
            fnames = ['chl_nn.nc', 'geo_coordinates.nc', 'wqsf.nc', 'time_coordinates.nc', 'w_aer.nc', 'par.nc',
                      'trsp.nc', 'iwv.nc']
            for f in fnames:
                path = join(d, f)
                if exists(path):
                    fs.append(path)
            ds = xr.open_mfdataset(fs)
            ds = ds.assign_coords(lon=ds.longitude, lat=ds.latitude)
            for name in ds.data_vars.keys():
                try:
                    band = int(re.match('Oa(\d\d)_reflectance.*', name).group(1))
                    ds[name].attrs['wavelength'] = wavelengths_centre[band - 1]
                except AttributeError:
                    pass
            yield ds
        except AttributeError:
            print(f"{d}")
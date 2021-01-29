"""
Subset Sentinel 3 data and save in Zarr format

Usage:
  subset_s3 <input_directory> <save_directory>
  subset_s3 (-h | --help)

Options:
  -h --help     Show this screen.

"""

import xarray as xr
from os.path import basename, join
from pyproj import transform
from sklearn.neighbors import NearestNeighbors
from glob import glob
from docopt import docopt


lon_cv, lat_cv = -6.07, 53.293
x_cv, y_cv = transform(4326, 32629, lon_cv, lat_cv)


def load_dataset(fname):
    """
    load and convert to z = x, y index
    """

    ds = xr.open_zarr(fname)
    # ds = ds.isel(time=0)
    ds_z = ds.stack(z=['x', 'y'])

    x, y = transform(4326, 32629, ds_z.lon.values, ds_z.lat.values)
    ds_z = ds_z.assign_coords(xx=xr.DataArray(x, dims='z'),
                              yy=xr.DataArray(y, dims='z'))
    return ds_z


def get_nearest_neighbors(ds):
    coords_xy = list(zip(ds.xx.values, ds.yy.values))
    # Get central pixel
    nbrs = NearestNeighbors(n_neighbors=1, algorithm='auto').fit(coords_xy)
    distance, index = nbrs.kneighbors([[x_cv, y_cv]])
    x_pixel, y_pixel = ds.isel(z=index[0]).xx.values[0], ds.isel(z=index[0]).yy.values[0]

    nbrs = NearestNeighbors(n_neighbors=100, algorithm='auto').fit(coords_xy)
    distances, indices = nbrs.kneighbors([[x_pixel, y_pixel]])

    ds_s = ds.isel(z=indices[0])
    return ds_s.assign_coords(distance=xr.DataArray(distances[0], dims='z'))


def reorder_ds(ds):

    s3_bands = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 21]

    def get_wavelength_da(suffix):
        for b in s3_bands:
            da = ds[f'Oa{str(b).zfill(2)}{suffix}']
            try:
                wavelength = da.attrs['wavelength']
            except KeyError:
                wavelength = None
            yield wavelength, da

    def get_dataarray(var_name):
        wavelengths, das = list(zip(*get_wavelength_da(var_name)))
        ds_refl = xr.concat(das, "band")
        try:
            ds_refl['wavelength'] = xr.DataArray(list(wavelengths), dims='band')
        except:
            pass
        return ds_refl.sortby('distance')

    da_refl = get_dataarray('_reflectance')
    da_refl_err = get_dataarray('_reflectance_err')
    da_refl_satpy = get_dataarray('')
    da_refl_satpy['wavelength'] = da_refl['wavelength']

    ds2 = xr.merge([da_refl, da_refl_err, da_refl_satpy, ds], compat='override')

    das_other = {k: v for k, v in ds2.items() if not k.startswith('Oa')}
    ds2 = xr.Dataset({
                    'reflectivity': da_refl,
                    'reflectivity_satpy': da_refl_satpy,
                    'reflectivity_err': da_refl_err, **das_other})
    ds2.attrs = ds.attrs
    return ds2


def main(source_directory, save_directory):
    for fname in glob(join(source_directory, '*.zarr')):
        try:
            dsz = load_dataset(fname)
            ds_s = get_nearest_neighbors(dsz)
            ds2 = reorder_ds(ds_s)
            ds2.unstack().to_zarr(join(save_directory, basename(fname)))
        except Exception as exc:
            print(exc)


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args['<input_directory>'], args['<save_directory>'])



# ref = ds2['reflectivity'].isel(z=slice(0, 1)).mean(dim='z')
# ref_err = ds2['reflectivity_err'].isel(z=slice(0, 1)).mean(dim='z')
# plt.errorbar(ref.wavelength, ref, ref_err)
# plt.grid()
# plt.show()
#
# for i in range(10):
#     ds2['reflectivity'].isel(z=i).plot(x='wavelength')
# plt.grid()
# plt.show()
#
#
# ds_t = ds2.isel(z=slice(0, 100))
# plt.axis('equal')
# plt.scatter(ds_t.x, ds_t.x.y)
# plt.scatter(x_cv, y_cv)
# plt.show()




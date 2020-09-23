import xarray as xr
import numpy as np

# @xr.register_dataset_accessor("geo")
# class GeoAccessor:
#     def __init__(self, xarray_obj):
#         self._obj = xarray_obj
#         self._center = None
#
#     @property
#     def center(self):
#         """Return the geographic center point of this dataset."""
#         if self._center is None:
#             # we can use a cache on our accessor objects, because accessors
#             # themselves are cached on instances that access them.
#             lon = self._obj.latitude
#             lat = self._obj.longitude
#             self._center = (float(lon.mean()), float(lat.mean()))
#         return self._center
#
#     def plot(self):
#         """Plot data on a map."""
#         return "plotting!"

@xr.register_dataarray_accessor("geo")
class GeoAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        self._center = None

    @property
    def center(self):
        """Return the geographic center point of this dataset."""
        return self._obj.mean() - self._offset

    @center.setter
    def center(self, value):
        self._offset = value


da = xr.DataArray(np.random.randn(2, 3), dims=('x', 'y'), coords={'x': [10, 20]})

da.geo.center = 10
print(da.geo.center)

# @xr.register_dataset_accessor("mask")
# class Mask:
#     def __init__(self, xarray_obj):
#         self._obj = xarray_obj
#         self._center = None
#
#     def getbitmask(self, wqsf, items=None):
#         """Get the bitmask."""
#         if items is None:
#             items = ["INVALID", "SNOW_ICE", "INLAND_WATER", "SUSPECT",
#                      "AC_FAIL", "CLOUD", "HISOLZEN", "OCNN_FAIL",
#                      "CLOUD_MARGIN", "CLOUD_AMBIGUOUS", "LOWRW", "LAND"]
#         bflags = BitFlags(wqsf)
#         return reduce(np.logical_or, [bflags[item] for item in items])


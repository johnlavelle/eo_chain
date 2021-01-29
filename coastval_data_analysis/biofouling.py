from coastval_spectrum import get_data, compute_spectra
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.son import SON
import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pylab as plt


client = MongoClient()
client = MongoClient('localhost', 27017)
db = client['coastval_data_analysis-db']

spec_names_required = ['SATHPL0452', 'SATHPL0609', 'SATHSE0612', 'SATHPE0332', 'SATHPE0610']


def sampled_specs(instrument):
    result = db.radiances.aggregate([
        {'$match': {'instrument': instrument}},
        {'$sample': {'size': 2000}},
        {"$group":
            {
             "_id": '$_id',
            'start_time': {'$push': '$start_time'},
            'light_data': {'$push': '$light_data'}
            }
        },
    ], allowDiskUse=True)
    return result


res = db.radiances.aggregate([
    {'$match': {'start_time': {'$gte': datetime(2019, 8, 1), '$lte': datetime(2019, 11, 1)}}},
    {"$project": {
         "year": {"$year": "$start_time"},
         "month": {"$month": "$start_time"},
         "day": {"$dayOfMonth": "$start_time"}
    }
    },
    {"$group": {
        "_id": None,
        "distinctDate": {"$addToSet": {"year": "$year", "month": "$month", "day": "$day"}}
    }}
], allowDiskUse=True)

for r in res:
    print(r)

# X_specs = {i: np.vstack([d['light_data'] for d in sampled_specs(i)]) for i in spec_names_required}
#
# print(X_specs['SATHPE0610'].shape)
# print(X_specs['SATHPL0452'].shape)
#
#
# pca = PCA(n_components=6)
# pca.fit(X_specs['SATHPE0332'])
# plt.plot(np.cumsum(pca.explained_variance_ratio_))
# plt.show()
#
# for comp in pca.components_:
#     plt.plot(comp)
# plt.show()

# def specs():
#     i = 0
#     for res in sampled_times():
#         t_start = res['start_time'][0]
#         dfs = list(get_data(t_start, t_start + timedelta(seconds=10)))
#         spec_names = [r[0] for r in dfs]
#         if all((n in spec_names) for n in spec_names_required):
#             yield {k: d['light'].iloc[0] for k, d in dfs}
#             i += 1
#             if i > 100:
#                 break
#
#
# X_specs = {k: np.vstack([s[k].values for s in specs()]) for k in spec_names_required}
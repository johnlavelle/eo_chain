from pymongo import MongoClient
from datetime import timedelta


def get_tilt_stats(db):
    for rad in db.radiances.find():
        result = db.tilt.aggregate([
            # {"$sort": {"timestamp": 1}},
          {"$match":
            {"timestamp": {
                "$gte": rad['start_time'],
                "$lt": rad['end_time']}}
          },
          {"$group":
             {
             "_id": rad['_id'],
             # "roll_mean": {"$avg": "$roll"},
             # "pitch_mean": { "$avg": "$pitch"},
             # "heading_mean": {"$avg": "$pitch"},
             "roll_max": {"$max": {'$abs': "$roll"}},
             "pitch_max": {"$max": {'$abs': "$pitch"}},
             "heading": {'$first': "$heading"},
             # "roll_min": {"$min": {'$abs': "$roll"}},
             # "pitch_min": {"$min": {'$abs': "$pitch"}},
             # "heading_min": {"$min": {'$abs': "$pitch"}},
             # "roll_std": {"$stdDevSamp": "$roll"},
             # "pitch_std": {"$stdDevSamp": "$pitch"},
             # "heading_std": { "$stdDevSamp": "$pitch"}
             }},
        # {"$out": 'tilt_stats'}
        ])
        try:
            yield next(result)
        except StopIteration:
            pass


def get_environment_stats(db):
    for rad in db.radiances.find():
        result = db.environment.aggregate([
            # {"$sort": {"timestamp": 1}},
            {"$match":
                {"timestamp": {
                    "$gte": rad['start_time'] - timedelta(seconds=2),
                    "$lt": rad['end_time'] + timedelta(seconds=2)}}
            },
            {"$group":
                {
                    "_id": rad['_id'],
                    "temp_mean": {"$avg": "$temp"},
                    "pressure_mean": {"$avg": "$pressure"},
                    "temp_std": {"$stdDevSamp": "$temp"},
                    "pressure_std": {"$stdDevSamp": "$pressure"},
                }},
        ])
        try:
            yield next(result)
        except StopIteration:
            pass


if __name__ == '__main__':
    client = MongoClient()
    client = MongoClient('localhost', 27017)
    db = client['coastval_data_analysis-db']

    # # db.tilt_stats.drop()
    # db.environment_stats.drop()
    #
    # print('Creating Indexes')
    # db.tilt.create_index([("timestamp", ASCENDING)], name='ts_index')
    # db.environment.create_index([("timestamp", ASCENDING)], name='ts_index')
    #
    # print('Computing stats for     tilt')
    # for t in get_tilt_stats(db):
    #     post_id = db.tilt_stats.insert_one(t).inserted_id
    #
    # print('Computing stats for environment')
    # for t in get_environment_stats(db):
    #     post_id = db.environment_stats.insert_one(t).inserted_id


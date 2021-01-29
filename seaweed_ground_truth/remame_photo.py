from glob import glob
import json
from os.path import join, dirname
from shutil import copyfile
import pandas as pd
from datetime import datetime
from os import mkdir
import re
import tools

SRC = '/data/seaweed/sbir_seaweed_survey_20210114/photos_org/'
info = tools.get_info()


def rename_file(fname_new):
    m = re.match('([A-Z]*)([0-9]*)', fname_new)
    short_name, idx = m.group(1), m.group(2)
    long_name, colloquial_name = info['names'][short_name]

    timestamp = datetime.fromtimestamp(int(data['photoTakenTime']['timestamp']))
    timestamp_str = timestamp.strftime("%Y%m%dT%H%M%S")
    name = long_name.replace(" ", "")
    full_path_new = join(dirname(full_path).replace('photos_org', 'photos'), f"{long_name.replace(' ', '')}{idx}.jpg")
    try:
        mkdir(dirname(full_path_new))
    except OSError:
        pass
    copyfile(full_path, full_path_new)
    return full_path_new, timestamp


times = []
names = []
comments = []
for full_path in glob(join(SRC,  '*.MP.jpg')):
    try:
        with open(full_path + '.json') as json_file:
            data = json.load(json_file)
        fname_short = data['description'].rstrip().replace(' ', '_').replace('/', '').upper()
        if not fname_short:
            continue
        if 'BAD' in fname_short:
            continue
        if fname_short in info["exclude_photo"]:
            continue
        print(fname_short)
        full_path_new, timestamp = rename_file(fname_short)
        names.append(fname_short)
        times.append(timestamp)
        try:
            comments.append(info["comments"][fname_short])
        except KeyError:
            comments.append('')
    except:
        pass


df = pd.DataFrame({'name': names, 'comment': comments}, index=times)
df.to_csv(join(SRC, 'name_times.csv'))




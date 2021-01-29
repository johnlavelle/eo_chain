import re
from os import getcwd
from os.path import join
import json


def get_info():
    cwd = getcwd()
    with open(join(cwd, 'info.json')) as json_file:
        info = json.load(json_file)
    return info


def get_species_name(fname_new, info):
    short_name = re.match('([A-Z]*)', fname_new).group(0)
    long_name, colloquial_name = info['names'][short_name]
    return short_name, long_name, colloquial_name
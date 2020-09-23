#!/usr/bin/python3

"""oceancolour-s2

Usage:
    oceancolour-s2.py <filename>

Example:
    Process a single file:
        python oceancolour-s2.py data/S2A_MSIL1C_20200106T114451_N0208_R123_T29UPV_20200106T120826.SAFE
    Process all the .SAFE files in the data directory:
        python oceancolour-s2.py 'data/*.SAFE'

"""

import os
import subprocess
from os.path import join, exists
import shutil
from docopt import docopt
from glob import glob


gpt = '/opt/snap/bin/gpt'
pconvert = '/opt/snap/bin/pconvert'
SRC_DIR = '/data/sentinel2/'

def get_image_name(fname_s2):
    return fname_s2.replace('.SAFE', '.tif').replace('.zip', '.tif')


def process_file(fname_s2):
    # SRC_S2 = join(SRC_DIR, fname_s2)

    print(os.path.basename(fname_s2))
    cwd = join('/tmp/', os.path.basename(fname_s2))
    try:
        os.mkdir(cwd)
    except FileExistsError:
        pass
    os.chdir(cwd)

    try:
        # subprocess.run([gpt, '/home/jlavelle/code/github_twm/eo_chain_snap/Sentinel2-Resample-Idepix-Subset.xml',
        #                 '-SsourceProduct=' + fname_s2]).check_returncode()
        subprocess.run([gpt, 'S2Resampling', '-SsourceProduct=' + fname_s2]).check_returncode()
        # subprocess.run([gpt, 'Idepix.S2', '-SsourceProduct=' + join('target.dim')]).check_returncode()
        subprocess.run([gpt, 'c2rcc.msi', '-SsourceProduct=' + join('target.dim')]).check_returncode()

        subprocess.run([pconvert, '-b', '4,3,2', '-f', 'tif', 'target.dim']).check_returncode()
        tif_name = join(SRC_DIR, get_image_name(fname_s2))
        shutil.copyfile('target.tif', tif_name)
        shutil.rmtree(cwd)
        print(f'Created {tif_name}.')
    except Exception as excep:
        print(excep)


if __name__ == '__main__':
    args = docopt(__doc__)
    for fn in glob(args['<filename>']):
        if exists(get_image_name(fn)):
            continue
        process_file(fn)
        # process_file(args['<filename>'])

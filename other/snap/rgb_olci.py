import subprocess
from glob import glob
import re
from os.path import join

prods = glob('/data/S3/source/*/*/*/S3A_OL_2_WFR*')

for prod in prods:
    process = subprocess.Popen(['pconvert',
                                '-f', 'tif',
                                '-p', 'rgbprofiles/olci_tristimulus.txt',
                                prod,
                                '-o', '/data/sentinel3/out/'],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    yyyymmdd = re.match('.*(\d{4}/\d{2}/\d{2}).*', prods[0]).group(1)


stdout, stderr = process.communicate()
stdout, stderr
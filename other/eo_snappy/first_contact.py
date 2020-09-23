from snappy import ProductIO
import numpy as np
# import matplotlib.pyplot as plt

p = ProductIO.readProduct('/home/ubuntu/code/github/snap-engine/snap-python/src/main/resources/snappy/testdata/MER_FRS_L1B_SUBSET.dim')
rad13 = p.getBand('radiance_13')
w = rad13.getRasterWidth()
h = rad13.getRasterHeight()
rad13_data = np.zeros(w * h, np.float32)
rad13.readPixels(0, 0, w, h, rad13_data)
p.dispose()
rad13_data.shape = h, w
# imgplot = plt.imshow(rad13_data)
# imgplot.write_png('radiance_13.png')
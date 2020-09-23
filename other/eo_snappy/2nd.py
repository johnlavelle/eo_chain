from snappy import ProductIO

fname = '/home/ubuntu/code/github/snap-engine/snap-python/src/main/resources/snappy/testdata/MER_FRS_L1B_SUBSET.dim'   # read product

p = ProductIO.readProduct(fname)
print(list(p.getBandNames()))
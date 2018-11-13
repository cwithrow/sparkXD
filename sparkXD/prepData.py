import os
import sys
import numpy as np
from matplotlib import pyplot as plt
# %matplotlib inline
# to load data as dataframes
from sparkXD.spark import get_spark
from sparkXD.prep_methods import load


from pyspark.sql import functions as f


# paths
path = "data/"

noisypath= os.path.join(path, "raw/DR7_raw.csv")
stdpath = os.path.join(path,"std/stripe82calibStars_v2.6.dat.gz")



# Load raw/noisy data (previously downloaded from SDSS) and correct for extinction


sqlContext = get_spark()

# Load noisy, raw DR7 data as csv into a dataframe

df_noisy = load.load_raw(noisypath, sqlContext)

print ("Size of df_noisy: %s" % (load.sizeconv(sys.getsizeof(df_noisy))))


# Show sample of data
df_noisy.show(2)

# Select stars/unresolved light sources from the sample data
df_noisy = df_noisy.filter(df_noisy.type == 6)
df_noisy.registerTempTable("noisy")

print ("Shape of df_noisy: %d rows" % (df_noisy.count()))

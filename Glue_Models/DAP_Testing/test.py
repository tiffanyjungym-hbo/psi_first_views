#########################################
### IMPORT LIBRARIES AND SET VARIABLES
#########################################

#Import python modules
#from lib import sample
from datetime import datetime

#Import pyspark modules
#from pyspark.context import SparkContext
#import pyspark.sql.functions as f

#Import glue modules
#from awsglue.utils import getResolvedOptions
#from awsglue.context import GlueContext
#from awsglue.dynamicframe import DynamicFrame
#from awsglue.job import Job
import sys
import os

print("working dir:" +str(os.getcwd()))
print(os.listdir())
#print(os.walkpath
import glob
print(glob.glob("/tmp/*.py"))

print("testing")

script = os.path.realpath(__file__)
print("SCript path:", script)


for file in glob.glob("*.*"):
    print(file)

from lib.sample import run
from lib import sample1
print(sample1)
print(help(sample1))
run()

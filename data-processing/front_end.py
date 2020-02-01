#import gzip
import os

from pyspark import SparkContext, SparkConf
#from pyspark.sql import SQLContext
from dictionaries import getIndexes

# prompt user for input
start = input("Enter start month and year (e.g. January 2019): ")
end = input("Enter end month and year: ")

# retrieve a list of paths to index files
indexList = getIndexes(start,end)
#print(pathList[0])

# organize the input files according to time
conf = SparkConf().setAppName('ingestion')
sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
#rdd = sc.textFile(pathList[0])
for index in indexList:
    archives = sc.textFile(index).collect()
#d = rdd.take(5)
#print(rdd.count())
    for archive in archives:
#    with gzip.open(path,'rt') as lines:
#    print()
#    for line in lines:
        time = ""
        current_time = archive.split('/')[5].split('-')[2]
        if current_time != time:
            time = current_time
            os.makedirs(os.getcwd()+'/input/temp', exist_ok=True)
            file = open('input/temp/'+time+'_wet.txt','a+')
        file.write('s3://commoncrawl/'+archive+'\n')
#       print('got line', line)
    file.close()

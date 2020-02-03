#import gzip
import os

from pyspark import SparkContext, SparkConf
#from pyspark.sql import SQLContext
from dictionaries import getIndexes

# constants
submit_str = '$SPARK_HOME/bin/spark-submit --driver-class-path ~/postgresql-42.2.9.jar --master local[*] ./word_count.py '
cleanup_str = 'rm -rf spark-warehouse/'

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
os.makedirs(os.getcwd()+'/input/temp', exist_ok=True)
submit_file = open('submit.sh','w+')
cleanup_file = open('cleanup.sh','a+')
submit_file.write('#!/bin/sh'+'\n')
cleanup_file.write('#!/bin/sh'+'\n')
cleanup_file.write('rm submit.sh'+'\n')
cleanup_file.write('rm -rf input/temp'+'\n')
time = ""
for index in indexList:
    archives = sc.textFile(index).collect()
#d = rdd.take(5)
#print(rdd.count())
    for archive in archives:
#    with gzip.open(path,'rt') as lines:
#    print()
#    for line in lines:
        current_time = archive.split('/')[5].split('-')[2]
        if current_time != time:
            time = current_time
            input_file = open('input/temp/'+time+'_wet.txt','a+')
            submit_file.write(submit_str+'./input/temp/'+time+'_wet.txt o'+time+'\n')
            cleanup_file.write(cleanup_str+'o'+time+'\n')
        input_file.write('s3://commoncrawl/'+archive+'\n')
#       print('got line', line)

input_file.close()
submit_file.close()
cleanup_file.close()
os.popen('chmod 755 submit.sh')
os.popen('chmod 755 cleanup.sh')

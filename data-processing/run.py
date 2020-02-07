import os

from pyspark import SparkContext, SparkConf
from indexes import getIndexes

# constants
submit_str = '$SPARK_HOME/bin/spark-submit --py-files sparkcc.py --jars ~/postgresql-42.2.9.jar --master spark://ec2-44-226-24-33.us-west-2.compute.amazonaws.com:7077 ./word_count.py hdfs://ec2-44-226-24-33.us-west-2.compute.amazonaws.com:9000/'
#cleanup_str = 'hdfs dfs -rm -R /input'

# configuration to read from s3
conf = SparkConf().setAppName('ingestion')
sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# open files and write headers
submit_file = open('submit.sh','w+')
cleanup_file = open('cleanup.sh','w+')
submit_file.write('#!/bin/sh'+'\n')
cleanup_file.write('#!/bin/sh'+'\n')
cleanup_file.write('rm submit.sh'+'\n')
cleanup_file.write('rm -rf input'+'\n')
cleanup_file.write('hdfs dfs -rm -r /input'+'\n')

os.popen('hdfs dfs -mkdir /input')
#cleanup_file.write('rm -rf input/temp'+'\n')

# prompt user for input
start = input("Enter start month and year (e.g. January 2019): ")
end = input("Enter end month and year: ")

# retrieve a list of paths to index files
indexList = getIndexes(start,end)

# Loop to read the index files and create input files from their content
os.makedirs(os.getcwd()+'/input', exist_ok=True)
time = ""
for index in indexList:
    archives = sc.textFile(index).collect()
    for archive in archives:
        current_time = archive.split('/')[5].split('-')[2]
        if current_time != time:
            time = current_time
            filename = 'input/'+time+'_wet.txt'
            input_file = open(filename,'a+')
            input_file.write('s3://commoncrawl/'+archive+'\n')
#           os.popen('hdfs dfs -copyFromLocal '+filename+' /'+filename)
            submit_file.write(submit_str+filename+' o'+time+'\n')
#           cleanup_file.write(cleanup_str+'o'+time+'\n')
        #input_file.write('s3://commoncrawl/'+archive+'\n')

input_file.close()
submit_file.close()
os.popen('hdfs dfs -copyFromLocal $PWD/input /')
cleanup_file.close()

os.popen('chmod 755 submit.sh')
os.popen('chmod 755 cleanup.sh')

# Terminal command to execute the job
#os.popen('./submit.sh')

# START.PY: This script creates the input files and a shell script to submit the spark job.

import os

from pyspark import SparkContext, SparkConf
from indexes import getIndexes

# Configuration to read from S3
conf = SparkConf().setAppName('run')
sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Write header for the submit file and construct the "submit string"
submit_file = open('submit.sh','w+')
submit_file.write('#!/bin/sh\n')
submit_file.write('start=$(date +%s)\n')

# Create the necessary directories locally and on HDFS
os.makedirs(os.getcwd()+'/input', exist_ok=True)
os.popen('hdfs dfs -mkdir /input')

# Prompt the user for input
pdns = input("Enter the master node Public DNS: ")
key = input("Enter the search term: ")
start = input("Enter start month and year (e.g. January 2019): ")
end = input("Enter end month and year: ")

# Construct the "submit string"
submit_str = '$SPARK_HOME/bin/spark-submit --py-files sparkcc.py --jars ~/postgresql-42.2.9.jar --master spark://'+pdns+':7077 ./word_count.py hdfs://'+pdns+':9000/'

# Retrieve a list of paths to index files based on the user request
indexList = getIndexes(start,end)

# Loop to read the index files and create input files from their content
time = ""
for index in indexList:
    parity_count = 0
    archives = sc.textFile(index).collect()
    for archive in archives:
        current_time = archive.split('/')[5].split('-')[2]
        if current_time != time:
            time = current_time
            filename = 'input/'+time+'_wet.txt'
            input_file = open(filename,'a+')
            input_file.write('s3://commoncrawl/'+archive+'\n')

            # Run an equal nunber of timestamped inputs from the submit file, as set by parity_count (for testing)
            if parity_count < 5:
                submit_file.write(submit_str+filename+' o'+time+' '+key+'\n')
                parity_count += 1
            else:
                submit_file.write('#'+submit_str+filename+' o'+time+' '+key+'\n')
        # ATTENTION: the next line makes the difference between a test case and a full job
        #input_file.write('s3://commoncrawl/'+archive+'\n')

# Additional commands to record the runtime of the job
submit_file.write('end=$(date +%s)\n')
submit_file.write('runtime=$((end-start))\n')
submit_file.write('echo "Execution time was $runtime seconds"\n')

# Copy the input files to HDFS
os.popen('hdfs dfs -copyFromLocal $PWD/input /')

# Close the open file objects
input_file.close()
submit_file.close()

# Create a cleanup file
cleanup_file = open('cleanup.sh','w+')
cleanup_file.write('#!/bin/sh\n')
cleanup_file.write('rm submit.sh\n')
cleanup_file.write('rm -rf input\n')
cleanup_file.write('hdfs dfs -rm -r /input\n')
cleanup_file.write('rm -rf spark-warehouse\n')
cleanup_file.close()

# Change permissions on the submit and cleanup files to make them executable
os.popen('chmod 755 submit.sh')
os.popen('chmod 755 cleanup.sh')

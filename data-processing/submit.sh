#!/bin/sh
$SPARK_HOME/bin/spark-submit --py-files sparkcc.py,process_output.py --driver-class-path ~/postgresql-42.2.9.jar --master spark://44.231.205.255:7077 ./word_count.py hdfs://44.231.205.255:9000/user/20170322212946_wet.txt o20170322212946
#$SPARK_HOME/bin/spark-submit --driver-class-path ~/postgresql-42.2.9.jar --master local[*] ./word_count.py ./input/20170322212946_wet.txt o20170322212946
#$SPARK_HOME/bin/spark-submit --driver-class-path ~/postgresql-42.2.9.jar --master local[*] ./word_count.py ./input/20170322212947_wet.txt o20170322212947

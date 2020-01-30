#!/bin/sh
# This is a comment!
$SPARK_HOME/bin/spark-submit --driver-class-path ~/postgresql-42.2.9.jar --master local[*] ./word_count.py ./input/test_wet.txt wordfreq

import os
import pandas as pd
import matplotlib.pyplot as plt
from pylab import *
from pyspark.sql import SparkSession

DBURL = os.environ['DB_URL']
DBPORT = os.environ['DB_PORT']
DBUSER = os.environ['DB_UN']
DBPASS = os.environ['DB_PW']
JDBCDRIVER = os.environ['JDBC_DRIVER']

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", JDBCDRIVER) \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option('url', 'jdbc:postgresql://'+DBURL+':'+DBPORT+'/'+DBUSER) \
    .option('dbtable', 'wordfreqtbl') \
    .option('user', DBUSER) \
    .option('password', DBPASS) \
    .option('driver', 'org.postgresql.Driver') \
    .load()

#df[['time', 'df']].plot()
pdf=df.toPandas()
pdf.plot(kind='bar',x='time',y='df')
axes = figure().add_subplot(111)
a=axes.get_xticks().tolist()
#ax2 = plt.subplot(111)
#ax2.margins(2, 2)
#plt.figure(figsize=(20,20))
plt.savefig('zelda_02_04_2017.png')
#plt.show()

#    .option("url", "jdbc:postgresql://"+DB_URL+":"+DB_PORT+/databasename") \
#    .option("dbtable", "tablename") \
#    .option("user", "username") \
#    .option("password", "password") \
#    .option("driver", "org.postgresql.Driver") \
#    .load()

#rdf.write.mode('append') \
#                .format('jdbc') \
#                .option('url', 'jdbc:postgresql://'+DBURL+':'+DBPORT+'/'+DBUSER) \
#                .option('dbtable', 'wordfreqtbl') \
#                .option('user', DBUSER) \
#                .option('password', DBPASS) \
#                .option('driver', 'org.postgresql.Driver') \
#                .save()

#df.printSchema()
#df.show()

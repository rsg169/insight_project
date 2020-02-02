#from pyspark import SparkContext
#from pyspark.sql import SQLContext, SparkSession

import os

from pyspark.sql.functions import lit
#import psycopg2
#from db_config import config

def write_to_db(self, sqlc):
        #df = sqlc.read.parquet("spark-warehouse/"+self.args.output)
        #df.printSchema()
        # restructure the dataframe
        #rdf = df.select("key", "val.tf", "val.df")

        try:
                DBURL = os.environ['DB_URL']
                DBPORT = os.environ['DB_PORT']
                DBUSER = os.environ['DB_UN']
                DBPASS = os.environ['DB_PW']
                DRIVERPATH = os.environ['JDBC_DRIVER']

        except:
                print('Missing credentials. Please set environment variables appropriately.')
                exit()

        df = sqlc.read.parquet("spark-warehouse/"+self.args.output)
        #df.printSchema()
        # restructure the dataframe
        #print(self.args.search)

        time = self.args.input.split('/')[2].split('_')[0]
        rdf = df.select("key", "val.tf", "val.df").withColumn("time",lit(time))
        # Connect to the PostgreSQL database server
        #conn = None

        try:
                # read connection parameters
                #params = config()

                # connect to the PostgreSQL server
                #print('Connecting to the PostgreSQL database...')
                #conn = psycopg2.connect(**params)

                # create a cursor
                #cur = conn.cursor()

                # execute a statement
                #print('PostgreSQL database version:')
                #cur.execute('SELECT version()')

                # display the PostgreSQL database server version
                #db_version = cur.fetchone()
                #print(db_version)

                rdf.write.mode('append') \
                .format('jdbc') \
                .option('url', 'jdbc:postgresql://'+DBURL+':'+DBPORT+'/'+DBUSER) \
                .option('dbtable', 'wordfreqtbl') \
                .option('user', DBUSER) \
                .option('password', DBPASS) \
                .option('driver', 'org.postgresql.Driver') \
                .save()

                # df.write.mode('append') \
                #.format('jdbc') \
                #.option('url', 'jdbc:postgresql://'postgresql-db.cgxod5cfbc2h.us-west-2.rds.amazonaws.com:5432/postgres') \
                #.option('dbtable', 'wordfreqtbl') \
                #.option('user', DBUSER) \
                #.option('password', DBPASS) \
                #.option('driver', 'org.postgresql.Driver') \
                #.save()

                #   .option('user', postgres) \
                #   .option('password', pwd) \

                # close the communication with the PostgreSQL
                #cur.close()
        #except(Exception, psycopg2.DatabaseError) as error:
        except(Exception) as error:
                print(error)
        #finally:
                #if conn is not None:
                   #conn.close()
                   #print('Database connection closed.')

#def run_job(self, sc, sqlc):
#        input_data = sc.textFile
#sc = SparkContext.getOrCreate()
#sqlContext = SQLContext(sc)
#df = sqlContext.read.parquet("spark-warehouse/wordfreq")
#for row in df.sort(df.val.desc()).take(10): print(row)
#df.printSchema()
#df.show()
#df.filter(df.key=='wtol').show()

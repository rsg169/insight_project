import os

from pyspark.sql.functions import lit

def dbWrite(self, sqlc):

        # Retrieve database credentials from local environment
        try:
                DBURL = os.environ['DB_URL']
                DBPORT = os.environ['DB_PORT']
                DBUSER = os.environ['DB_UN']
                DBPASS = os.environ['DB_PW']
                DRIVERPATH = os.environ['JDBC_DRIVER']

        except:
                print('Missing credentials. Please set environment variables appropriately.')
                exit()

        # Read the word count output into a dataframe
        df = sqlc.read.parquet("spark-warehouse/"+self.args.output)

        # Parse the timestamp information and add it to the dataframe
        time = self.args.input.split('/')[2].split('_')[0]
        timestamp = time[0:4]+'-'+time[4:6]+'-'+time[6:8]+' '+time[8:10]+':'+time[10:12]+':'+time[12:14]
        rdf = df.select("key", "val.tf", "val.df").withColumn("time",lit(timestamp))

        # Write the dataframe to the database
        try:
                rdf.write.mode('append') \
                .format('jdbc') \
                .option('url', 'jdbc:postgresql://'+DBURL+':'+DBPORT+'/'+DBUSER) \
                .option('dbtable', 'wordfreqtbl') \
                .option('user', DBUSER) \
                .option('password', DBPASS) \
                .option('driver', 'org.postgresql.Driver') \
                .save()

        except(Exception) as error:
                print(error)

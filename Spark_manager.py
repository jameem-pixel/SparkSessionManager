from pyspark.sql import SparkSession

class SparkSessionManager:
    
    
    def __init__(self, server, database, appname, confiq_mem):
        self.db_credentials = json.load(open(os.path.dirname(__file__) + '/db_credentials.json'))
        self.credential = self.db_credentials[server]
        self.ip_server = self.credential['ip_server']
        self.username = self.credential['username']
        self.password = self.credential['password']
        self.database = database
        self.server = server 
        
        
        self._spark = SparkSession.builder \
        .appName(appname) \
        .config("spark.executor.memory", f"{confiq_mem}G") \
        .config("spark.driver.memory", f"{confiq_mem}G") \
        .config("spark.jars", "/home/factentry/otto_cats/otto_catsentry_process/migration/mssql-jdbc-12.6.0.jre11.jar") \
        .getOrCreate()
        
    def get_spark(cls):
        return cls._spark
    
    def read_table(self,table):

        properties = {
                    "user": self.username,
                    "password": self.password,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "trustServerCertificate": "true"
                }
        url ="jdbc:sqlserver://"+ self.ip_server.replace(',',':') +";databaseName="+self.database
        sdf = self._spark.read.jdbc(url=url, table=table, properties=properties)
        return sdf
    


    def read_csv(self,filepath):
        spark_df = self._spark.read \
        .option("header", "true") \
        .option("delimiter", ",")\
        .csv(filepath)
        return spark_df
    
    def read_parquet(self,filepath):
        spark_df = self._spark.read.parquet(filepath)
        return spark_df

    def write_table(self,spark,mode,table):
        properties = {
                    "user": self.username,
                    "password": self.password,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "trustServerCertificate": "true"
                }
        try:
            spark.write.jdbc(url="jdbc:sqlserver://"+ self.ip_server +";databaseName="+self.database, table=table, mode=mode, properties=properties)
            return True  
        except Exception as e:
            print("Error:", e)
            return False 
        
    
    def write_parquet(self,spark,mode,filepath):
        try:
            spark.write.mode(mode).parquet(filepath)
            return True  
        except Exception as e:
            print("Error:", e)
            return False 
        
    def write_csv(self,spark,mode,filepath):
        try:
            spark.write.mode(mode).parquet(filepath)
            spark.write \
            .mode(mode) \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(filepath)
            return True  
        except Exception as e:
            print("Error:", e)
            return False 
        
    def stop_spark_session(spark):
        if spark._spark is not None:
            spark._spark.stop()
            spark._spark = None
        
        


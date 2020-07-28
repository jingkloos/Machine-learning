
from pyspark.sql.functions import regexp_extract,col
from pyspark.sql import SparkSession
import re

class WebLogParser:
    def __init__(self,spark_session,file_path,*args):
        self.file=file_path
        self.patterns=args
        self.spark=spark_session
        self.log_df=None
    
    def parse_log(self):
        column_name=self.log_df.columns[0]
        for pattern in self.patterns:
            for i in range(len(pattern[1])):
                self.log_df=self.log_df.withColumn(pattern[1][i],regexp_extract(col(column_name),pattern[0],i+1))
        return self.log_df

    def read_data(self):
        try:
            self.log_df=self.spark.read.text(self.file)
            print('Successfully loaded {} rows of data'.format(self.log_df.count()))
            print('Data schema is:')
            print(self.log_df.schema)
        except:
            print('Something went wrong when loading data')


if __name__ =='__main__':
    spark=SparkSession.builder.appName('Web Log Analyzer').master('local[*]').getOrCreate()
    host_pattern=(r'(^\S+\.[\S+\.]+\S+)\s',['host'])
    ts_pattern=(r'\[(\d{2}/\w{3}/\d{4}(:\d{2}){3} -\d{4})\]',['timestamp'])
    request_pattern=(r'\"(\S+)\s(\S+)\s*(\S*)\"',['method','endpoint','protocol'])
    status_pattern=(r'\s(\d{3})\s',['status'])
    content_size_pattern=(r'\s(\d+)$',['content_size'])
    web_log_parser=WebLogParser(spark,'NASA_access_log_Aug95.gz',host_pattern,ts_pattern,request_pattern,status_pattern,content_size_pattern)
    web_log_parser.read_data()
    parsed_log_df=web_log_parser.parse_log()
    error_url_sum_df=parsed_log_df.filter(parsed_log_df['status']!=200).groupBy('endpoint').count().sort('count',ascending=False).limit(10)
    error_url_sum_pdf=error_url_sum_df.toPandas()
    print('**Top 10 endpoints with errors:**')
    print(error_url_sum_pdf)

import requests
from zipfile import ZipFile
import os
from pyspark.sql import SparkSession
from pathlib import Path

class DataLoader:
    def __init__(self,spark_session):
        self.spark=spark_session

    def get_file(self,url,path):
        print('Downloading files using requests module..............')
        try:
            r=requests.get(url)
            with open(path,'wb') as f:
                f.write(r.content)
            print('Download succeed, check {}'.format(path))
        except:
            print('Download failed!')
            raise
    def unzip_file(self,dest_path,src_path):
        print('unzipping file......')
        with ZipFile(src_path,'r') as zip_obj:
            zip_obj.extractall(dest_path)
        print('Unzip succeed')

    def load_to_DF(self,path,ext,*args):
        files=args
        movies_df=None
        ratings_df=None
        dfs={'movies':movies_df,'ratings':ratings_df}
        for file_name in files:
            file_path=os.path.join(path,file_name+ext)
            # print(file_path)
            if os.path.exists(file_path) and file_name in dfs:
                dfs[file_name]=self.spark.read.load(file_path,format=ext[1:],header=True,inferSchema=True)
                # print(dfs[file_name])
        return dfs
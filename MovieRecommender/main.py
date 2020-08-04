import os
import argparse
import time
import gc
from pyspark.sql import SparkSession
from load_data import DataLoader
from pathlib import Path
from als_recommender import ALSRecommender

data_path=Path('data/ml-latest')
data_folder=Path('data/')
raw_file_path=Path('downloads/ml_latest.zip')
spark=SparkSession.builder.appName('Movie Recommender').master('local[*]').getOrCreate()
data_loader=DataLoader(spark)
folders=['data','downloads','best_model','user_recs','archive']
for folder in folders:
    if not os.path.exists(folder):
        os.mkdir(folder)

if not os.path.exists(data_path):
    if not os.path.exists(raw_file_path):
        url='http://files.grouplens.org/datasets/movielens/ml-latest.zip'
        data_loader.get_file(url,raw_file_path)
    data_loader.unzip_file(data_folder,raw_file_path)

dfs=data_loader.load_to_DF(data_path,'.csv','movies','ratings')
ratings_df=dfs['ratings'].drop('timestamp')
movies_df=dfs['movies']
print('ratings_df size: {}\nmovies_df size: {}'.format(ratings_df.count(),movies_df.count()))
als=ALSRecommender(spark,ratings_df,movies_df,'best_model','user_recs','archive')

t0=time.time()
als.refresh_model_recommendations(10,10,0.06,10)
print('Model refresh complete successfully! It took {:.2f}s'.format(time.time()-t0))
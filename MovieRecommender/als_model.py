from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lower
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from zipfile import ZipFile
import os
from os.path import basename
from datetime import datetime
from pathlib import Path
from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np

class ALSRecommender:
    def __init__(self,spark_session,ratings_df,movies_df,model_folder,recs_folder,archive_folder):
        self.spark=spark_session
        self.ratings_df=ratings_df
        self.movies_df=movies_df
        self.als_model=ALS(userCol='userId', itemCol='movieId', ratingCol='rating',coldStartStrategy='drop')
        self.best_model=None
        self.best_params={'rank':-1,'reg':0}
        self.evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
        self.model_folder=model_folder
        self.recs_folder=recs_folder
        self.archive_folder=archive_folder
        self.top_n=10
    
    def tune_model(self,max_iter,ranks,reg_params,dataset,ratios,verbose=False):
        """
        input: 
        ----------
        max_iter: int number of iteration
        ranks: list     ranks to try
        reg_params: list    reg_param to try
        dataset: dataframe  the whole rating dataset
        ratios: list    ratios to split dataset into training, validation and test
        return None
        """
        min_error=float('inf')
        error_list=[]
        training,validation,test=self.ratings_df.randomSplit(ratios,seed=2)
        for rank in ranks:
            for reg in reg_params:
                als=self.als_model.setMaxIter(max_iter).setRank(rank).setRegParam(reg)
                trained_model=als.fit(training)
                predictions=trained_model.transform(validation)
                rmse=self.evaluator.evaluate(predictions)
                error_list.append((rank,reg,rmse))
                if verbose:
                    print('{} latent factors and regularization is {}: '.format(rank,reg))
                    print('RMSE is {}'.format(rmse))
                if rmse<min_error:
                    min_error=rmse
                    self.best_model=trained_model
                    self.best_params['rank']=rank
                    self.best_params['reg']=reg 
        
        test_preds=self.best_model.transform(test)
        test_error=self.evaluator.evaluate(test_preds)
        all_preds=self.best_model.transform(self.ratings_df)
        all_error=self.evaluator.evaluate(all_preds)
        print('**Best model**')
        print('rank is {}\n regularization is {}\n'.format(self.best_params['rank'],self.best_params['reg']))
        print('rmse:')
        print('validation data: {}'.format(min_error))
        print('test data: {}'.format(test_error))
        print('all data: {}'.format(all_error))
    
    def _set_params(self,max_iter,rank,reg):
        self.als_model=self.als_model.setMaxIter(max_iter).setRank(rank).setRegParam(reg)

    def _add_new_ratings(self,ratings):
        """
        add new ratings to ratings_df
        ---------
        input:
        ratings list of list [[userId,movieId, rating]]
        """
        new_pdf=pd.DataFrame(data=ratings,columns=['userId','movieId','rating'])
        new_rating=self.spark.createDataFrame(new_pdf)
        self.ratings_df=self.ratings_df.union(new_rating)
    
    def refresh_model_recommendations(self,max_iter,rank,reg,top_n,new_ratings=[]): 
        '''
        Update model and its recommendations
        --------------------------------------
        input:
        max_iter int
        rank int
        reg int
        top_n int
        movie_ids list
        '''
        
        print('start refreshing als model.........')
        #add new ratings
        if len(new_ratings)>0:
            print('Add new ratings......')
            self._add_new_ratings(new_ratings)
        print('set up parameters......')
        self._set_params(max_iter,rank,reg)
        print('trainning als model..........')
        self.best_model=self.als_model.fit(self.ratings_df)
        print('saving training results..........')
        self._save_model()
        print('generating recomendations for all users and save the results...........')
        self._create_all_recommendations(top_n)
        

    def _save_model(self):
        # path=Path(self.model_folder+'/')
        if self.best_model:
            if len(os.listdir(self.model_folder))>0:
                self._archive_model('model')
            self.best_model.write().overwrite().save(self.model_folder)

    def _load_model(self):
        try:
            # path=Path(self.model_folder+'/')
            self.best_model=ALSModel.load(self.model_folder)
            print('Model load succeed.')
        except:
            print('Model load failed. You can re-train the model.')
            raise
    
    def _archive_model(self,file_type):
        dt_str=datetime.now().strftime('%Y%m%d%H%M%S')
        # archive_path=Path(self.archive_folder)
        if file_type=='model':
            archive_file='best_model_'+dt_str+'.zip'
            dir_name = self.model_folder
        elif file_type=='recs':
            archive_file='user_recs_'+dt_str+'.zip'
            dir_name = self.recs_folder
        else:
            print('Wrong file type!')
            return
        with ZipFile(os.path.join(self.archive_folder,archive_file),'w') as zip_obj:
            for folder_name,sub_folders,file_names in os.walk(dir_name):
                for file_name in file_names:
                    file_path=os.path.join(folder_name,file_name)
                    zip_obj.write(file_path,basename(file_path))
    
    def recommend_by_user(self,user_id):
        user_recs=self._read_user_recs()
        user_recs_pdf=user_recs.filter(user_recs.userId==user_id).toPandas()
        rec_movies=[]

        for item in user_recs_pdf.values[0][1]:
            movie_id=item[0]
            score=item[1]
            rec_movies.append((movie_id,score))
        recs_pdf=pd.DataFrame(rec_movies,columns=['movieId','score'])
        movies_pdf=self.movies_df.toPandas()
        recs_pdf=recs_pdf.merge(movies_pdf,on='movieId')
        
        user_movies=self.ratings_df.filter(ratings_df.userId==user_id).orderBy('rating',ascending=False).limit(5)
        user_movies_pdf=user_movies.join(self.movies_df,on='movieId').toPandas()   
        print('User {} has rated the following movies highly:'.format(user_id))
        print(user_movies_pdf)
        print('Recommended movies for user {}:'.format(user_id))
        print(recs_pdf)
        return recs_pdf

    def recommend_by_movie(self,fav_movie,top_n):
        similar_movies=self._find_similar_movies(fav_movie,top_n)
        movie=similar_movies.iloc[0].title
        recs_pdf=similar_movies.iloc[1:]
        print('People who like movie {} also like these:'.format(movie))
        print(recs_pdf)
        return recs_pdf


    def _find_similar_movies(self,fav_movie,top_n):
        if not self.best_model:
            self._load_model()
        movie_factors_pdf=self.best_model.itemFactors.toPandas()
        movies_pdf=self.movies_df.toPandas()
        movie_id=self._fuzzy_match(fav_movie,fuzzy_ratio=70,verbose=False)
        try:
            target_id_feature=movie_factors_pdf.loc[movie_factors_pdf.id==movie_id].features.to_numpy()[0]
        except:
            return 'There is no movie with id '+str(movie_id)
        similarities=[]
        
        for feature in movie_factors_pdf['features'].to_numpy():
            similarity=np.dot(target_id_feature,feature)/(np.linalg.norm(target_id_feature)*np.linalg.norm(feature))
            similarities.append(similarity)

        similarity_pdf=pd.DataFrame({'similarity':similarities},index=movie_factors_pdf.id.to_numpy())
        top_movies=similarity_pdf.sort_values(by=['similarity'],ascending=False).head(top_n+1)
        movie_recs_pdf=top_movies.merge(movies_pdf,left_index=True,right_on='movieId',how='inner')
        movie_recs_pdf.sort_values(by=['similarity'],ascending=False,inplace=True)
        movie_recs_pdf.reset_index(inplace=True)
        return movie_recs_pdf

    def _create_all_recommendations(self,top_n):
        # path=str(Path(self.recs_folder))
        if self.best_model:
            if len(os.listdir(self.recs_folder))>0:
                self._archive_model('recs')
            user_recs=self.best_model.recommendForAllUsers(top_n)
            user_recs.write.mode('overwrite').save(self.recs_folder)
        else:
            print('Model doesn''t exist. Please train/load a model')
    
    def _read_user_recs(self):
        # path=str(Path(self.recs_folder))
        user_recs=self.spark.read.load(self.recs_folder)
        return user_recs
    
    def _fuzzy_match(self,fav_movie,fuzzy_ratio=50,verbose=True):
        """
        Output: list []
        the closest match movie idx based on fuzzy ratio, If no match found, return None
        ---------
        Input:
        movie_mapper: dict {title:index}
        fav_movie: string favorite movie title
        fuzzy_ratio: float indicate how much you want the title match
        verbose: bool print log message if true
        """
        match_list=[]
        movies_pdf=self.movies_df.toPandas()
        movie_mapper={movie['title']:movie['movieId'] for _,movie in movies_pdf.iterrows()}
        for title,idx in movie_mapper.items():
            ratio=fuzz.ratio(title.lower(),fav_movie.lower())
            if ratio>=fuzzy_ratio and len(match_list)<10:
                match_list.append((title,idx,ratio))
        #sort match_list by ratio
        match_list=sorted(match_list,key=lambda x:x[2],reverse=True)
        
        if not match_list:
            print('No match is found!')
            return
        if verbose:
            print('Found possible matches in our database: {}\n'.format([x[0] for x in match_list]))
        return match_list[0][1]


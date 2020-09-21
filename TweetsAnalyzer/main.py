from pathlib import Path
import os
from tweet_analyzer import TweetAnalyzer
from twitter_streamer import TwitterStreamer
import logging
import sys
import pandas as pd
import numpy as np
import warnings

logging.basicConfig(filename='log',level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings('ignore')

if not os.path.exists('data'):
    os.makedirs('data')

if not os.path.exists('model'):
    os.makedirs('model')

output_file=Path('data/tweets.json')
if not os.path.exists(output_file) or sys.argv[1] == 'Y':
    logging.info('Start streaming tweets from twitter.........')
    filter_list=[]
    time_limit=int(sys.argv[2])
    for i in range(3,len(sys.argv)):
        filter_list.append(sys.argv[i])
    streamer=TwitterStreamer()
    streamer.stream_tweets(output_file=output_file,filter_list=filter_list,time_limit=time_limit)
    logging.info('Done streaming tweets from twitter.........')

logging.info('Start analyzing tweets.........')
df_tweets=pd.read_json(output_file,lines=True)
tweet_analyzer=TweetAnalyzer()
tweet_analyzer.get_df(df_tweets)

logging.info('Cleaning tweets.........')
tweets_df=tweet_analyzer.clean_df()
print(tweets_df.head())
print('Loaded {} rows of tweets'.format(tweets_df.shape[0]))

logging.info('Getting sentiment statistics.........')
sentiment_df=tweet_analyzer.get_sentiment_statics()
print(sentiment_df.head())

logging.info('Classifying tweets...........')
tweet_topics_df=tweet_analyzer.classify_tweets(5,5)
print(tweet_topics_df.head())

logging.info('Done analyzing tweets.........')


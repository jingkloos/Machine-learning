from json import loads
from kafka import KafkaConsumer
from time import sleep
import datetime
import pandas as pd
from tweet_analyzer import TweetAnalyzer
import joblib
import numpy as np
import warnings
warnings.filterwarnings('ignore')


'''
use pretrained lda model to cluster tweets and calculate its sentiment
'''

if __name__ == '__main__':
    consumer = KafkaConsumer('tweets3',
        bootstrap_servers = 'localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=500,
        group_id=None,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
    # print(consumer.beginning_offsets())
    tweet_analyzer=TweetAnalyzer()
    sentiments={'1':0,'0':0,'-1':0}
    topics_df=pd.read_csv('model\\topics.csv')
    topics={'topic0':0,'topic1':0,'topic2':0,'topic3':0,'topic4':0,'topic5':0}
    lda=joblib.load('model\lda_model')
    vecterizer=joblib.load('model\\tfidf_model')

    for msg in consumer:
        msg_df=loads(msg.value)
        words=tweet_analyzer.tokenization_and_stemming(msg_df['text'])
        clean_text=' '.join(words)
        sentiment=tweet_analyzer.get_sentiment(clean_text)
        sentiments[str(sentiment)]+=1
        vec=vecterizer.transform([clean_text])
        output=lda.transform(vec)
        output_topic=np.argmax(output,axis=1)[0]
        topics['topic'+str(output_topic)]+=1
        print(sentiments,topics)
        sleep(1)
        
    



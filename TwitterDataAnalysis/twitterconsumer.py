from json import loads,dumps
from kafka import KafkaConsumer,TopicPartition
from twitteranalyzer import TweetAnalyzer
from time import sleep
import datetime

# class HashtagCollector():
#     @classmethod
#     def aggregate_hashtags(cls,consumer,hashtags):
#         for message in consumer:
#             message=message.value
#             message_dict=loads(message)
#             if len(message_dict['entities']['hashtags'])>0:

#                 for hashtag in message_dict['entities']['hashtags']:
#                     hashtag_value=hashtag['text']
#                     if hashtag_value in hashtags:
#                         hashtags[hashtag_value]+=1
#                     else:
#                         hashtags[hashtag_value]=1
#         return hashtags

if __name__ == '__main__':
    consumer = KafkaConsumer('tweets',
        bootstrap_servers = ['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        auto_commit_interval_ms=500,
        # group_id='test-consumer-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
    hashtags={}
    for message in consumer:
        message=message.value
        # message_dict=loads(message)
        # if len(message_dict['entities']['hashtags'])>0:
        #     for hashtag in message_dict['entities']['hashtags']:
        #         hashtag_value=hashtag['text'].lower()
        #         if hashtag_value in hashtags:
        #             hashtags[hashtag_value]+=1
        #         else:
        #             hashtags[hashtag_value]=1
        print(message)

    
    # hashtags=HashtagCollector().aggregate_hashtags(consumer,hashtags)
    


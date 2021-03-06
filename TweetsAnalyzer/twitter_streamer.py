from tweepy.streaming import StreamListener
from twitter_authenticator import TwitterAuthenticator
from tweepy import Stream
import datetime
from json import dumps
from kafka import KafkaProducer
import sys




#    This class is to authorize twitter api and stream data from twitter

class TwitterStreamer():
    def __init__(self):
        self.twitter_authenticator=TwitterAuthenticator()

    def stream_tweets(self,filter_list=None,producer=None,time_limit=10,output_file=None):
        listener=TwitterListener(producer,time_limit,output_file)
        auth=self.twitter_authenticator.get_authentication()
        stream=Stream(auth,listener)
        if filter_list:
            stream.filter(track=filter_list,languages=['en'])
        else:
            stream.sample(languages=['en'])


#    This class is twitter stream listener

class TwitterListener(StreamListener):

    def __init__(self,producer,time_limit,output_file):
        self.output_file=output_file
        self.time_limit=time_limit
        self.start_time=datetime.datetime.now()
        self.producer=producer

    def on_data(self, raw_data):
        try:
            if datetime.datetime.now()<self.start_time+datetime.timedelta(seconds=self.time_limit):
                if self.output_file:
                    with open(self.output_file,'a') as f:
                        f.write(raw_data)
                if self.producer:
                    self.producer.send('tweets3',value=raw_data)
                return True
            else:
                return False
        except BaseException as e:
            print("Error on data: %s" % str(e))
            return True

    def on_error(self, status_code):
        # end the listener if stream limit is reached
        if status_code == 420:
            return False
        print(status_code)

if __name__=='__main__':
    output_file='tweets.json'
    filter_list=[]
    time_limit=10
    if len(sys.argv)>1:
        time_limit=int(sys.argv[1])
        for i in range(2,len(sys.argv)):
            filter_list.append(sys.argv[i])
    
    producer=KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x:dumps(x).encode('utf-8'))
    streamer=TwitterStreamer()
    streamer.stream_tweets(producer=producer,filter_list=filter_list,time_limit=time_limit)
    
from tweepy.streaming import StreamListener
from twitterauthenticator import TwitterAuthenticator
from tweepy import Stream
import datetime
# from kafka import KafkaProducer
from json import dumps
import sys




#    This class is to authorize twitter api and stream data from twitter

class TwitterStreamer():
    def __init__(self):
        self.twitter_authenticator=TwitterAuthenticator()

    def stream_tweets(self,filterlist=None,outputfile=None,timelimit=10,producer=None):
        listener=TwitterListener(outputfile,timelimit,producer)
        auth=self.twitter_authenticator.get_authentication()
        stream=Stream(auth,listener)
        if filterlist:
            stream.filter(track=filterlist)


#    This class is twitter stream listener

class TwitterListener(StreamListener):

    def __init__(self,outputfile,timelimit,producer):
        self.outputfile=outputfile
        self.timelimit=timelimit
        self.starttime=datetime.datetime.now()
        self.producer=producer

    def on_data(self, raw_data):
        try:
            if datetime.datetime.now()<self.starttime+datetime.timedelta(seconds=self.timelimit):
                if self.outputfile:
                    with open(self.outputfile,'a') as f:
                        f.write(raw_data)
                if self.producer:
                    self.producer.send('tweets',value=raw_data)
                    # print('raw_data is sent')

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
    filterlist=['covid']
    outputfile='tweets_'+ datetime.datetime.now().strftime('%Y%m%d%H%M%S') +'.json'
    
    # producer=KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))
    timelimit=int(sys.argv[1])
    streamer=TwitterStreamer()
    streamer.stream_tweets(outputfile=outputfile,filterlist=filterlist,timelimit=timelimit)
    
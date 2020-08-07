from twitterclient import TwitterClient
import pandas as pd




class TweetAnalyzer():
    
    @classmethod
    def to_dataframe(cls,tweets):
        df = pd.DataFrame()
        try:
            df['id']=[tweet['id'] for tweet in tweets]
            df['text']=[tweet['text'] for tweet in tweets]
            df['text_length']=[len(tweet['text']) for tweet in tweets]
            df['likes']=[tweet['favorite_count'] for tweet in tweets]
            df['retweets']=[tweet['retweet_count'] for tweet in tweets]
            df['source']=[tweet['source'] for tweet in tweets]
            df['created_at']=[tweet['created_at'] for tweet in tweets]
            df['user']=[tweet['user']['name'] for tweet in tweets]
            df['hashtags']=[tweet['entities']['hashtags'] for tweet in tweets]
            return df
        except BaseException as e:
            print('Input is not in a valid format, input must be a list of dictionary.')   
            return df


if __name__ == '__main__':
    twitter_client=TwitterClient('Vikings')
    api=twitter_client.client

    tweets=api.user_timeline(id=twitter_client.user, count=5)
    tweets=[tweet._json for tweet in tweets]
    df=TweetAnalyzer.to_dataframe(tweets)
    
    print(df.head())
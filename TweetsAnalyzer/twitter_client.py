from tweepy import API
from tweepy import Cursor
from twitter_authenticator import TwitterAuthenticator

#    This class is to create a twitter client
class TwitterClient():
    def __init__(self,twitter_user=None):
        self.auth=TwitterAuthenticator().get_authentication()
        self.client=API(self.auth)
        self.user=twitter_user

    



if __name__ =='__main__':
    twitter_api=TwitterClient()
    api=twitter_api.client
    user=twitter_api.user

    friends=api.friends(screen_name=user,count=1)
    hometweets=api.home_timeline(count=10)
    for tweet in hometweets:
        print(tweet.text)


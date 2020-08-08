
from tweepy import OAuthHandler
import twitter_tokens
#    This class is to do authentication wherever needed ###
class TwitterAuthenticator():
    
    def get_authentication(self):
        auth=OAuthHandler(twitter_tokens.CONSUMER_KEY,twitter_tokens.CONSUMER_SECRET)
        auth.set_access_token(twitter_tokens.ACCESS_TOKEN,twitter_tokens.ACCESS_SECRET)
        return auth



from tweepy import OAuthHandler
import twittertokens
#    This class is to do authentication wherever needed ###
class TwitterAuthenticator():
    
    def get_authentication(self):
        auth=OAuthHandler(twittertokens.CONSUMER_KEY,twittertokens.CONSUMER_SECRET)
        auth.set_access_token(twittertokens.ACCESS_TOKEN,twittertokens.ACCESS_SECRET)
        return auth


from twitter_client import TwitterClient
import pandas as pd
import logging 
import numpy as np
import nltk
import re
from nltk.stem.snowball import SnowballStemmer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from textblob import TextBlob


nltk.download('punkt',quiet=True)
nltk.download('stopwords',quiet=True)    


'''LDA model'''
class TweetAnalyzer():
    def __init__(self,df_tweets):
        self.df=df_tweets
        self.model=None
        self.vectorizer=None
        
    def clean_df(self):
        '''
        This process is to clean the dataframe
        output:
        A list of text from tweets df
        '''
        
        logging.info('drop duplicates')
        self.df.dropna(subset=['id'],inplace=True)
        logging.info('pick certain columns and filter out retweeted tweets and non english tweets')
        cols=['created_at','text','user','retweeted','reply_count','retweet_count','favorite_count','lang']
        self.df=self.df[cols]
        self.df=self.df[(self.df.lang=='en') & (self.df.retweeted == False)]
        logging.info('clean text and add sentiment')
        self.df['clean_text']=np.array([' '.join(self.tokenization_and_stemming(text)) for text in self.df['text'].tolist()])
        self.df['sentiment']=np.array([self.get_sentiment(text) for text in self.df['clean_text']])
        return self.df

    def get_sentiment(self,text):
        '''
        calculate sentiment of a tweet
        input:
        text: string 
        output:
        sentiment int
        1 -- positive
        0 -- neutral
        -1 -- negative
        '''
        analysis=TextBlob(text)
        sentiment=analysis.sentiment.polarity 
        if sentiment > 0:
            return 1
        elif sentiment==0:
            return 0
        else:
            return -1

    def get_sentiment_statics(self):
        sentiment_map={1:'positive',0:'neutral',-1:'negative'}
        sentiment_counts=self.df['sentiment'].value_counts()
        sentiment_counts.index=[sentiment_map[i] for i in sentiment_counts.index]
        sentiment_df=pd.DataFrame(sentiment_counts).reset_index().rename(columns={'index':'sentiment','sentiment':'count'})
        sentiment_df['pct']=np.round(100*sentiment_df['count']/sentiment_df['count'].sum(),2)
        return sentiment_df

    # tokenization and stemming
    def tokenization_and_stemming(self,text):
        # Use nltk's English stopwords.
        stopwords = nltk.corpus.stopwords.words('english')
        extra_sw=["'s","'m","n't","br","http","https","rt"]
        stopwords+=extra_sw
        stemmer = SnowballStemmer("english")
        tokens = []
        # exclude stop words and tokenize the document, generate a list of string 
        for word in nltk.word_tokenize(text):
            if word.lower() not in stopwords:
                tokens.append(word.lower())

        filtered_tokens = []
        
        # filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)
        for token in tokens:
            if re.search('[a-zA-Z]', token) and not re.search('^//',token):
                filtered_tokens.append(token)      
        # stemming find the root word
        stems = [stemmer.stem(t) for t in filtered_tokens]
        return stems

    def classify_tweets(self,n_topics,n_words):
        data=self.df.loc[:,'text'].tolist()
        self.model=LatentDirichletAllocation(n_components=n_topics)
        self.vectorizer = CountVectorizer(max_df=0.99, max_features=1000,
                                min_df=0.01, stop_words='english',
                                tokenizer=self.tokenization_and_stemming, ngram_range=(1,1))
        tfidf_matrix = self.vectorizer.fit_transform(data) #fit the vectorizer to synopses
        lda_output=self.model.fit_transform(tfidf_matrix)
        topic_keywords = self._get_topic_words(n_words)        
        df_topic_words = pd.DataFrame(topic_keywords)
        df_topics=pd.DataFrame(np.argmax(lda_output,axis=1),columns=['topic'])
        df_topic_words=df_topic_words.join(df_topics['topic'].value_counts())
        df_topic_words.columns = ['Word '+str(i) for i in range(df_topic_words.shape[1]-1)]+['topic_count']
        df_topic_words.index = ['Topic '+str(i) for i in range(df_topic_words.shape[0])]
        return df_topic_words
        

    # get top n keywords for each topic
    def _get_topic_words(self, n_words):
        words = np.array(self.vectorizer.get_feature_names())
        topic_words = []
        # for each topic, we have words weight
        for topic_words_weights in self.model.components_:
            top_words = topic_words_weights.argsort()[::-1][:n_words]
            topic_words.append(words.take(top_words))
        return topic_words

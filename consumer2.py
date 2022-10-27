# -*- coding: utf-8 -*-
"""
Created on Mon Jul 12 12:38:11 2021

@author: Abdelhaq Guartet
"""
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from textblob import TextBlob
import json
import re
from  nltk.tokenize import word_tokenize
import string
tweets={}
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def text_lowercase(text):
    return text.lower()
def clean_tweet(tweet):
    
    return ''.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z\t])|(\w+:\/\/\S+)","", tweet).split())
#analyse du sentiment 
def tokenize(text):
    text = word_tokenize(text)
    return text
def extract_hashtags(text):
      
    # initializing hashtag_list variable
    hashtag_list = []      
    # splitting the text into words
    for word in text.split():          
        # checking the first charcter of every word
        if word[0] == '#':              
            # adding the word to the hashtag_list
            hashtag_list.append('#'+ word[1:])
     
    return hashtag_list
def analyze_sentiment(tweet):
    text_clean = "".join([i for i in tweet_text if i not in string.punctuation])
    analysis = TextBlob(text_clean)
    
    
    if analysis.sentiment.polarity > 0:
        return "Positive"
    elif analysis.sentiment.polarity <0:
        return "Negative"
    else:
        return "Neutre"

consumer =KafkaConsumer("mytopic",bootstrap_servers='[localhost:9092]',auto_offset_reset='latest',
     enable_auto_commit=True,
     auto_commit_interval_ms =  5000,
     fetch_max_bytes = 128,
     max_poll_records = 100)
for message in consumer:
    #test=text_lowercase(message.value)
    
    tweet=json.loads(message.value)
    tweet_date=tweet["created_at"]
    tweet_user=tweet["user"]["screen_name"]
    tweet_text=tweet["text"]
    sentiment=analyze_sentiment(tweet_text)
    hashtag=extract_hashtags(tweet_text)
    netoyer= "".join([i for i in tweet_text if i not in string.punctuation])
    mot_frequent=tokenize(netoyer)
    print(netoyer)
    #print (tweet.sentiment.polarity)
    #tweet_sentiment=dosentiment(tweet)
  
    es.index(index="index_se",
                 doc_type="test-type",
                 body={"author": tweet_user,
                       "date":tweet_date,
                       "message": tweet_text,
                       "sentiment1": sentiment,
                       "hashtag":hashtag,
                       "localisation":tweet["user"]["location"],
                       "mots_frequents" :mot_frequent})
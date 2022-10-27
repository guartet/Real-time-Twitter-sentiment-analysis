
import time
import inflect

import string
import nltk
import collections


from nltk.tokenize import WordPunctTokenizer

nltk.download('stopwords')
#nltk.download('punkt')
#nltk.download('wordnet')
from nltk.corpus import stopwords # used for preprocessing
#from nltk.stem import WordNetLemmatizer # used for preprocessing
#from nltk.stem import PorterStemmer
#from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize

from kafka import KafkaConsumer
import json
from textblob import TextBlob
import re
import numpy as np 
from elasticsearch import Elasticsearch

topic_name = 'mytopic'


# create instance of elasticsearch
es = Elasticsearch()


consumer = KafkaConsumer(
     topic_name,
     
     bootstrap_servers=['localhost'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     auto_commit_interval_ms =  5000,
     fetch_max_bytes = 128,
     max_poll_records = 100,
     
     )
def clean_tweet(tweet):
    
    return ''.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z\t])|(\w+:\/\/\S+)","", tweet).split())


#analyse du sentiment 

def analyze_sentiment(tweet):
    
    analysis = TextBlob(clean_tweet(tweet))
    
    
    if analysis.sentiment.polarity > 0:
        
        return "Positive"
    elif analysis.sentiment.polarity == 0:
        return "Neutre"
    else:
        return "Negative"

"""
def Convert(string):
    li = list(string.split(";"))
    return li

text = "hello world hello hello hajar imane soukayna soukayna hajar hajar hey faculte faculte"
texte = Convert(text)
word = collections.Counter(texte)
print(word.most_common(3))
"""


def Convert(string):
    li = list(string.split(","))
    return li

# function to print all the hashtags in a text

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
    # printing the hashtag_list
    """
    print("The hashtags in \"" + text + "\" are :")
    for hashtag in hashtag_list:
        print(hashtag)"""

def freq(stri):
  
    # break the string into list of words 
    stri = stri.split()         
    str2 = []
    nb=[]
    # loop till string values present in list str
    for i in stri:             
  
        # checking for the duplicacy
        if i not in str2:
  
            # insert value in str2
            str2.append(i) 
              
    for i in range(0, len(str2)):
  
        # count the frequency of each word(present 
        # in str2) in str and print
        #print('Frequency of', str2[i], 'is :', str.count(str2[i])) 
        nb.append(stri.count(str2[i]))
    l=max(nb)
    intl=nb.index(l)
    return intl
    #☺print('Frequency of', str2[intl], 'is :', str.count(str2[intl]))


def tokenize(text):
    text = word_tokenize(text)
    return text


# make all text lowercase
def text_lowercase(text):
    return text.lower()



"""def processing(text):
    word_punct_token = WordPunctTokenizer().tokenize(str(text))

    clean_token=[]
    for token in word_punct_token:
        
        token = token.lower()
    # remove any value that are not alphabetical
        new_token = re.sub(r'[^a-zA-Z]+', '', token) 
    # remove empty value and single character value
        if new_token != "" and len(new_token) >= 2: 
            
            vowels=len([v for v in new_token if v in "aeiou"])
            
            if vowels != 0:
                
            # remove line that only contains consonants
                clean_token.append(new_token)

# Get the list of stop words
    stop_words = stopwords.words('english')
# add new stopwords to the list
   # stop_words.extend(["could","though","would","also","many",'much'])
   # print(stop_words)
# Remove the stopwords from the list of tokens
    tokens = [x for x in clean_token if x not in stop_words]
    return tokens"""


def ttttt(post_punctuation):
    #charger les stopwords 
    from nltk.corpus import stopwords
    nltk.download('stopwords')
    stop_wd = stopwords.words('english')

#Stemmer 
    from nltk.stem import LancasterStemmer
    lst = LancasterStemmer()
    txt = []
    for word in post_punctuation:
        
        if (word not in txt) & (word not in stop_wd):  
            
            txt.append(word)
    return txt
            
for message in consumer:
    
    tweets = message.value
    print(tweets)
    #dict_data = tweets.json()
    #dict_data = json.loads(tweets)
    
    #time.sleep(3)
    #tweets = p.clean(tweets)
    #tweets =text_lowercase(tweets)
    dict_data = json.loads(tweets)
    #print(dict_data["text"])
    
    """import string
    text = "Hello! How are you!! I'm very excited that you're going for a trip to Europe!! Yayy!"""
    text_clean = "".join([i for i in dict_data["text"] if i not in string.punctuation])

    
    
    anlyse_sentiment=analyze_sentiment(dict_data["text"])
    #text_clean=clean_tweet(dict_data["text"])
    
    #print(text_clean)
    
    Mot_Plus_frequant = tokenize(text_clean)
   # print(Mot_Plus_frequant)
    
    
    Hashtag=extract_hashtags(dict_data["text"])
    
    
    #print(Hashtag)
    
    mot_frequant = Convert(dict_data["text"])
    word = collections.Counter(mot_frequant)
    print("#########################################")
    #print(word.most_common(1))
    print("#########################################")
    
    #Word_FA=processing(tweets)
    Word_FA=ttttt(dict_data["text"])
    print(Word_FA)
    
   # freq=freq(dict_data["text"])
   #"localisation": dict_data["user.location"],
    print("\n")
    #print(freq)
    #print(dict_data["favorite_count"])
    #print(dict_data["retweet_count"])
    
    #print(mot_frequant)
    #print(dict_data["user"]["location"])
  
    es.index(index="index_s1",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "Sentiment":anlyse_sentiment,
                       "localisation": dict_data["user"]["location"],
                       
                       "location" : {
                        "lat" : 48.86412,
                        "lon" : 2.31959
                         },
                       "Like":dict_data["favorite_count"],
                       "Word_Fi":Word_FA,
                       "hashtag":Hashtag,
                       "Mot_P_Frequant":Mot_Plus_frequant,
                       })
    
    print(tweets)
    

 
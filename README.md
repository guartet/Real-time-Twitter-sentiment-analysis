# Real-time-Twitter-sentiment-analysis

This application analyzes tweets in the following way. First I collect the data from Twitter then I store them on kafka, Then I send the tweets from kafka to kafka consumer to analyze them with sparkStreaming, finally I display the results in kibana with ElasticSearch.
Tools: 
* Python 
* Twitter API 
* kafka 
* ElasticSearch/Kibana
* SparkStreaming
# the result Covid19 topic's is 
* Most used Hashtags

![image](https://user-images.githubusercontent.com/79747866/198354646-60650009-3f8a-4284-ae72-f19d1cbd4a43.png)

* Most frequent words

![image](https://user-images.githubusercontent.com/79747866/198354730-95f936d7-2d43-4ea4-afc7-75a58d2b3f05.png)

* Feelings statistics

![image](https://user-images.githubusercontent.com/79747866/198354789-7be6bfbb-3976-45d0-bc5d-6a6dc33d2e38.png)

![image](https://user-images.githubusercontent.com/79747866/198354843-3986cbcf-b686-4691-8f5c-16e5d488faa3.png)

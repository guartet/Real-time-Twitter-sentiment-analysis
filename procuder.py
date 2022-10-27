"""API ACCESS KEYS"""


access_token = "835845423478763520-wqkuBoB4RxiXmyNPHOLirmQi1dZoHrV"
access_token_secret = "2ms1M4S6Iwyvuohsp4RGBTU2luUPfdZ5nrka5fUJb5jtQ"
consumer_key = "RHnpXisXzyRZ98sXFgVIQj0T3"
consumer_secret = "rUrSGMAnUUZgMxCDQ3n5QUJXVbpCO5aucVhw7xkbAOTfyXIDpv"


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server


topic_name = "mytopic"


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS() 
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["Apple"], stall_warnings=True, languages= ["en"])


class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
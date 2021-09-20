# coding: utf-8
import datetime
import json
import time

import tweepy
import logging
from google.auth import jwt
from google.cloud import pubsub_v1
from tweepy.streaming import StreamListener

# Config
service_account_info = json.load(open("../twitter-stock-sentiment-83440f2bdf14.json"))
publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=publisher_audience
)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path("twitter-stock-sentiment", "stock-twitter") 


# Load in a json file with your Tweepy API credentials
with open("account.json") as json_data:
    account_data = json.load(json_data)

# Select the account you want to listen with
listener_1 = account_data["user_1"]

# Authenticate to the API
auth = tweepy.OAuthHandler(listener_1["consumer_key"], listener_1["consumer_secret"])
auth.set_access_token(listener_1["access_token"], listener_1["access_token_secret"])

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)

# Define the list of terms to listen to
lst_hashtags = ["#tesla", "#elon musk", "#tsla"]

# Method to push messages to pubsub
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
            sent_json_data = json.dumps({
                "id": data["id"],
                "text": data["text"],
                "user_id": data["user_id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8")
            publisher.publish(topic_path, data=json.dumps({
                "id": data["id"],
                "text": data["text"],
                "user_id": data["user_id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
            print('{} is written to pubsub'.format(data['text']))
            print('{} json data is sent'.format(sent_json_data))
    except Exception as e:
        raise

# Method to format a tweet from tweepy
def reformat_tweet(tweet):
    x = tweet

    processed_doc = {
        "id": x["id"],
        "lang": x["lang"],
        "retweeted_id": x["retweeted_status"]["id"] if "retweeted_status" in x else None,
        "favorite_count": x["favorite_count"] if "favorite_count" in x else 0,
        "retweet_count": x["retweet_count"] if "retweet_count" in x else 0,
        "coordinates_latitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "coordinates_longitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "place": x["place"]["country_code"] if x["place"] else None,
        "user_id": x["user"]["id"],
        "created_at": time.mktime(time.strptime(x["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
    }

    if x["entities"]["hashtags"]:
        processed_doc["hashtags"] = [{"text": y["text"], "startindex": y["indices"][0]} for y in
                                     x["entities"]["hashtags"]]
    else:
        processed_doc["hashtags"] = []

    if x["entities"]["user_mentions"]:
        processed_doc["usermentions"] = [{"screen_name": y["screen_name"], "startindex": y["indices"][0]} for y in
                                         x["entities"]["user_mentions"]]
    else:
        processed_doc["usermentions"] = []

    if "extended_tweet" in x:
        processed_doc["text"] = x["extended_tweet"]["full_text"]
    elif "full_text" in x:
        processed_doc["text"] = x["full_text"]
    else:
        processed_doc["text"] = x["text"]

    return processed_doc

# Custom listener class
class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just pushes tweets to pubsub
    """

    def __init__(self):
        super(StdOutListener, self).__init__()
        self._counter = 0

    def on_status(self, data):
        write_to_pubsub(reformat_tweet(data._json))
        self._counter += 1
        logging.info('{} tweets received'.format(self._counter))
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False


# Start listening
l = StdOutListener()
stream = tweepy.Stream(auth, l, tweet_mode='extended')
stream.filter(track=lst_hashtags)

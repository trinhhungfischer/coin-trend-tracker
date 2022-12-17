from twython import TwythonStreamer
import json

class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            print(data['text'].encode('utf-8'))
            # producer.send_messages(topicName, json.dumps(data))
    def on_error(self, status_code, data, headers = None):
    	print(status_code)


CONSUMERKEY = ""
CONSUMERSECRET = ""
OAUTHTOKEN = ""
OAUTHTOKENSECRET=""

stream = MyStreamer(CONSUMERKEY, CONSUMERSECRET, OAUTHTOKEN, OAUTHTOKENSECRET)

results = stream.statuses.filter(track=twitterFilter)


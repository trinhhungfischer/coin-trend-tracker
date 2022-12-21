from twython import Twython
from kafka import *
from twython import TwythonStreamer

# Load the API keys from the config file
APP_KEY = 'cB403qOxik9ptgiKuVwGlLBFP'
APP_KEY_SECRET = 'w24V47sVpq2fNKN9aifXHV9DBbhJVTPxF6X0IGPN8WN4c0Tqqo'
CONSUMER_KEY = '1221779246374256640-y3i6uUc6VFDH9aGNRnLLLTUZxKHb81'
CONSUMER_KEY_SECRET = 'Msd8Jc3AFdm1BBxslBs02P97VZTCV68VnDGyLeBwdnQsZ'

twitter = Twython(APP_KEY, APP_KEY_SECRET, CONSUMER_KEY, CONSUMER_KEY_SECRET)
result = twitter.search(q='%23BTC', count=10)
print(result)

# -*- coding: utf-8 -*-
"""
Created on Fri Jan 16 17:27:10 2015
this is twitter producer for Kafka
this program fetch twitter data relevant to
stocks and put it in Kafka
@author: shafiab
"""

class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
	    print data
            #producer.send_messages(topicName, json.dumps(data))
    def on_error(self, status_code, data):
    	print '!!! error occurred !!!'
    	print self
    	print data
        print status_code


CONSUMERKEY = ""
CONSUMERSECRET = ""
OAUTHTOKEN = ""
OAUTHTOKENSECRET=""

stream = MyStreamer(CONSUMERKEY, CONSUMERSECRET, OAUTHTOKEN, OAUTHTOKENSECRET)
twitterFilter = cashtag('NYSE100')+cashtag('NYSE100')+cashtag('DOW30')+cashtag('COMPANIES')
results = stream.statuses.filter(track=twitterFilter)


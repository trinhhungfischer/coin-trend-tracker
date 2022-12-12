import snscrape.modules.twitter as sntwitter
import pandas as pd

# Creating list to append tweet data to
tweets_list1 = []

# tweet_file = open("tweets.", "w")

# Using TwitterSearchScraper to scrape data and append tweets to list
for i, tweet in enumerate(sntwitter.TwitterSearchScraper('%23BTC ').get_items()):
    if i>200:
        break
    tweet.content = tweet.content.replace('\n', ' ')
    
    tweets_list1.append([tweet.date, tweet.id, tweet.content, tweet.user.username])
    print(tweet)
    
# Creating a dataframe from the tweets list above 
tweets_df1 = pd.DataFrame(tweets_list1, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])

tweets_df1.to_csv('tweets.csv', index=False)
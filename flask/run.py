from datetime import datetime, timedelta
from flask import Flask, redirect, url_for, render_template
from flask_cors import CORS
from cassandra.cluster import Cluster
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider

def cassandra_conn():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
    session = cluster.connect("tweets_info")
    print('================== Connect Cassandra Successful ============================')
    return session, cluster

session, cluster = cassandra_conn()

app = Flask(__name__)
CORS(app)

@app.route('/', methods=['GET'])
def index():
    return get_hottwitter()

@app.route('/hottwitter', methods=['GET'])
def get_hottwitter():
    results = session.execute('select * from tweets_info.recent_tweets')
    ids = []
    timestamps = []
    texts = []
    count = 0
    top = []
    top_like = 0
    top_retweet = 0
    top_reply = 0
    top_quote = 0
    for row in results:
        ids.append(row.tweet_id)
        timestamps.append(row.created_at)
        texts.append(row.tweet_text)
        if row.like_count >= top_like:
            top_like = row.like_count
        if row.retweet_count >= top_retweet:
            top_retweet = row.retweet_count
        if row.reply_count >= top_reply:
            top_reply = row.reply_count
        if row.quote_count >= top_quote:
            top_quote = row.quote_count
        count += 1
    
    top = [top_like, top_retweet, top_reply, top_quote]
    return render_template('hottwitter.html', ids=ids, timestamps=timestamps, texts=texts, count = count, top=top)

@app.route('/tophashtag', methods=['GET'])
def get_tophashtag():
    results = session.execute('select * from tweets_info.total_tweets;')

    # Table
    hashtags = []
    tweets_total = []
    tweets_like = []
    tweets_retweet = []
    tweets_reply = []
    tweets_quote = []
    
    count = 0
    top = []
    top_total = 0
    top_like = 0
    top_retweet = 0
    top_reply = 0
    top_quote = 0
    top_total_hashtag = ''
    top_like_hashtag = ''
    top_retweet_hashtag = ''
    top_reply_hashtag = ''
    top_quote_hashtag = ''
    
    for row in results:
        hashtags.append(row.hashtag)
        tweets_total.append(row.total_tweets)
        tweets_like.append(row.total_likes)
        tweets_retweet.append(row.total_retweets)
        tweets_reply.append(row.total_replies)
        tweets_quote.append(row.total_quotes)
        
        if row.total_tweets >= top_total:
            top_total = row.total_tweets
            top_total_hashtag = row.hashtag
        
        if row.total_likes >= top_like:
            top_like = row.total_likes
            top_like_hashtag = row.hashtag 
                       
        if row.total_retweets >= top_retweet:
            top_retweet = row.total_retweets
            top_retweet_hashtag = row.hashtag
            
        if row.total_replies >= top_reply:
            top_reply = row.total_replies
            top_reply_hashtag = row.hashtag
            
        if row.total_quotes >= top_quote:
            top_quote = row.total_quotes
            top_quote_hashtag = row.hashtag
        
        count += 1
    
    top = [top_total, top_like, top_retweet, top_reply, top_quote,
           top_total_hashtag, top_like_hashtag, top_retweet_hashtag, top_reply_hashtag, top_quote_hashtag]
    
    sort_arrays_by_reference(tweets_total, hashtags, tweets_like, tweets_retweet, tweets_reply, tweets_quote, reverse=True)    
    
    
    # Chart: Ve top 10 tweet, top 10 like, top 10 retweet, top 10 reply cac hashtag
    top10_tweets_total_hashtag = []
    top10_tweets_total = []
    for i in range(10):
        top10_tweets_total_hashtag.append(hashtags[i])
        top10_tweets_total.append(tweets_total[i])
    
    return render_template('tophashtag.html',
                    table_count = count,
                    table_hashtags = hashtags, 
                    table_tweets_total = tweets_total,
                    table_tweets_like = tweets_like,
                    table_tweets_retweet = tweets_retweet,
                    table_tweets_reply = tweets_reply,
                    table_tweets_quote = tweets_quote,
                    top = top,
                    chart10_tweets_total_hashtags = top10_tweets_total_hashtag,
                    chart10_tweets_total = top10_tweets_total,
                    )

@app.route('/sentiment', methods=['GET'])
def get_sentiment():
    results = session.execute('select * from tweets_info.total_sentiment;')

    hashtags = []
    total_tweets = []
    total_positives = []
    total_negatives = []
    total_neutrals = []
    count = 0

    top_total_tweets_hashtag = ''
    top_total_tweets = 0
    top_positive_hashtag = ''
    top_positive = 0
    top_negative_hashtag = ''
    top_negative = 0
    top_neutral_hashtag = ''
    top_neutral = 0


    for row in results:
        hashtags.append(row.hashtag)
        total_tweets.append(row.total_tweets)
        total_positives.append(row.total_positives)
        total_negatives.append(row.total_negatives)
        total_neutrals.append(row.total_neutrals)
    
        if row.total_tweets >= top_total_tweets:
            top_total_tweets = row.total_tweets
            top_total_tweets_hashtag = row.hashtag
        
        if row.total_positives >= top_positive:
            top_positive = row.total_positives
            top_positive_hashtag = row.hashtag
        
        if row.total_negatives >= top_negative:
            top_negative = row.total_negatives
            top_negative_hashtag = row.hashtag
        
        if row.total_neutrals >= top_neutral:
            top_neutral = row.total_neutrals
            top_neutral_hashtag = row.hashtag
        
        count += 1
    top = [top_total_tweets, top_positive, top_neutral, top_negative,
    top_total_tweets_hashtag, top_positive_hashtag, top_neutral_hashtag, top_negative_hashtag]

    # Chart
    sort_arrays_by_reference(total_tweets, hashtags, total_positives, total_negatives, total_neutrals, reverse=True)

    return render_template('sentiment.html',
                    top = top,
                    table_count = count,
                    table_hashtags = hashtags,
                    table_total_tweets = total_tweets,
                    table_total_positives = total_positives,
                    table_total_neutrals = total_neutrals,
                    table_total_negatives = total_negatives,
                    chart100_hashtags = hashtags[:100],
                    chart100_total_positives = total_positives[:100],
                    chart100_total_negatives = total_negatives[:100],
                    chart100_total_neutrals = total_neutrals[:100],
                    )


def sort_arrays_by_reference(reference, *arrays, reverse=False):
    zipped = zip(reference, *arrays)
    sorted_zipped = sorted(zipped, key=lambda x: x[0], reverse=reverse)
    sorted_reference, *sorted_arrays = zip(*sorted_zipped)
    
    results = [list(sorted_reference)]
    for array in sorted_arrays:
        results.append(list(array))

    return results

# def map_to_percentages(array):
#     total = sum(array)
#     return [round(x / total * 100, 2) for x in array]

if __name__ == "__main__":
    app.run(debug=True)

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
    # Table

    # Chart: Ve top 10 tweet, top 10 like, top 10 retweet, top 10 reply cac hashtag

    return render_template('tophashtag.html', chart="conic-gradient(#F15854 4%, #4D4D4D 0 8%, #5DA5DA 0 17%,#DECF3F 0 48%,#B276B2 0 54%,#FAA43A 0);")

@app.route('/sentiment', methods=['GET'])
def get_sentiment():
    # Chua biet ban muon ve gi

    return render_template('sentiment.html')

if __name__ == "__main__":
    app.run(debug=True)

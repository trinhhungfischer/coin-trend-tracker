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
    for row in results:
        print(row)
        ids.append(row.tweet_id)
        timestamps.append(row.created_at)
        texts.append(row.tweet_text)
        count += 1
    return render_template('hottwitter.html', ids=ids, timestamps=timestamps, texts=texts, count = count)

@app.route('/tophashtag', methods=['GET'])
def get_tophashtag():
    return render_template('tophashtag.html')

@app.route('/sentiment', methods=['GET'])
def get_sentiment():
    return render_template('sentiment.html')

if __name__ == "__main__":
    app.run(debug=True)

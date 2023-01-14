DROP KEYSPACE IF EXISTS tweets_info;
CREATE KEYSPACE tweets_info WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use tweets_info;

CREATE TABLE recent_tweets
(
 tweet_id text,
 user_id text,
 tweet_text text,
 like_count int,
 retweet_count int,
 reply_count int,
 quote_count int,
 created_at timestamp,
 hashtags list<text>,
 PRIMARY KEY ((tweet_id), user_id, created_at)
);


CREATE TABLE total_tweets_per_hashtag
(
 hashtag text,
 total_tweets bigint,
 total_likes bigint,
 total_retweets bigint,
 total_replies bigint,
 total_quotes bigint,
 record_date text,
 timestamp timestamp,
 PRIMARY KEY (hashtag, record_date)
);

CREATE TABLE total_tweets_windows
(
 hashtag text,
 total_tweets bigint,
 total_likes bigint,
 total_retweets bigint,
 total_replies bigint,
 total_quotes bigint,
 record_date text,
 timestamp timestamp,
 PRIMARY KEY (hashtag, record_date)
);

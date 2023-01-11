DROP KEYSPACE IF EXISTS latesttweets;
CREATE KEYSPACE latesttweets WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use latesttweets;

CREATE TABLE recenttweets
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



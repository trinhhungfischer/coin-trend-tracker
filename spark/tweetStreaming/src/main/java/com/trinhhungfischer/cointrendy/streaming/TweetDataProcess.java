package com.trinhhungfischer.cointrendy.streaming;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.joda.time.Duration;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * This class processes the Tweets data
 */
public class TweetDataProcess {
    private static final Logger logger = Logger.getLogger(TweetDataProcess.class);

    /**
     * Method to get and save Tweets data that has hashtags
     * @param dataStream original Tweet data stream
     */
    public static void processTweetData(
            JavaDStream<TweetData> dataStream
    ) {
        JavaDStream<TweetData> tweetDataWithHashTag = dataStream
                .filter((tweetData -> filterTweetWithHashTags(tweetData)));

        saveToCassandra(tweetDataWithHashTag);
    }

    private static void saveToCassandra(final JavaDStream<TweetData> dataStream) {
        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("tweetId", "tweet_id");
        columnNameMappings.put("authorId", "user_id");
        columnNameMappings.put("text", "tweet_text");
        columnNameMappings.put("likeCount", "like_count");
        columnNameMappings.put("retweetCount", "retweet_count");
        columnNameMappings.put("replyCount", "reply_count");
        columnNameMappings.put("quoteCount", "quote_count");
        columnNameMappings.put("createdAt", "created_at");
        columnNameMappings.put("hashtags", "hashtags");

        // Call CassandraStreamingJavaUtil function to save in DB
        CassandraStreamingJavaUtil.javaFunctions(dataStream)
                .writerBuilder("latesttweets",
                        "recenttweets",
                        CassandraJavaUtil.mapToRow(TweetData.class, columnNameMappings))
                .withConstantTTL(Duration.standardSeconds(120)) // Keeps data for 120 seconds
                .saveToCassandra();
    }

    /**
     * Filter the tweet with the hashtag
     * @param tweetData
     * @return
     */
    private static boolean filterTweetWithHashTags(TweetData tweetData) {
        if (tweetData.getHashtags().size() > 0) {
            return true;
        }
        else return false;
    }
}

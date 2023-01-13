package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.dto.AggregateKey;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.entity.TotalTweetIndexData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import com.trinhhungfischer.cointrendy.common.entity.WindowTweetIndexData;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Serializable;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;

/**
 * Class to process Tweet Data stem to produce Tweet data details
 * @author trinhhungfischer
 */
public class RealTimeTrendingProcessor {

    private static final Logger logger = Logger.getLogger(RealTimeTrendingProcessor.class);

    /**
     * This method to get tweets for each different hashtags
     * and update them accordingly when there is new tweets with each
     * related hashtags
     * @param filteredTweetData
     * @param broadcastData
     */
    public static void processTotalTweet(JavaDStream<TweetData> filteredTweetData,
                                         Broadcast<HashtagData> broadcastData) {
        // Need to keep state for total count
        StateSpec<AggregateKey, TweetAnalysisField, TweetAnalysisField, Tuple2<AggregateKey, TweetAnalysisField>> stateSpec = StateSpec
                .function(RealTimeTrendingProcessor::updateState)
                .timeout(Durations.seconds(3600));

        // We need to get count of tweets group by hashtag
        JavaDStream<TotalTweetIndexData> totalTweetDStream = filteredTweetData
                .flatMapToPair(tweetData -> {
                        List<Tuple2<AggregateKey, TweetAnalysisField>> output = new ArrayList();
                        for (String hashtag: tweetData.getHashtags()) {
                            AggregateKey aggregateKey = new AggregateKey(hashtag);
                            TweetAnalysisField tweetIndex = new TweetAnalysisField(1L, tweetData.getLikeCount(),
                                    tweetData.getRetweetCount(), tweetData.getReplyCount(), tweetData.getQuoteCount());
                            output.add(new Tuple2<>(aggregateKey, tweetIndex));
                        }
                        return output.iterator();
                    })
                .filter(hashtagPair -> {
                    String hashtag = hashtagPair._1().getHashtag();
                    return broadcastData.value().isNeededHashtags(hashtag);
                })
                .reduceByKey((a, b) -> TweetAnalysisField.add(a, b))
                .mapWithState(stateSpec)
                .map(tuple2 -> tuple2)
                .map(RealTimeTrendingProcessor::mapToTotalTweetData);

        saveTotalTweet(totalTweetDStream);
    }

    private static TotalTweetIndexData mapToTotalTweetData(Tuple2<AggregateKey, TweetAnalysisField> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " +
                        tuple._2());
        TotalTweetIndexData totalTweetIndexData = new TotalTweetIndexData();
        totalTweetIndexData.setHashtag(tuple._1.getHashtag());
        totalTweetIndexData.setTotalTweets(tuple._2().getNumTweet());
        totalTweetIndexData.setTotalLikes(tuple._2().getNumLike());
        totalTweetIndexData.setTotalRetweets(tuple._2().getNumRetweet());
        totalTweetIndexData.setTotalReplies(tuple._2().getNumQuote());
        totalTweetIndexData.setTotalQuotes(tuple._2().getNumTweet());
        totalTweetIndexData.setRecordDate(new Timestamp(new Date().getTime()));
        return totalTweetIndexData;
    }

    private static void saveTotalTweet(final JavaDStream<TotalTweetIndexData> totalTweetData) {
        // Map class property to cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("hashtag", "hashtag");
        columnNameMappings.put("totalTweets", "total_tweets");
        columnNameMappings.put("totalLikes", "total_likes");
        columnNameMappings.put("totalRetweets", "total_retweets");
        columnNameMappings.put("totalReplies", "total_replies");
        columnNameMappings.put("totalQuotes", "total_quotes");
        columnNameMappings.put("recordDate", "record_date");


        // Call CassandraStreamingJavaUtils function to save in DB
        CassandraStreamingJavaUtil.javaFunctions(totalTweetData).writerBuilder(
                "tweets_info",
                "total_tweets_per_hashtag",
                CassandraJavaUtil.mapToRow(TotalTweetIndexData.class, columnNameMappings)
        ).saveToCassandra();
    }


    /**
     * Function to get running sum by maintaining the state
     *
     * @param key
     * @param currentIndex
     * @param state
     * @return
     */
    private static Tuple2<AggregateKey, TweetAnalysisField> updateState(
            AggregateKey key,
            org.apache.spark.api.java.Optional<TweetAnalysisField> currentIndex,
            State<TweetAnalysisField> state
    ) {
        TweetAnalysisField objectOption = currentIndex.get();
        objectOption = objectOption == null ? new TweetAnalysisField(0L, 0L, 0L, 0L, 0L) : objectOption;
        long totalTweet = objectOption.getNumTweet() + (state.exists() ? state.get().getNumTweet() : 0);
        long totalLike = objectOption.getNumLike() + (state.exists() ? state.get().getNumTweet() : 0);
        long totalRetweet = objectOption.getNumRetweet() + (state.exists() ? state.get().getNumRetweet() : 0);
        long totalReply = objectOption.getNumReply() + (state.exists() ? state.get().getNumReply() : 0);
        long totalQuote = objectOption.getNumQuote() + (state.exists() ? state.get().getNumQuote() : 0);

        TweetAnalysisField newSum = new TweetAnalysisField(totalTweet, totalLike, totalRetweet, totalReply, totalQuote);

        Tuple2<AggregateKey, TweetAnalysisField> totalPair = new Tuple2<>(key, newSum);
        state.update(newSum);

        return totalPair;
    }


    /**
     * Method to get window tweets counts of different type for each hashtag. Window duration = 30 seconds
     * and Slide interval = 10 seconds
     *
     * @param filteredData data stream
     * @param broadcastData Broad tweets hashtag data
     */
    public static void processWindowTweetTotalData(JavaDStream<TweetData> filteredData, Broadcast<HashtagData> broadcastData) {
        // reduce by key and window (30 sec window and 10 sec slide).
        JavaDStream<WindowTweetIndexData> analysisDStream = filteredData
                .flatMapToPair(tweetData -> {
                    List<Tuple2<AggregateKey, TweetAnalysisField>> output = new ArrayList();
                    for (String hashtag: tweetData.getHashtags()) {
                        AggregateKey aggregateKey = new AggregateKey(hashtag);
                        TweetAnalysisField tweetIndex = new TweetAnalysisField(1L, tweetData.getLikeCount(),
                                tweetData.getRetweetCount(), tweetData.getReplyCount(), tweetData.getQuoteCount());
                        output.add(new Tuple2<>(aggregateKey, tweetIndex));
                    }
                    return output.iterator();
                })
                .reduceByKeyAndWindow((a, b) -> TweetAnalysisField.add(a, b), Durations.seconds(30),
                        Durations.seconds(10))
                .map(RealTimeTrendingProcessor::mapToWindowTweetData);

        saveWindowTotalTweet(analysisDStream);

    }

    /**
     * Function to create WindowTweetIndexData object from Tweet data
     *
     * @param tuple
     * @return
     */
    private static WindowTweetIndexData mapToWindowTweetData(Tuple2<AggregateKey, TweetAnalysisField> tuple) {
        logger.debug("Window Count : " +
                "key " + tuple._1().getHashtag() + " value " + tuple._2());

        WindowTweetIndexData totalTweetIndexData = new WindowTweetIndexData();
        totalTweetIndexData.setHashtag(tuple._1.getHashtag());
        totalTweetIndexData.setTotalTweets(tuple._2().getNumTweet());
        totalTweetIndexData.setTotalLikes(tuple._2().getNumLike());
        totalTweetIndexData.setTotalRetweets(tuple._2().getNumRetweet());
        totalTweetIndexData.setTotalReplies(tuple._2().getNumQuote());
        totalTweetIndexData.setTotalQuotes(tuple._2().getNumTweet());
        totalTweetIndexData.setRecordDate(new Timestamp(new Date().getTime()));
        return totalTweetIndexData;
    }

    private static void saveWindowTotalTweet(final JavaDStream<WindowTweetIndexData> totalTweetData) {
        // Map class property to cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("hashtag", "hashtag");
        columnNameMappings.put("totalTweets", "total_tweets");
        columnNameMappings.put("totalLikes", "total_likes");
        columnNameMappings.put("totalRetweets", "total_retweets");
        columnNameMappings.put("totalReplies", "total_replies");
        columnNameMappings.put("totalQuotes", "total_quotes");
        columnNameMappings.put("recordDate", "record_date");


        // Call CassandraStreamingJavaUtils function to save in DB
        CassandraStreamingJavaUtil.javaFunctions(totalTweetData).writerBuilder(
                "tweets_info",
                "total_tweets_per_hashtag",
                CassandraJavaUtil.mapToRow(WindowTweetIndexData.class, columnNameMappings)
        ).saveToCassandra();
    }

}


class TweetAnalysisField implements Serializable {
    private Long numTweet = 1L;
    private Long numLike;
    private Long numRetweet;
    private Long numReply;
    private Long numQuote;

    public TweetAnalysisField(Long num_tweet, Long num_like, Long num_retweet, Long num_reply, Long num_quote) {
        this.numTweet = num_tweet;
        this.numLike = num_like;
        this.numRetweet = num_retweet;
        this.numReply = num_reply;
        this.numQuote = num_quote;
    }

    public Long getNumTweet() {
        return numTweet;
    }

    public Long getNumLike() {
        return numLike;
    }

    public Long getNumRetweet() {
        return numRetweet;
    }

    public Long getNumReply() {
        return numReply;
    }

    public Long getNumQuote() {
        return numQuote;
    }

    public static TweetAnalysisField add(TweetAnalysisField o1, TweetAnalysisField o2) {
        long totalTweet = o1.getNumTweet() + o2.getNumTweet();
        long totalLike = o1.getNumLike() + o2.getNumLike();
        long totalRetweet = o1.getNumRetweet() + o2.getNumRetweet();
        long totalReply = o1.getNumReply() + o2.getNumReply();
        long totalQuote = o1.getNumQuote() + o2.getNumQuote();
        return  new TweetAnalysisField(totalTweet, totalLike, totalRetweet, totalReply, totalQuote);
    }
}
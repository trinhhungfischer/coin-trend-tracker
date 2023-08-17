package com.trinhhungfischer.cointrendy.streaming;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.trinhhungfischer.cointrendy.common.dto.AggregateKey;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import com.trinhhungfischer.cointrendy.common.dto.TweetSentimentField;
import com.trinhhungfischer.cointrendy.common.entity.TweetSentimentData;
import com.trinhhungfischer.cointrendy.common.entity.WindowTweetSentimentData;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
//
/**
 * Class to process filtered Tweets data to get tweet sentiment
 */
public class RealtimeSentimentProcessor {
    private static final Logger logger = Logger.getLogger(RealtimeSentimentProcessor.class);

    public static void processTweetTotalSentiment(JavaDStream<TweetData> filteredTweetData,
                                                  Broadcast<HashtagData> broadcastData) {
        // Everytime data state is changed, it will update the tweet sentiment table
        StateSpec<AggregateKey, TweetSentimentField, TweetSentimentField, Tuple2<AggregateKey, TweetSentimentField>> stateSpec = StateSpec
                .function(RealtimeSentimentProcessor::updateState)
                .timeout(Durations.seconds(3600));

        //
        JavaDStream<TweetSentimentData> totalSentimentDStream = filteredTweetData.
                map(TweetSentimentField::mapToTweetSentimentField)
                .flatMapToPair(sentimentPair -> {
                            List<Tuple2<AggregateKey, TweetSentimentField>> output = new ArrayList();

                            for (String hashtag : sentimentPair._1().getHashtags()) {
                                AggregateKey aggregateKey = new AggregateKey(hashtag);
                                output.add(new Tuple2<>(aggregateKey, sentimentPair._2()));
                            }

                            return output.iterator();
                        }
                )
                .filter(hashtagPair -> {
                    String hashtag = hashtagPair._1().getHashtag();
                    return broadcastData.value().isNeededHashtags(hashtag);
                })
                .reduceByKey((a, b) -> TweetSentimentField.add(a, b))
                .mapWithState(stateSpec)
                .map(tuple2 -> tuple2)
                .map(RealtimeSentimentProcessor::mapToTotalSentiment);

        // Save to database
        saveTotalSentimentTweet(totalSentimentDStream);

    }

    private static Tuple2<AggregateKey, TweetSentimentField> updateState(
            AggregateKey key,
            org.apache.spark.api.java.Optional<TweetSentimentField> currentIndex,
            State<TweetSentimentField> state
    ) {
        TweetSentimentField objectOption = currentIndex.get();
        objectOption = objectOption == null ? new TweetSentimentField(0L, 0L, 0L, 0L) : objectOption;
        long totalTweet = objectOption.getNumTweet() + (state.exists() ? state.get().getNumTweet() : 0);
        long totalPositive = objectOption.getNumPositive() + (state.exists() ? state.get().getNumPositive() : 0);
        long totalNegative = objectOption.getNumNegative() + (state.exists() ? state.get().getNumNegative() : 0);
        long totalNeural = objectOption.getNumNeutral() + (state.exists() ? state.get().getNumNeutral() : 0);

        TweetSentimentField newSum = new TweetSentimentField(totalTweet, totalPositive, totalNegative, totalNeural);

        Tuple2<AggregateKey, TweetSentimentField> totalPair = new Tuple2<>(key, newSum);
        state.update(newSum);

        return totalPair;
    }


    private static TweetSentimentData mapToTotalSentiment(Tuple2<AggregateKey, TweetSentimentField> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " + tuple._2());

        TweetSentimentData tweetSentimentData = new TweetSentimentData();
        tweetSentimentData.setHashtag(tuple._1().getHashtag());
        tweetSentimentData.setTotalTweets(tuple._2().getNumTweet());
        tweetSentimentData.setTotalPositives(tuple._2().getNumPositive());
        tweetSentimentData.setTotalNegatives(tuple._2().getNumNegative());
        tweetSentimentData.setTotalNeutrals(tuple._2().getNumNeutral());
        Date currentTime = new Date();
        tweetSentimentData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(currentTime));
        tweetSentimentData.setTimestamp(new Timestamp(currentTime.getTime()));

        return tweetSentimentData;
    }

    private static void saveTotalSentimentTweet(final JavaDStream<TweetSentimentData> tweetSentiment) {
        // Map class property to cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("hashtag", "hashtag");
        columnNameMappings.put("totalTweets", "total_tweets");
        columnNameMappings.put("totalPositives", "total_positives");
        columnNameMappings.put("totalNegatives", "total_negatives");
        columnNameMappings.put("totalNeutrals", "total_neutrals");
        columnNameMappings.put("recordDate", "record_date");
        columnNameMappings.put("timestamp", "timestamp");

        // Call CassandraStreamingJavaUtils function to save in DB
        CassandraStreamingJavaUtil.javaFunctions(tweetSentiment).writerBuilder(
                "tweets_info",
                "total_sentiment",
                CassandraJavaUtil.mapToRow(TweetSentimentData.class, columnNameMappings)
        ).saveToCassandra();
    }

    /**
     * Method to get window tweets counts of different type for each hashtag. Window duration = 30 seconds
     * and Slide interval = 10 seconds
     *
     * @param filteredData  data stream
     * @param broadcastData Broad tweets hashtag data
     */
    public static void processWindowTotalSentiment(JavaDStream<TweetData> filteredData, Broadcast<HashtagData> broadcastData) {
        // Reduce by key and window (30 sec window and 10 seconds slide)
        JavaDStream<WindowTweetSentimentData> sentimentDStream = filteredData
                .map(TweetSentimentField::mapToTweetSentimentField)
                .flatMapToPair(
                        sentimentPair -> {
                            List<Tuple2<AggregateKey, TweetSentimentField>> output = new ArrayList();

                            for (String hashtag : sentimentPair._1().getHashtags()) {
                                AggregateKey aggregateKey = new AggregateKey(hashtag);
                                output.add(new Tuple2<>(aggregateKey, sentimentPair._2()));
                            }

                            return output.iterator();
                        }
                )
                .filter(hashtagPair -> {
                    String hashtag = hashtagPair._1().getHashtag();
                    return broadcastData.value().isNeededHashtags(hashtag);
                })
                .reduceByKeyAndWindow((a, b) -> TweetSentimentField.add(a, b), Durations.seconds(30), Durations.seconds(10))
                .map(RealtimeSentimentProcessor::mapToWindowSentimentDate);

        saveWindowSentimentData(sentimentDStream);
    }

    private static WindowTweetSentimentData mapToWindowSentimentDate(Tuple2<AggregateKey, TweetSentimentField> tuple) {
        logger.debug("Window Count : " +
                "key " + tuple._1().getHashtag() + " value " + tuple._2());

        WindowTweetSentimentData tweetSentimentData = new WindowTweetSentimentData();
        tweetSentimentData.setHashtag(tuple._1().getHashtag());
        tweetSentimentData.setTotalTweets(tuple._2().getNumTweet());
        tweetSentimentData.setTotalPositives(tuple._2().getNumPositive());
        tweetSentimentData.setTotalNegatives(tuple._2().getNumNegative());
        tweetSentimentData.setTotalNeutrals(tuple._2().getNumNeutral());
        Date currentTime = new Date();
        tweetSentimentData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(currentTime));
        tweetSentimentData.setTimestamp(new Timestamp(currentTime.getTime()));
        return tweetSentimentData;
    }

    private static void saveWindowSentimentData(final JavaDStream<WindowTweetSentimentData> tweetSentimentData) {
        // Map class properties to cassandra table columns
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("hashtag", "hashtag");
        columnNameMappings.put("totalTweets", "total_tweets");
        columnNameMappings.put("totalPositives", "total_positives");
        columnNameMappings.put("totalNegatives", "total_negatives");
        columnNameMappings.put("totalNeutrals", "total_neutrals");
        columnNameMappings.put("recordDate", "record_date");
        columnNameMappings.put("timestamp", "timestamp");

        // Call CassandraStreamingJavaUtils function to save in DB
        CassandraStreamingJavaUtil.javaFunctions(tweetSentimentData).writerBuilder(
                "tweets_info",
                "total_sentiment_windows",
                CassandraJavaUtil.mapToRow(WindowTweetSentimentData.class, columnNameMappings)
        ).saveToCassandra();
    }

}
package com.trinhhungfischer.cointrendy.batch;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.trinhhungfischer.cointrendy.common.TweetDataTimestampComparator;
import com.trinhhungfischer.cointrendy.common.dto.AggregateKey;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetAnalysisField;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import com.trinhhungfischer.cointrendy.common.entity.TweetIndexData;
import com.trinhhungfischer.cointrendy.common.entity.WindowTweetIndexData;
import com.trinhhungfischer.cointrendy.streaming.RealTimeTrendingProcessor;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BatchTrendingProcessor {
    private static final Logger logger = Logger.getLogger(RealTimeTrendingProcessor.class);


    public static void processTotalTweetData(JavaRDD<TweetData> filteredTweetData,
                                             Broadcast<HashtagData> broadcastData) {
        //
        JavaPairRDD<AggregateKey, TweetAnalysisField> analysisDStream = filteredTweetData
                .flatMapToPair(tweetData -> {
                    List<Tuple2<AggregateKey, TweetAnalysisField>> output = new ArrayList();
                    for (String hashtag : tweetData.getHashtags()) {
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
                .reduceByKey((a, b) -> TweetAnalysisField.add(a, b));

        JavaRDD<TweetIndexData> tweetIndexDStream = analysisDStream
                .map(BatchTrendingProcessor::mapToTotalTweetData);

        persistTotalTweetData(tweetIndexDStream);
    }

    private static void persistTotalTweetData(JavaRDD<TweetIndexData> tweetIndexDStream) {
        // Map class property to cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("hashtag", "hashtag");
        columnNameMappings.put("totalTweets", "total_tweets");
        columnNameMappings.put("totalLikes", "total_likes");
        columnNameMappings.put("totalRetweets", "total_retweets");
        columnNameMappings.put("totalReplies", "total_replies");
        columnNameMappings.put("totalQuotes", "total_quotes");
        columnNameMappings.put("recordDate", "record_date");
        columnNameMappings.put("timestamp", "timestamp");

        // Call CassandraStreamingJavaUtils function to save in DB
        CassandraJavaUtil.javaFunctions(tweetIndexDStream).writerBuilder(
                "tweets_info",
                "total_tweets_batch",
                CassandraJavaUtil.mapToRow(TweetIndexData.class, columnNameMappings)
        ).saveToCassandra();
    }


    private static TweetIndexData mapToTotalTweetData(Tuple2<AggregateKey, TweetAnalysisField> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " +
                        tuple._2().getNumTweet());
        TweetIndexData tweetIndexData = new TweetIndexData();
        tweetIndexData.setHashtag(tuple._1.getHashtag());
        tweetIndexData.setTotalTweets(tuple._2().getNumTweet());
        tweetIndexData.setTotalLikes(tuple._2().getNumLike());
        tweetIndexData.setTotalRetweets(tuple._2().getNumRetweet());
        tweetIndexData.setTotalReplies(tuple._2().getNumQuote());
        tweetIndexData.setTotalQuotes(tuple._2().getNumTweet());
        tweetIndexData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        tweetIndexData.setTimestamp(new Timestamp(new Date().getTime()));
        return tweetIndexData;
    }

    public static void processWindowTotalTweetData(JavaRDD<TweetData> filteredTweetData,
                                                   Broadcast<HashtagData> broadcastData) {
        Date minTimestamp = filteredTweetData.min(new TweetDataTimestampComparator()).getCreatedAt();
        Date maxTimestamp = filteredTweetData.max(new TweetDataTimestampComparator()).getCreatedAt();
        long diffInMillis = Math.abs(maxTimestamp.getTime() - minTimestamp.getTime());
        long diff = TimeUnit.DAYS.convert(diffInMillis, TimeUnit.MILLISECONDS);
        Calendar c = Calendar.getInstance();
        c.setTime(minTimestamp);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        Date start = c.getTime();

        for (int i = 0; i < diff; i++) {
            c.setTime(start);
            c.add(Calendar.DATE, 1);
            Date end = c.getTime();
            processInterval(filteredTweetData, broadcastData, start, end);
            start = end;
        }
    }

    private static void processInterval(JavaRDD<TweetData> data, Broadcast<HashtagData> broadcastData, Date start, Date end) {
        // Filter the data in a given period
        JavaRDD<TweetData> filteredTimeData = data
                .filter(measurement ->
                        (measurement.getCreatedAt().equals(start) || measurement.getCreatedAt().after(start))
                                && measurement.getCreatedAt().before(end)
                );

        // Transform date filter data to window tweet index data
        JavaRDD<WindowTweetIndexData> tweetIndexDataJavaRDD = filteredTimeData
                .flatMapToPair(tweetData -> {
                    List<Tuple2<AggregateKey, TweetAnalysisField>> output = new ArrayList();
                    for (String hashtag : tweetData.getHashtags()) {
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
                .map(BatchTrendingProcessor::mapToWindowTweetIndexData);

        persistWindowTotalTweetData(tweetIndexDataJavaRDD);
    }

    private static WindowTweetIndexData mapToWindowTweetIndexData(Tuple2<AggregateKey, TweetAnalysisField> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " +
                        tuple._2().getNumTweet());
        WindowTweetIndexData tweetIndexData = new WindowTweetIndexData();
        tweetIndexData.setHashtag(tuple._1.getHashtag());
        tweetIndexData.setTotalTweets(tuple._2().getNumTweet());
        tweetIndexData.setTotalLikes(tuple._2().getNumLike());
        tweetIndexData.setTotalRetweets(tuple._2().getNumRetweet());
        tweetIndexData.setTotalReplies(tuple._2().getNumQuote());
        tweetIndexData.setTotalQuotes(tuple._2().getNumTweet());
        tweetIndexData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        tweetIndexData.setTimestamp(new Timestamp(new Date().getTime()));
        return tweetIndexData;
    }

    private static void persistWindowTotalTweetData(JavaRDD<WindowTweetIndexData> tweetIndexDStream) {
        // Map class property to cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("hashtag", "hashtag");
        columnNameMappings.put("totalTweets", "total_tweets");
        columnNameMappings.put("totalLikes", "total_likes");
        columnNameMappings.put("totalRetweets", "total_retweets");
        columnNameMappings.put("totalReplies", "total_replies");
        columnNameMappings.put("totalQuotes", "total_quotes");
        columnNameMappings.put("recordDate", "record_date");
        columnNameMappings.put("timestamp", "timestamp");

        // Call CassandraStreamingJavaUtils function to save in DB
        CassandraJavaUtil.javaFunctions(tweetIndexDStream).writerBuilder(
                "tweets_info",
                "total_tweets_windows_batch",
                CassandraJavaUtil.mapToRow(WindowTweetIndexData.class, columnNameMappings)
        ).saveToCassandra();
    }


}



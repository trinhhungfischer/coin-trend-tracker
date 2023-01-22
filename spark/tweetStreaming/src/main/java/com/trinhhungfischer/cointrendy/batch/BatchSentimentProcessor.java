package com.trinhhungfischer.cointrendy.batch;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.trinhhungfischer.cointrendy.common.TweetDataTimestampComparator;
import com.trinhhungfischer.cointrendy.common.dto.AggregateKey;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import com.trinhhungfischer.cointrendy.common.dto.TweetSentimentField;
import com.trinhhungfischer.cointrendy.common.entity.TweetSentimentData;
import com.trinhhungfischer.cointrendy.common.entity.WindowTweetSentimentData;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BatchSentimentProcessor {

    private static final Logger logger = Logger.getLogger(BatchSentimentProcessor.class);

    public static void processTweetSentimentData(JavaRDD<TweetData> filteredTweetData,
                                                 Broadcast<HashtagData> broadcastData) {
        JavaPairRDD<AggregateKey, TweetSentimentField> analysisDStream = filteredTweetData
                .map(TweetSentimentField::mapToTweetSentimentField)
                .flatMapToPair(sentimentPair -> {
                    List<Tuple2<AggregateKey, TweetSentimentField>> output = new ArrayList();

                    for (String hashtag : sentimentPair._1().getHashtags()) {
                        AggregateKey aggregateKey = new AggregateKey(hashtag);
                        output.add(new Tuple2<>(aggregateKey, sentimentPair._2()));
                    }

                    return output.iterator();
                })
                .filter(hashtagPair -> {
                    String hashtag = hashtagPair._1().getHashtag();
                    return broadcastData.value().isNeededHashtags(hashtag);
                })
                .reduceByKey((a, b) -> TweetSentimentField.add(a, b));

        JavaRDD<TweetSentimentData> tweetSentimentDataJavaRDD = analysisDStream
                .map(BatchSentimentProcessor::mapToTotalSentiment);

        tweetSentimentDataJavaRDD.foreach(tuple -> System.out.println(tuple.getHashtag()));

        persistSentimentTweetData(tweetSentimentDataJavaRDD);
    }

    private static TweetSentimentData mapToTotalSentiment(Tuple2<AggregateKey, TweetSentimentField> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " + tuple._2().getNumTweet());

        TweetSentimentData tweetSentimentData = new TweetSentimentData();
        tweetSentimentData.setTotalTweets(tuple._2().getNumTweet());
        tweetSentimentData.setTotalPositives(tuple._2().getNumPositive());
        tweetSentimentData.setTotalNegatives(tuple._2().getNumNegative());
        tweetSentimentData.setTotalNeutrals(tuple._2().getNumNeutral());
        tweetSentimentData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        tweetSentimentData.setTimestamp(new Timestamp(new Date().getTime()));

        return tweetSentimentData;
    }

    private static void persistSentimentTweetData(JavaRDD<TweetSentimentData> tweetSentimentDStream) {
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
        CassandraJavaUtil.javaFunctions(tweetSentimentDStream).writerBuilder(
                "tweets_info",
                "total_sentiment_batch",
                CassandraJavaUtil.mapToRow(TweetSentimentData.class, columnNameMappings)
        ).saveToCassandra();
    }

    public static void processWindowSentimentTweetData(JavaRDD<TweetData> filteredTweetData,
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
        JavaRDD<WindowTweetSentimentData> tweetIndexDataJavaRDD = filteredTimeData
                .map(TweetSentimentField::mapToTweetSentimentField)
                .flatMapToPair(sentimentPair -> {
                    List<Tuple2<AggregateKey, TweetSentimentField>> output = new ArrayList();

                    for (String hashtag : sentimentPair._1().getHashtags()) {
                        AggregateKey aggregateKey = new AggregateKey(hashtag);
                        output.add(new Tuple2<>(aggregateKey, sentimentPair._2()));
                    }

                    return output.iterator();
                })
                .filter(hashtagPair -> {
                    String hashtag = hashtagPair._1().getHashtag();
                    return broadcastData.value().isNeededHashtags(hashtag);
                })
                .reduceByKey((a, b) -> TweetSentimentField.add(a, b))
                .map(BatchSentimentProcessor::mapToWindowTweetSentiment);

        persistWindowSentimentTweetData(tweetIndexDataJavaRDD);
    }

    private static WindowTweetSentimentData mapToWindowTweetSentiment(Tuple2<AggregateKey, TweetSentimentField> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " + tuple._2().getNumTweet());

        WindowTweetSentimentData tweetSentimentData = new WindowTweetSentimentData();
        tweetSentimentData.setTotalTweets(tuple._2().getNumTweet());
        tweetSentimentData.setTotalPositives(tuple._2().getNumPositive());
        tweetSentimentData.setTotalNegatives(tuple._2().getNumNegative());
        tweetSentimentData.setTotalNeutrals(tuple._2().getNumNeutral());
        tweetSentimentData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        tweetSentimentData.setTimestamp(new Timestamp(new Date().getTime()));

        return tweetSentimentData;
    }

    private static void persistWindowSentimentTweetData(JavaRDD<WindowTweetSentimentData> tweetSentimentDStream) {
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
        CassandraJavaUtil.javaFunctions(tweetSentimentDStream).writerBuilder(
                "tweets_info",
                "total_sentiment_batch",
                CassandraJavaUtil.mapToRow(WindowTweetSentimentData.class, columnNameMappings)
        ).saveToCassandra();
    }


}

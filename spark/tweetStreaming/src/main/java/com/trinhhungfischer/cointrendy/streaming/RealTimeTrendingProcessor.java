package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.dto.AggregateKey;
import com.trinhhungfischer.cointrendy.common.entity.TotalTweetData;
import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
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
     * This method to get window tweets for each different hashtags.
     * Window duration = 30 seconds and Slide interval = 10 seconds
     * @param filteredTweetData
     */
    public static void processTotalTweetPerHashtag(JavaDStream<TweetData> filteredTweetData) {
        // Need to keep state for total count
        StateSpec<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> stateSpec = StateSpec
                .function(RealTimeTrendingProcessor::updateState)
                .timeout(Durations.seconds(3600));

        // We need to get count of tweets group by hashtag
        JavaDStream<TotalTweetData> totalTweetDStream = filteredTweetData
                .flatMapToPair(tweetData -> {
                        List<Tuple2<AggregateKey, Long>> output = new ArrayList();
                        for (String hashtag: tweetData.getHashtags()) {
                            AggregateKey aggregateKey = new AggregateKey(hashtag);
                            output.add(new Tuple2<>(aggregateKey, 1L));
                        }
                        return output.iterator();
                    })
                .reduceByKey((a, b) -> a + b)
                .mapWithState(stateSpec)
                .map(tuple2 -> tuple2)
                .map(RealTimeTrendingProcessor::mapToTrafficData);

        saveTotalTweetPerHashtag(totalTweetDStream);
    }

    private static TotalTweetData mapToTrafficData(Tuple2<AggregateKey, Long> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " +
                        tuple._2());
        TotalTweetData totalTweetData = new TotalTweetData();
        totalTweetData.setHashtag(tuple._1.getHashtag());
        totalTweetData.setTotalTweets(tuple._2());
        totalTweetData.setRecordDate(new Timestamp(new Date().getTime()));
        return totalTweetData;
    }

    private static void saveTotalTweetPerHashtag(final JavaDStream<TotalTweetData> totalTweetData) {
        // Map class property to cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("hashtag", "hashtag");
        columnNameMappings.put("totalTweets", "total_tweets");
        columnNameMappings.put("recordDate", "record_date");

        // Call CassandraStreamingJavaUtils function to save in DB
        CassandraStreamingJavaUtil.javaFunctions(totalTweetData).writerBuilder(
                "tweets_info",
                "total_tweets_per_hashtag",
                CassandraJavaUtil.mapToRow(TotalTweetData.class, columnNameMappings)
        ).saveToCassandra();
    }


    /**
     * Function to get running sum by maintaining the state
     *
     * @param key
     * @param currentSum
     * @param state
     * @return
     */
    private static Tuple2<AggregateKey, Long> updateState(
            AggregateKey key,
            org.apache.spark.api.java.Optional<Long> currentSum,
            State<Long> state
    ) {
        Long objectOption = currentSum.get();
        objectOption = objectOption == null ? 0l : objectOption;
        long totalSum = objectOption + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    }


}

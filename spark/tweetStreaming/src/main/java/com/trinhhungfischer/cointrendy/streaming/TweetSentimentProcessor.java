package com.trinhhungfischer.cointrendy.streaming;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.trinhhungfischer.cointrendy.common.constants.Sentiment;
import com.trinhhungfischer.cointrendy.common.constants.SentimentWord;
import com.trinhhungfischer.cointrendy.common.dto.AggregateKey;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import com.trinhhungfischer.cointrendy.common.entity.TweetSentimentData;
import com.trinhhungfischer.cointrendy.common.entity.WindowTweetIndexData;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Class to process filtered Tweets data to get tweet sentiment
 *
 */
public class TweetSentimentProcessor {
    private static final Logger logger = Logger.getLogger(TweetSentimentProcessor.class);

    public static void processTweetTotalSentiment(JavaDStream<TweetData> filteredTweetData,
                                             Broadcast<HashtagData> broadcastData) {
        // Everytime data state is changed, it will update the tweet sentiment table
        StateSpec<AggregateKey, TweetSentimentField, TweetSentimentField, Tuple2<AggregateKey, TweetSentimentField>> stateSpec = StateSpec
                .function(TweetSentimentProcessor::updateState)
                .timeout(Durations.seconds(3600));

        //
        JavaDStream<TweetSentimentData> totalSentimentDStream = filteredTweetData.
                map(TweetSentimentProcessor::mapToTweetSentimentField)
                .flatMapToPair(
                        sentimentPair -> {
                            List<Tuple2<AggregateKey, TweetSentimentField>> output = new ArrayList();

                            for (String hashtag: sentimentPair._1().getHashtags()) {
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
                .map(TweetSentimentProcessor::mapToTotalSentiment);

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
        long totalNeural = objectOption.getNumNeural() + (state.exists() ? state.get().getNumNeural() : 0);

        TweetSentimentField newSum = new TweetSentimentField(totalTweet, totalPositive, totalNegative, totalNeural);

        Tuple2<AggregateKey, TweetSentimentField> totalPair = new Tuple2<>(key, newSum);
        state.update(newSum);

        return totalPair;
    }

    private static Tuple2<TweetData, TweetSentimentField> mapToTweetSentimentField(TweetData tweetData) {
        TweetSentimentField sentimentField;

        String tweetText = tweetData.getText();
        Sentiment curSentiment = SentimentWord.getWordSentiment(tweetText);
        switch (curSentiment) {
            case POSITIVE:
                sentimentField = new TweetSentimentField(1L, 1L, 0L, 0L);
                break;
            case NEGATIVE:
                sentimentField = new TweetSentimentField(1L, 0L, 1L, 0L);
                break;
            case NEUTRAL:
                sentimentField = new TweetSentimentField(1L, 0L, 0L, 1L);
                break;
            default:
                sentimentField = new TweetSentimentField(1L, 0L, 0L, 0L);
                break;
        }

        Tuple2<TweetData, TweetSentimentField> output = new Tuple2<TweetData, TweetSentimentField>(tweetData, sentimentField);

        return output;
    }

    private static TweetSentimentData mapToTotalSentiment(Tuple2<AggregateKey, TweetSentimentField> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getHashtag() + " value " + tuple._2().getNumTweet());

        TweetSentimentData tweetSentimentData = new TweetSentimentData();
        tweetSentimentData.setTotalTweets(tuple._2().getNumTweet());
        tweetSentimentData.setTotalPositives(tuple._2().getNumPositive());
        tweetSentimentData.setTotalNegatives(tuple._2().getNumNegative());
        tweetSentimentData.setTotalNeutrals(tuple._2().getNumNeural());
        tweetSentimentData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        tweetSentimentData.setTimestamp(new Timestamp(new Date().getTime()));

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

}

class TweetSentimentField implements Serializable {
    private Long numTweet;
    private Long numPositive;
    private Long numNegative;
    private Long numNeural;

    public TweetSentimentField(Long numTweet, Long numPositive, Long numNegative, Long numNeural) {
        this.numTweet = numTweet;
        this.numPositive = numPositive;
        this.numNegative = numNegative;
        this.numNeural = numNeural;
    }

    public Long getNumTweet() {
        return numTweet;
    }

    public Long getNumPositive() {
        return numPositive;
    }

    public Long getNumNegative() {
        return numNegative;
    }

    public Long getNumNeural() {
        return numNeural;
    }

    public static TweetSentimentField add(TweetSentimentField o1, TweetSentimentField o2){
        long totalTweet = o1.getNumTweet() + o2.getNumTweet();
        long totalPositive = o1.getNumPositive() + o2.getNumPositive();
        long totalNegative = o1.getNumNegative() + o2.getNumNegative();
        long totalNeural = o1.getNumNeural() + o2.getNumNeural();
        return new TweetSentimentField(totalTweet, totalPositive, totalNegative, totalNeural);

    }
}
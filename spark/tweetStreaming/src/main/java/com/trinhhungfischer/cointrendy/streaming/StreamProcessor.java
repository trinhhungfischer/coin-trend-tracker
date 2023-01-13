package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Tuple2;

import java.util.*;

public class StreamProcessor {

    private static final Logger logger = Logger.getLogger(StreamProcessor.class);

    private final JavaDStream<ConsumerRecord<String, TweetData>> directKafkaConsumer;
    private JavaDStream<TweetData> transformedStream;
    private JavaDStream<TweetData> filteredStream;

    public StreamProcessor(JavaDStream<ConsumerRecord<String, TweetData>> directKafkaConsumer) {
        this.directKafkaConsumer = directKafkaConsumer;
    }

    /**
     * This method does transform operations allowing RDD-to-RDD function applied on
     * current DStream
     * @param item
     * @return
     */
    private static JavaRDD<TweetData> transformRecord(JavaRDD<ConsumerRecord<String, TweetData>> item) {
        OffsetRange[] offsetRanges;
        offsetRanges =  ((HasOffsetRanges) item.rdd()).offsetRanges();
        return item.mapPartitionsWithIndex(addMetaData(offsetRanges), true);
    }

    public StreamProcessor appendToHDFS(final SparkSession sql, final String file) {
        transformedStream.foreachRDD(rdd -> {
                    if (rdd.isEmpty()) {
                        return;
                    }
                    Dataset<Row> dataFrame = sql.createDataFrame(rdd, TweetData.class);
                    Dataset<Row> dfStore = dataFrame.selectExpr(
                            "tweetId", "text", "editTweetIds",
                            "hashtags", "language", "retweetCount", "replyCount", "likeCount",
                            "quoteCount", "createdAt", "authorId",
                            "metaData.topic as topic",
                            "metaData.fromOffset as fromOffset",
                            "metaData.kafkaPartition as kafkaPartition",
                            "metaData.untilOffset as untilOffset",
                            "metaData.hour as hour",
                            "metaData.dayOfMonth as dayOfMonth",
                            "metaData.month as month",
                            "metaData.year as year"
                    );
                    dfStore.printSchema();
                    dfStore.write()
                            .partitionBy("topic", "kafkaPartition", "year", "month", "dayOfMonth", "hour")
                            .mode(SaveMode.Append)
                            .parquet(file);
                }
        );
        return this;
    }

    public StreamProcessor transform() {
        this.transformedStream = directKafkaConsumer.transform(StreamProcessor::transformRecord);
        return this;
    }

    public StreamProcessor processTweetData() {
        TweetDataProcessor.processTweetData(transformedStream);
        return this;
    }

    public StreamProcessor filterTweetData() {
        // We need filtered stream for total counts and sentiment counts later
        var map = this.mapToPair(transformedStream);
        var state = this.mapWithState(map);
        this.filteredStream = filterByState(state).map(tuple -> tuple._1);
        return this;
    }

    public StreamProcessor cache() {
        this.filteredStream.cache();
        return this;
    }

    public StreamProcessor processTotalTweetData(Broadcast<HashtagData> broadcastData) {
        RealTimeTrendingProcessor.processTotalTweet(filteredStream, broadcastData);
        return this;
    }

    public StreamProcessor processWindowTotalTweetData(Broadcast<HashtagData> broadcastData) {
        RealTimeTrendingProcessor.processWindowTweetTotalData(filteredStream, broadcastData);
        return this;
    }

    /**
     * Helper method sections from here
     */

    /**
     *
     * @param offsetRanges
     * @return
     */
    private static Function2<Integer, Iterator<ConsumerRecord<String, TweetData>>, Iterator<TweetData>> addMetaData(
            final OffsetRange[] offsetRanges)
    {
        return (index, items) -> {
            List<TweetData> list = new ArrayList<>();
            while (items.hasNext()) {
                ConsumerRecord<String, TweetData> next = items.next();
                TweetData dataItem = next.value();

                Map<String, String> meta = new HashMap<>();
                meta.put("topic", offsetRanges[index].topic());
                meta.put("fromOffset", "" + offsetRanges[index].fromOffset());
                meta.put("kafkaPartition", "" + offsetRanges[index].partition());
                meta.put("untilOffset", "" + offsetRanges[index].untilOffset());
                meta.put("hour", "" + dataItem.getCreatedAt().toLocalDateTime().getHour());
                meta.put("dayOfMonth", "" + dataItem.getCreatedAt().toLocalDateTime().getDayOfMonth());
                meta.put("month", "" + dataItem.getCreatedAt().toLocalDateTime().getMonth());
                meta.put("year", "" + dataItem.getCreatedAt().toLocalDateTime().getYear());

                dataItem.setMetaData(meta);
                list.add(dataItem);
            }
            return list.iterator();
        };
    }

    private JavaPairDStream<String, TweetData> mapToPair(final JavaDStream<TweetData> stream) {
        var dStream = stream.mapToPair(tweetData -> new Tuple2<>(tweetData.getTweetId(), tweetData));
        dStream.print();
        return dStream;
    }
    
    private JavaMapWithStateDStream<String, TweetData, Boolean, Tuple2<TweetData, Boolean>> mapWithState(final JavaPairDStream<String, TweetData> key) {
        // Check tweets id is already processed
        StateSpec<String, TweetData, Boolean, Tuple2<TweetData, Boolean>> stateFunc = StateSpec
                .function(StreamProcessor::updateState)
                .timeout(Durations.seconds(3600));//maintain state for one hour

        var dStream = key.mapWithState(stateFunc);
        dStream.print();
        return dStream;
    }

    private JavaDStream<Tuple2<TweetData, Boolean>> filterByState(
            final JavaMapWithStateDStream<String, TweetData, Boolean, Tuple2<TweetData, Boolean>> state) {
        var dsStream = state .filter(tuple -> tuple._2.equals(Boolean.FALSE));
        logger.info("Starting Stream Processing");
        dsStream.print();
        return dsStream;
    }

    private static Tuple2<TweetData, Boolean> updateState(String str,
              Optional<TweetData> tweetData, State<Boolean> state) {

        Tuple2<TweetData, Boolean> tweets= new Tuple2<>(tweetData.get(), false);
        if (state.exists()) {
            tweets = new Tuple2<>(tweetData.get(), true);
        } else {
            state.update(Boolean.TRUE);
        }
        return tweets;
    }
    
}

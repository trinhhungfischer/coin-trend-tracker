package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

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
     *
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

    private static Function2<Integer, Iterator<ConsumerRecord<String, TweetData>>, Iterator<TweetData>> addMetaData(
            final OffsetRange[] offsetRanges
    ) {
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

}

package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class StreamProcessor {

    private static final Logger logger = Logger.getLogger(StreamProcessor.class);

    private final JavaDStream<ConsumerRecord<String, TweetData>> directKafkaConsumer;
    private JavaDStream<TweetData> transformedStream;
    private JavaDStream<TweetData> filteredStream;

    public StreamProcessor(JavaDStream<ConsumerRecord<String, TweetData>> directKafkaConsumer) {
        this.directKafkaConsumer = directKafkaConsumer;
    }

}

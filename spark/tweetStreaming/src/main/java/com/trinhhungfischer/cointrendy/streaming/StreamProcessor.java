package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
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
//                meta.put("dayOfWeek", "" + dataItem.getTimestamp().toLocalDate().getDayOfWeek().getValue());

                dataItem.setMetaData(meta);
                list.add(dataItem);
            }
            return list.iterator();
        };
    }

}

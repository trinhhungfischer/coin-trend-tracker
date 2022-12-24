package com.trinhhungfischer.cointrendy;

import com.trinhhungfischer.cointrendy.batch.LatestOffsetReader;
import com.trinhhungfischer.cointrendy.common.TweetDataDeserializer;
import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import com.trinhhungfischer.cointrendy.streaming.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.*;

import com.trinhhungfischer.cointrendy.common.ProcessorUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class PipelineProcessor implements Serializable {

    private static final Logger logger = Logger.getLogger(PipelineProcessor.class);
    private final Properties properties;
    public PipelineProcessor(Properties properties) {
        this.properties = properties;
    }

    public static void main(String[] args) {
        System.out.println("Hello world!");
    }


    private void start() throws Exception {
        String parquetFile = properties.getProperty("com.twitter.app.hdfs") + ".twitter-data-parquet";
        Map<String, Object> kafkaProperties = getKafkaParams(properties);

        SparkConf conf = ProcessorUtils.getSparkConf(properties, "streaming-processor");

        // Batch interval of 5 seconds for incoming streams
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));


        //
        streamingContext.checkpoint(properties.getProperty("com.twitter.app.spark.checkpoint.dir"));
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Map<TopicPartition, Long> offsets = getOffsets(parquetFile, sparkSession);


        //
        JavaInputDStream<ConsumerRecord<String, TweetData>> kafkaStream = getKafkaStream(
             properties,
             streamingContext,
             kafkaProperties,
             offsets
        );

        logger.info("Starting Kafka Steaming Process");

        StreamProcessor streamProcessor = new StreamProcessor(kafkaStream);


    }



    /**
     *
     * @param prop  Properties to configure the streaming
     * @param jars  JARS files to configure
     * @return      SparkConf class
     */
    private static SparkConf getSparkConf(Properties prop, String[] jars) {
        return new SparkConf()
                .setAppName(prop.getProperty(""))
                .setMaster(prop.getProperty("com.twitter.app.spark.master"))
                .set("spark.cassandra.connection.host", prop.getProperty("com.twitter.app.cassandra.host"))
                .set("spark.cassandra.connection.port", prop.getProperty("com.twitter.app.cassandra.port"))
                .set("spark.cassandra.auth.username", prop.getProperty("com.twitter.app.cassandra.username"))
                .set("spark.cassandra.auth.password", prop.getProperty("com.twitter.app.cassandra.password"))
                .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("com.twitter.app.cassandra.keep_alive"));
    }


    /**
     *
     * @param properties
     * @return
     */
    private static Map<String, Object> getKafkaParams(Properties properties) {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("com.twitter.app.kafka.brokerlist"));
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDataDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("com.twitter.app.kafka.topic"));
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("com.twitter.app.kafka.resetType"));
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return kafkaProperties;
    }

    private JavaInputDStream<ConsumerRecord<String, TweetData>> getKafkaStream(
            Properties prop,
            JavaStreamingContext streamingContext,
            Map<String, Object> kafkaProperties,
            Map<TopicPartition, Long> fromOffsets
    ) {
        List<String> topicSet = Arrays.asList(new String[]{prop.getProperty("com.iot.app.kafka.topic")});
        if (fromOffsets.isEmpty()) {
            return KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicSet, kafkaProperties)
            );
        }

        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSet, kafkaProperties, fromOffsets)
        );
    }


    private Map<TopicPartition, Long> getOffsets(final String parquetFile, final SparkSession sparkSession) {
        try {
            LatestOffsetReader latestOffSetReader = new LatestOffsetReader(sparkSession, parquetFile);
            return latestOffSetReader.read().offsets();
        } catch (Exception e) {
            return new HashMap<>();
        }
    }
}

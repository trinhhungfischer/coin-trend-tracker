package com.trinhhungfischer.cointrendy;

import com.trinhhungfischer.cointrendy.batch.LatestOffsetReader;
import com.trinhhungfischer.cointrendy.common.PropertyFileReader;
import com.trinhhungfischer.cointrendy.common.TweetDataDeserializer;
import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import com.trinhhungfischer.cointrendy.streaming.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.*;

import com.trinhhungfischer.cointrendy.common.ProcessorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.reflect.ClassTag;

/**
 * This class represents Kafka Twitter Streaming and create pipeline for processing the Tweets Data
 * @author trinhhungfischer
 */
public class PipelineProcessor implements Serializable {

    private static final Logger logger = Logger.getLogger(PipelineProcessor.class);
    private final Properties properties;
    public PipelineProcessor(Properties properties) {
        this.properties = properties;
    }

    public static void main(String[] args) throws Exception {
        String file = "twitter-spark.properties";
        Properties properties = PropertyFileReader.readPropertyFile(file);
        PipelineProcessor pipelineProcessor = new PipelineProcessor(properties);
        pipelineProcessor.start();
    }

    private void start() throws Exception {
        String parquetFile = properties.getProperty("com.twitter.app.hdfs") + "twitter-data-parquet";
        Map<String, Object> kafkaProperties = getKafkaParams(properties);

        SparkConf conf = ProcessorUtils.getSparkConf(properties, "streaming-processor");

        // Batch interval of 5 seconds for incoming streams
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));


        // Please note that while data checkpointing is useful for stateful processing, it comes with a latency cost.
        // Hence, it's necessary to use this wisely.
        // This is necessary because we keep state in some operations.
        // We are not using this for fault-tolerance. For that, we use Kafka offset @see commitOffset
        streamingContext.checkpoint(properties.getProperty("com.twitter.app.spark.checkpoint.dir"));
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Map<TopicPartition, Long> offsets = getOffsets(parquetFile, sparkSession);


        // Create kafka steamer instance
        JavaInputDStream<ConsumerRecord<String, TweetData>> kafkaStream = getKafkaStream(
             properties,
             streamingContext,
             kafkaProperties,
             offsets
        );

        logger.info("Starting Kafka Steaming Process");

        // Broadcast variables. Basically we are sending the data to each worker nodes on a Spark cluster.

        // Streaming process starts here.
        StreamProcessor streamProcessor = new StreamProcessor(kafkaStream);

        streamProcessor.transform().
                appendToHDFS(sparkSession, parquetFile);

        // Commit offset to Kafka
        commitOffset(kafkaStream);

        streamingContext.start();
        streamingContext.awaitTermination();
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

    /**
     * This method creates a directed stream from properties
     * and stream context
     * @param prop
     * @param streamingContext
     * @param kafkaProperties
     * @param fromOffsets
     * @return
     */
    private JavaInputDStream<ConsumerRecord<String, TweetData>> getKafkaStream(
            Properties prop,
            JavaStreamingContext streamingContext,
            Map<String, Object> kafkaProperties,
            Map<TopicPartition, Long> fromOffsets
    ) {
        List<String> topicSet = Arrays.asList(new String[]{prop.getProperty("com.twitter.app.kafka.topic")});
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

    /**
     * Commit the ack to kafka after process have completed
     * This is our fault-tolerance implementation
     *
     * @param directKafkaStream
     */
    private void commitOffset(JavaInputDStream<ConsumerRecord<String, TweetData>> directKafkaStream) {
        directKafkaStream.foreachRDD((JavaRDD<ConsumerRecord<String, TweetData>> trafficRdd) -> {
            if (!trafficRdd.isEmpty()) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) trafficRdd.rdd()).offsetRanges();

                CanCommitOffsets canCommitOffsets = (CanCommitOffsets) directKafkaStream.inputDStream();
                canCommitOffsets.commitAsync(offsetRanges, new TwitterOffsetCommitCallback());
            }
        });
    }
}

/**
 * This class implements OffsetCommitCallback interface
 *
 */
final class TwitterOffsetCommitCallback implements OffsetCommitCallback, Serializable {

    private static final Logger log = Logger.getLogger(TwitterOffsetCommitCallback.class);

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        log.info("---------------------------------------------------");
        log.info(String.format("{0} | {1}", new Object[]{offsets, exception}));
        log.info("---------------------------------------------------");
    }
}

package com.trinhhungfischer.cointrendy;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

import java.util.Properties;

/**
 *
 */
public class TweetDataProducer {
    private static final Logger logger = Logger.getLogger(TweetDataProducer.class);
    private final Producer<String, TweetData> producer;



    public TweetDataProducer(Producer<String, TweetData> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = PropertyFileReader.readPropertyFile();
        Producer<String, TweetData> producer = new Producer(new ProducerConfig(properties));
        TweetDataProducer tweetDataProducer = new TweetDataProducer(producer);
        tweetDataProducer.sendTweetData(properties.getProperty("kafka.topic"));
    }

    public void sendTweetData(String topic) {

//        producer.send(new KeyedMessage<>(topic, eve));
    }

}

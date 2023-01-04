package com.trinhhungfischer.cointrendy;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class TweetDataProducer {
    private static final Logger logger = Logger.getLogger(TweetDataProducer.class);
    private final Producer<String, TweetData> producer;

    private final FilteredTweetStream filteredTweetStream = new FilteredTweetStream();

    public TweetDataProducer(Producer<String, TweetData> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = PropertyFileReader.readPropertyFile();
        Producer<String, TweetData> producer = new Producer(new ProducerConfig(properties));
        TweetDataProducer tweetDataProducer = new TweetDataProducer(producer);
        tweetDataProducer.sendTweetData(properties.getProperty("kafka.topic"));
    }

    public void sendTweetData(String topic) throws IOException, URISyntaxException {
        Map<String, String> rules = new HashMap();
        rules.put("cats has:images", "cat images");
        rules.put("dogs has:images", "dog images");
        FilteredTweetStream.setupRules(rules);
        FilteredTweetStream.transferStream(producer, topic);
    }

}

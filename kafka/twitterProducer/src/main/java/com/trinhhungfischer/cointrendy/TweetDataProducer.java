package com.trinhhungfischer.cointrendy;

import com.trinhhungfischer.cointrendy.constant.CoinTicker;
import com.trinhhungfischer.cointrendy.constant.TweetConstant;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Timer;
import java.util.TimerTask;
import java.text.ParseException;
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

    public void sendTweetData(String topic) throws IOException, URISyntaxException, InterruptedException, ParseException {

        FilteredTweetStream.transferStream(producer, topic);
    }

}
package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.constants.Sentiment;
import com.trinhhungfischer.cointrendy.common.dto.HashtagData;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import com.trinhhungfischer.cointrendy.common.entity.TweetSentimentData;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

/**
 * Class to process filtered Tweets data to get tweet sentiment
 *
 */
public class TweetSentimentProcessor {
    private static final Logger logger = Logger.getLogger(TweetSentimentProcessor.class);

    public static void processTweetTotalSentiment(JavaDStream<TweetData> filteredTweetData,
                                             Broadcast<HashtagData> broadcastData) {
//        JavaDStream<TweetSentimentData> tweetSentimentData = filteredTweetData

    }

}


class TweetSentimentField implements Serializable {
    private Long numTweet;
    private Sentiment sentiment;

}
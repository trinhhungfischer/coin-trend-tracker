package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Class to process Tweet Data stem to produce Tweet data details
 * @author trinhhungfischer
 */
public class RealTimeTrendingProcessor {

    private static final Logger logger = Logger.getLogger(RealTimeTrendingProcessor.class);

    public static void processTotalTweetData(JavaDStream<TweetData> filteredTweetData) {
        // Reduce by key and window (30 seconds window and 10 seconds slide)
//        JavaDStream<WindowTweetData> traficDStream = filteredTweetData.
//                mapToPair(tweetData -> new Tuple2<>(

    }

}

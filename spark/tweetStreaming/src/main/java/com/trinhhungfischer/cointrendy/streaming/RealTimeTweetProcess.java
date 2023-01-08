package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.dto.AggregateKey;
import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import com.trinhhungfischer.cointrendy.common.entity.WindowTweetData;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

/**
 * Class to process Tweet Data stem to produce Tweet data details
 * @author trinhhungfischer
 */
public class RealTimeTweetProcess {

    private static final Logger logger = Logger.getLogger(RealTimeTweetProcess.class);

    public static void processWindowTweetData(JavaDStream<TweetData> filteredTweetData) {
        // Reduce by key and window (30 seconds window and 10 seconds slide)
//        JavaDStream<WindowTweetData> traficDStream = filteredTweetData.
//                mapToPair(tweetData -> new Tuple2<>(

    }

}

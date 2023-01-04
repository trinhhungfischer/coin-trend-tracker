package com.trinhhungfischer.cointrendy.streaming;

import com.trinhhungfischer.cointrendy.common.entity.TweetData;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Class to process Tweet Data stem to produce Tweet data details
 * @author trinhhungfischer
 */
public class RealTimeTweetProcess {

    private static final Logger logger = Logger.getLogger(RealTimeTweetProcess.class);

    public static void processWindowTweetData(JavaDStream<TweetData> filteredTweetData) {



    }

}

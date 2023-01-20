package com.trinhhungfischer.cointrendy.batch;

import com.trinhhungfischer.cointrendy.common.dto.*;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BatchSentimentProcessor {

    private static final Logger logger = Logger.getLogger(BatchSentimentProcessor.class);

    public static void processTweetSentimentData(JavaRDD<TweetData> filteredTweetData,
                                                 Broadcast<HashtagData> broadcastData) {
        JavaPairRDD<AggregateKey, TweetSentimentField> analysisDStream = filteredTweetData
                .map(TweetSentimentField::mapToTweetSentimentField)
                .flatMapToPair(sentimentPair -> {
                    List<Tuple2<AggregateKey, TweetSentimentField>> output = new ArrayList();

                    for (String hashtag: sentimentPair._1().getHashtags()) {
                        AggregateKey aggregateKey = new AggregateKey(hashtag);
                        output.add(new Tuple2<>(aggregateKey, sentimentPair._2()));
                    }

                    return output.iterator();
                })
                .filter(hashtagPair -> {
                    String hashtag = hashtagPair._1().getHashtag();
                    return broadcastData.value().isNeededHashtags(hashtag);
                })
                .reduceByKey((a, b) -> TweetSentimentField.add(a, b));

    }

}

package com.trinhhungfischer.cointrendy.common.dto;


import com.trinhhungfischer.cointrendy.common.SentimentJob;
import com.trinhhungfischer.cointrendy.common.constants.Sentiment;
import scala.Tuple2;

import java.io.Serializable;

public class TweetSentimentField implements Serializable {
    private final Long numTweet;
    private final Long numPositive;
    private final Long numNegative;
    private final Long numNeutral;

    public TweetSentimentField(Long numTweet, Long numPositive, Long numNegative, Long numNeutral) {
        this.numTweet = numTweet;
        this.numPositive = numPositive;
        this.numNegative = numNegative;
        this.numNeutral = numNeutral;
    }

    public static TweetSentimentField add(TweetSentimentField o1, TweetSentimentField o2) {
        long totalTweet = o1.getNumTweet() + o2.getNumTweet();
        long totalPositive = o1.getNumPositive() + o2.getNumPositive();
        long totalNegative = o1.getNumNegative() + o2.getNumNegative();
        long totalNeural = o1.getNumNeutral() + o2.getNumNeutral();
        return new TweetSentimentField(totalTweet, totalPositive, totalNegative, totalNeural);

    }

    public static Tuple2<TweetData, TweetSentimentField> mapToTweetSentimentField(TweetData tweetData) {
        TweetSentimentField sentimentField;

        String tweetText = tweetData.getText();
        Sentiment curSentiment = SentimentJob.getTextSentiment(tweetText);
        switch (curSentiment) {
            case POSITIVE:
                sentimentField = new TweetSentimentField(1L, 1L, 0L, 0L);
                break;
            case NEGATIVE:
                sentimentField = new TweetSentimentField(1L, 0L, 1L, 0L);
                break;
            case NEUTRAL:
                sentimentField = new TweetSentimentField(1L, 0L, 0L, 1L);
                break;
            default:
                sentimentField = new TweetSentimentField(1L, 0L, 0L, 0L);
                break;
        }

        Tuple2<TweetData, TweetSentimentField> output = new Tuple2<>(tweetData, sentimentField);
//
        return output;
    }

    public Long getNumTweet() {
        return numTweet;
    }

    public Long getNumPositive() {
        return numPositive;
    }

    public Long getNumNegative() {
        return numNegative;
    }

    public Long getNumNeutral() {
        return numNeutral;
    }
}
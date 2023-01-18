package com.trinhhungfischer.cointrendy.common.dto;

import scala.Serializable;

public class TweetAnalysisField implements Serializable {
    private Long numTweet = 1L;
    private Long numLike;
    private Long numRetweet;
    private Long numReply;
    private Long numQuote;

    public TweetAnalysisField(Long num_tweet, Long num_like, Long num_retweet, Long num_reply, Long num_quote) {
        this.numTweet = num_tweet;
        this.numLike = num_like;
        this.numRetweet = num_retweet;
        this.numReply = num_reply;
        this.numQuote = num_quote;
    }

    public Long getNumTweet() {
        return numTweet;
    }

    public Long getNumLike() {
        return numLike;
    }

    public Long getNumRetweet() {
        return numRetweet;
    }

    public Long getNumReply() {
        return numReply;
    }

    public Long getNumQuote() {
        return numQuote;
    }

    public static TweetAnalysisField add(TweetAnalysisField o1, TweetAnalysisField o2) {
        long totalTweet = o1.getNumTweet() + o2.getNumTweet();
        long totalLike = o1.getNumLike() + o2.getNumLike();
        long totalRetweet = o1.getNumRetweet() + o2.getNumRetweet();
        long totalReply = o1.getNumReply() + o2.getNumReply();
        long totalQuote = o1.getNumQuote() + o2.getNumQuote();
        return new TweetAnalysisField(totalTweet, totalLike, totalRetweet, totalReply, totalQuote);
    }
}
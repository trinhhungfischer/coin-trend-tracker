package com.trinhhungfischer.cointrendy.common.entity;

import java.sql.Timestamp;

public class TweetSentimentData {
    private String hashtag;
    private Long totalTweets;
    private Long totalPositives;
    private Long totalNegatives;
    private Long totalNeurals;
    private String recordDate;
    private Timestamp timestamp;

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }

    public Long getTotalTweets() {
        return totalTweets;
    }

    public void setTotalTweets(Long totalTweets) {
        this.totalTweets = totalTweets;
    }

    public Long getTotalPositives() {
        return totalPositives;
    }

    public void setTotalPositives(Long totalPositives) {
        this.totalPositives = totalPositives;
    }

    public Long getTotalNegatives() {
        return totalNegatives;
    }

    public void setTotalNegatives(Long totalNegatives) {
        this.totalNegatives = totalNegatives;
    }

    public Long getTotalNeurals() {
        return totalNeurals;
    }

    public void setTotalNeurals(Long totalNeurals) {
        this.totalNeurals = totalNeurals;
    }

    public String getRecordDate() {
        return recordDate;
    }

    public void setRecordDate(String recordDate) {
        this.recordDate = recordDate;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}

package com.trinhhungfischer.cointrendy.common.entity;

import java.sql.Timestamp;

public class TotalTweetData {
    private String hashtag;
    private Long totalTweets;
    private Timestamp recordDate;

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

    public Timestamp getRecordDate() {
        return recordDate;
    }

    public void setRecordDate(Timestamp recordDate) {
        this.recordDate = recordDate;
    }
}

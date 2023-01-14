package com.trinhhungfischer.cointrendy.common.entity;

import java.sql.Timestamp;

public class TweetIndexData {
    private String hashtag;
    private Long totalTweets;
    private Long totalLikes;
    private Long totalRetweets;
    private Long totalReplies;
    private Long totalQuotes;
    private String recordDate;
    private Timestamp timestamp;
    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag.toLowerCase();
    }

    public Long getTotalTweets() {
        return totalTweets;
    }

    public void setTotalTweets(Long totalTweets) {
        this.totalTweets = totalTweets;
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

    public Long getTotalLikes() {
        return totalLikes;
    }

    public void setTotalLikes(Long totalLikes) {
        this.totalLikes = totalLikes;
    }

    public Long getTotalRetweets() {
        return totalRetweets;
    }

    public void setTotalRetweets(Long totalRetweets) {
        this.totalRetweets = totalRetweets;
    }

    public Long getTotalReplies() {
        return totalReplies;
    }

    public void setTotalReplies(Long totalReplies) {
        this.totalReplies = totalReplies;
    }

    public Long getTotalQuotes() {
        return totalQuotes;
    }

    public void setTotalQuotes(Long totalQuotes) {
        this.totalQuotes = totalQuotes;
    }
}

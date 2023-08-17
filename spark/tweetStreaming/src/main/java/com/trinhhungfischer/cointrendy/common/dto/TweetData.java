package com.trinhhungfischer.cointrendy.common.dto;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;

public class TweetData implements Serializable {
    private String tweetId;
    private String text;
    private final ArrayList<String> editTweetIds;
    private final ArrayList<String> hashtags;
    private String language;
    private long retweetCount;
    private long replyCount;
    private long likeCount;
    private long quoteCount;
    private Timestamp createdAt;
    private String authorId;
//
    private Map<String, String> metaData;

    public TweetData(String tweetId, String text, ArrayList<String> editTweetIds, ArrayList<String> hashtags,
                     String language, long retweetCount, long replyCount, long likeCount, long quoteCount,
                     Timestamp createdAt, String authorId) {
        this.tweetId = tweetId;
        this.text = text;
        this.editTweetIds = editTweetIds;
        this.hashtags = hashtags;
        this.language = language;
        this.retweetCount = retweetCount;
        this.replyCount = replyCount;
        this.likeCount = likeCount;
        this.quoteCount = quoteCount;
        this.createdAt = createdAt;
        this.authorId = authorId;
    }

    public TweetData() {
        this.editTweetIds = new ArrayList<String>();
        this.hashtags = new ArrayList<String>();
    }


    public String getTweetId() {
        return tweetId;
    }

    public String getText() {
        return text;
    }

    public ArrayList<String> getEditTweetIds() {
        return editTweetIds;
    }

    public ArrayList<String> getHashtags() {
        return hashtags;
    }

    public long getRetweetCount() {
        return retweetCount;
    }

    public long getReplyCount() {
        return replyCount;
    }

    public long getLikeCount() {
        return likeCount;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public String getAuthorId() {
        return authorId;
    }

    public String getLanguage() {
        return language;
    }

    public long getQuoteCount() {
        return quoteCount;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }
}

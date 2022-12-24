package com.trinhhungfischer.cointrendy.common.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public class TweetData implements Serializable {
    private String tweetId;
    private String text;
    private ArrayList<String> editTweetIds;
    private ArrayList<String> hashtags;
    private String language;
    private int retweetCount;
    private int replyCount;
    private int likeCount;
    private int quoteCount;
    private Date createdAt;
    private String authorId;

    private Map<String, String> metaData;

    public TweetData(String tweetId, String text, ArrayList<String> editTweetIds, ArrayList<String> hashtags, String language,
                     int retweetCount, int replyCount, int likeCount, int quoteCount, Date createdAt, String authorId) {
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

    public int getRetweetCount() {
        return retweetCount;
    }

    public int getReplyCount() {
        return replyCount;
    }

    public int getLikeCount() {
        return likeCount;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public String getAuthorId() {
        return authorId;
    }

    public String getLanguage() {
        return language;
    }

    public int getQuoteCount() {
        return quoteCount;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }
}

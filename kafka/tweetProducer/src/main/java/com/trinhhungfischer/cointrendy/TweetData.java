package com.trinhhungfischer.cointrendy;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class TweetData implements Serializable {
    private String tweetId;
    private String text;
    private ArrayList<String> editTweetIds;
    private ArrayList<String> hashtags;
    private int retweetCount;
    private int replyCount;
    private int likeCount;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    private Date createdAt;
    private String authorId;



    public TweetData() {

    }

    public TweetData(String tweetId, String text, ArrayList<String> editTweetIds,
                     ArrayList<String> hashtags, int retweetCount, int replyCount,
                     int likeCount, Date createdAt, String authorId) {
        this.tweetId = tweetId;
        this.text = text;
        this.editTweetIds = editTweetIds;
        this.hashtags = hashtags;
        this.retweetCount = retweetCount;
        this.replyCount = replyCount;
        this.likeCount = likeCount;
        this.createdAt = createdAt;
        this.authorId = authorId;
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
}

package com.trinhhungfischer.cointrendy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class TweetData implements Serializable {
    private static final Logger logger = Logger.getLogger(TweetData.class);
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

    public TweetData() {
          this.editTweetIds = new ArrayList<String>();
          this.hashtags = new ArrayList<String>();
    }

    public TweetData(JsonNode jsonNode) {
        this();
        try {
            JsonNode data = jsonNode.get("data");
            this.tweetId = data.get("id").asText();
            this.text = data.get("text").asText();
            this.createdAt = new DateTime(data.get("created_at").asText()).toDate();
            this.authorId = data.get("author_id").asText();
            this.language = data.get("lang").asText();

            // Tweet Histories ID
            ArrayNode tweetHistoryIds = (ArrayNode) data.get("edit_history_tweet_ids");
            if (tweetHistoryIds!= null && tweetHistoryIds.size() > 0) {
                for (JsonNode tweetIdNode : tweetHistoryIds) {
                    String tweetId = tweetIdNode.asText();
                    if (tweetId != null) {
                        this.editTweetIds.add(tweetId);
                    }
                }
            }

            // Get Public Metrics includes retweets, replies, likes and quotes.
            JsonNode publicMetrics = data.get("public_metrics");
            if (publicMetrics!= null) {
                this.retweetCount = publicMetrics.get("retweet_count").asInt();
                this.replyCount = publicMetrics.get("reply_count").asInt();
                this.likeCount = publicMetrics.get("like_count").asInt();
                this.quoteCount = publicMetrics.get("quote_count").asInt();
            }

            // Get Tweet hashtags from Filter response
            ArrayNode hashtagsArray = (ArrayNode) data.get("entities").get("hashtags");
            if (hashtagsArray!= null && hashtagsArray.size() > 0) {
                for (JsonNode hashtagsNode : hashtagsArray) {
                    String hashtag = hashtagsNode.get("tag").asText();
                    this.hashtags.add(hashtag);
                }
            }


        }
        catch (Exception e) {
            logger.error("Cannot get tweet fields", e);
        }

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
}

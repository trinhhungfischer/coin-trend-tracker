package com.trinhhungfischer.cointrendy.constant;


import java.util.ArrayList;
import java.util.Arrays;

public class TweetConstant {
    private static final ArrayList<String> tweetFields = new ArrayList<String>(Arrays.asList(
                    "author_id",
                    "created_at",
                    "lang",
                    "entities",
                    "public_metrics",
                    "referenced_tweets"));

    private static final ArrayList<String> mediaFields = new ArrayList<String>(Arrays.asList(
            ""
    ));

    private static final ArrayList<String> tweetExpansion = new ArrayList<String>(Arrays.asList(
                    "author_id",
                    "author_name"));

    public static String getTweetFieldsString() {
        StringBuilder result = new StringBuilder();

        result.append("tweet.fields=");

        String tweetFieldString = String.join(",", tweetFields);
        result.append(tweetFieldString);

        return result.toString();
    }

    public static String getTweetExpansionString() {
        StringBuilder result = new StringBuilder();

        result.append("expansions=");
        for (String expansion : tweetExpansion) {
            result.append(expansion);
            result.append(",");
        }

        return result.toString();
    }

    public static String getMediaFieldsString() {
        StringBuilder result = new StringBuilder();

        result.append("media.fields=");
        for (String field : mediaFields) {
            result.append(field);
            result.append(",");
        }

        return result.toString();
    }

}

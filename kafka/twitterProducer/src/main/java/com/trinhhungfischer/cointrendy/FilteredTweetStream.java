package com.trinhhungfischer.cointrendy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trinhhungfischer.cointrendy.constant.CoinTicker;
import com.trinhhungfischer.cointrendy.constant.TweetConstant;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.management.Query;

import java.text.ParseException;
import java.time.Instant;

/*
 * Sample code to demonstrate the use of the Filtered Stream endpoint
 * */
public class FilteredTweetStream {

    private static final Logger logger = Logger.getLogger(FilteredTweetStream.class);

    /*
     * This method calls the filtered stream endpoint and streams Tweets from it
     */
    public static void transferStream(Producer<String, TweetData> producer, String topic)
            throws IOException, NumberFormatException, InterruptedException, ParseException {
        // a89340841emshfb89ac203c746d0p1foject9fe3jsnd656b5ca341c
        String apiKey = "a89340841emshfb89ac203c746d0p1foject9fe3jsnd656b5ca341c";
        String apiHost = "twitter135.p.rapidapi.com";

        HttpClient client = HttpClients.createDefault();

        int i = 1;
        while (true) {
            System.out.println("CALL " + i);
            String query = CoinTicker.getTickerRules().get(0); //
            if (i % 3 == 1) {
                query = CoinTicker.getTickerRules().get(1); //
            } else if (i % 3 == 2) {
                query = CoinTicker.getTickerRules().get(2); //
            }
            String apiUrl = "https://twitter135.p.rapidapi.com/v1.1/SearchTweets/?count=100&result_type=recent&q="
                    + query;
            HttpGet request = new HttpGet(apiUrl);

            request.addHeader("X-RapidAPI-Key", apiKey);
            request.addHeader("X-RapidAPI-Host", apiHost);
            try {
                HttpResponse response = client.execute(request);

                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject jsonObject = new JSONObject(responseBody);

                JSONArray tweetsArray = jsonObject.getJSONArray("statuses");
                long currentTimeUnixbefore18s = System.currentTimeMillis() - 18000;
                for (int j = 0; j < tweetsArray.length(); j++) {

                    JSONObject tweet = tweetsArray.getJSONObject(j);
                    String date = tweet.getString("created_at");
                    Date createdAt = new Date(date);
                    if (currentTimeUnixbefore18s <= createdAt.getTime()) {
                        // if (true) {
                        ArrayList<String> hashtags = new ArrayList<>();
                        ;

                        // String date = tweet.getString("created_at");

                        // Date createdAt = new Date(date);

                        String tweetId = tweet.getString("id_str");
                        String fullText = tweet.getString("full_text");
                        String userId = tweet.getJSONObject("user").getString("id_str");
                        JSONArray hashtagsArray = tweet.getJSONObject("entities").getJSONArray("hashtags");
                        String lang = tweet.getString("lang");
                        int retweetCount = tweet.getInt("retweet_count");
                        int favoriteCount = tweet.getInt("favorite_count");

                        System.out.println("Tweet " + (j + 1));
                        System.out.println("Tweet ID: " + tweetId);
                        System.out.println("Full Text: " + fullText);
                        System.out.println("User ID: " + userId);

                        System.out.print("Hashtags: ");

                        for (int k = 0; k < hashtagsArray.length(); k++) {
                            JSONObject hashtag = hashtagsArray.getJSONObject(k);
                            String hashtagText = hashtag.getString("text");
                            System.out.print("#" + hashtagText + " ");
                            hashtags.add(hashtagText);
                        }
                        System.out.println();

                        System.out.println("Language: " + lang);
                        System.out.println("Retweets: " + retweetCount);
                        System.out.println("Likes: " + favoriteCount);
                        System.out.println();
                        TweetData tweetData = new TweetData(tweetId, fullText, userId, hashtags, lang, retweetCount,
                                favoriteCount, createdAt);
                        producer.send(new KeyedMessage<>(topic, tweetData));
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            i++;
            System.out.println("Sleep 6s");
            System.out.println("----------------*************************-----------");
            Thread.sleep(6000);

        }
    }

}
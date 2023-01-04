package com.trinhhungfischer.cointrendy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * Sample code to demonstrate the use of the Filtered Stream endpoint
 * */
public class FilteredTweetStream {

    private static final Logger logger = Logger.getLogger(FilteredTweetStream.class);
    // To set your environment variables in your terminal run the following line:
    // export 'BEARER_TOKEN'='<your_bearer_token>'
    private static final String bearerToken = "AAAAAAAAAAAAAAAAAAAAAC%2BWkQEAAAAA5VceSkxXDVgtBKWMUhmSeZRVkuc%3DySnwaTG37tA1Xa5051I2A7lvjMsNdpodUSLQdgyxn9PxrxGWav";

//    public static void main(String args[]) throws IOException, URISyntaxException {
//        if (null != bearerToken) {
//            Map<String, String> rules = new HashMap();
//            rules.put("cats has:images", "cat images");
//            rules.put("dogs has:images", "dog images");
//            setupRules(bearerToken, rules);
//            transferStream(bearerToken);
//        } else {
//            System.out.println("There was a problem getting your bearer token. Please make sure you set the BEARER_TOKEN environment variable");
//        }
//    }

    /*
     * This method calls the filtered stream endpoint and streams Tweets from it
     * */
    public static void transferStream(Producer<String, TweetData> producer,
                                      String topic)
            throws IOException, URISyntaxException {

        HttpClient httpClient = getHttpClient();

        String uri = "https://api.twitter.com/2/tweets/search/stream?" + TweetConstant.getTweetFieldsString();


        URIBuilder uriBuilder = new URIBuilder(uri);

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();

        ObjectMapper mapper = new ObjectMapper();

        if (null != entity) {
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));

            String line = reader.readLine();
            while (line != null) {
                try {
                    if (!line.isEmpty()) {
                        JsonNode jsonNode = mapper.readTree(line);
                        TweetData tweetData = new TweetData(jsonNode);

                        producer.send(new KeyedMessage<>(topic, tweetData));
                    }
                } catch (JSONException e) {
                    logger.info("Input stream was delayed");
                }


                line = reader.readLine();
            }

        }
    }


    /*
     * Helper method to setup rules before streaming data
     * */
    public static void setupRules(Map<String, String> rules) throws IOException, URISyntaxException {
        List<String> existingRules = getRules();
        if (existingRules.size() > 0) {
            deleteRules(existingRules);
        }
        createRules(rules);
    }

    /*
     * Helper method to create rules for filtering
     * */
    private static void createRules(Map<String, String> rules) throws URISyntaxException, IOException {
        HttpClient httpClient = getHttpClient();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            logger.info(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    /*
     * Helper method to get existing rules
     * */
    private static List<String> getRules() throws URISyntaxException, IOException {
        List<String> rules = new ArrayList();
        HttpClient httpClient = getHttpClient();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("content-type", "application/json");
        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1) {
                JSONArray array = (JSONArray) json.get("data");
                for (int i = 0; i < array.length(); i++) {
                    JSONObject jsonObject = (JSONObject) array.get(i);
                    rules.add(jsonObject.getString("id"));
                }
            }
        }
        return rules;
    }

    /*
     * Helper method to delete rules
     * */
    private static void deleteRules(List<String> existingRules) throws URISyntaxException, IOException {
        HttpClient httpClient = getHttpClient();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            logger.info(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    private static String getFormattedString(String string, List<String> ids) {
        StringBuilder sb = new StringBuilder();
        if (ids.size() == 1) {
            return String.format(string, "\"" + ids.get(0) + "\"");
        } else {
            for (String id : ids) {
                sb.append("\"" + id + "\"" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

    private static String getFormattedString(String string, Map<String, String> rules) {
        StringBuilder sb = new StringBuilder();
        if (rules.size() == 1) {
            String key = rules.keySet().iterator().next();
            return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
        } else {
            for (Map.Entry<String, String> entry : rules.entrySet()) {
                String value = entry.getKey();
                String tag = entry.getValue();
                sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

    private static HttpClient getHttpClient() {
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();
        return httpClient;
    }


}
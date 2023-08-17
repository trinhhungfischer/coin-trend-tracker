package com.trinhhungfischer.cointrendy.common;

import com.trinhhungfischer.cointrendy.common.constants.Sentiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SentimentJob {

    private static final ArrayList<String> positiveWords = new ArrayList<String>(Arrays.asList(

            "upgrade", "upgraded", "long", "buy", "buying", "growth", "good",
            "gained", "well", "great", "nice", "top", "support", "update", "strong",
            "bullish", "bull", "highs", "win", "positive", "profits", "bonus",
            "potential", "success", "winner", "winning", "good", "profit", "opportunity",
            "rally", "upside", "promising", "breakout", "accumulation", "support", "innovation", "moon",
            "rebound", "hodl", "partnership", "optimistic", "advancement", "excitement", "excite", "upswing",
            "fortune"));

    private static final ArrayList<String> negativeWords = new ArrayList<String>(Arrays.asList(
            "downgraded", "loss", "decline", "risk", "volatility", "downtrend",
            "bears", "uncertainty", "risky", "FUD", "fear", "doubt",
            "bear", "slump", "setback", "pessimistic", "scam", "scams",
            "bearish", "weak", "scammed", "scamming", "panic", "panics",
            "volatile", "mismanagement", "fraud", "hype", "collapse", "bubble",
            "short", "ponzi", "fraudulent",
            "sell", "phishing",
            "selling",
            "forget",
            "down",
            "resistance",
            "sold",
            "sellers",
            "negative",
            "blowout",
            "losses",
            "war",
            "lost",
            "loser"));

    // public static final Sentiment getTextSentiment(String sentence) {
    // int positiveCount = 0;
    // int negativeCount = 0;

    // String[] words = sentence.split(" ");
    // for (String word : words) {
    // if (positiveWords.contains(word.toLowerCase())) {
    // positiveCount++;
    // } else if (negativeWords.contains(word.toLowerCase())) {
    // negativeCount++;
    // }
    // }

    // if (positiveCount > negativeCount) return Sentiment.POSITIVE;
    // else if (negativeCount > positiveCount) return Sentiment.NEGATIVE;
    // else return Sentiment.NEUTRAL;
    // }

    public static final Sentiment getTextSentiment(String sentence) {
        String content = "";
        try {
            // URL của API ChatGPT
            URL url = new URL("https://api.openai.com/v1/chat/completions");

            // Tạo kết nối HTTP
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization",
                    "Bearer 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");

            // Tạo JSON payload cho yêu cầu
            String payload = String.format(
                    "{\"model\": \"gpt-3.5-turbo\", \"messages\": [{\"role\": \"system\", \"content\": \"cryto coin field, only answers 'positive' or 'negative' or 'neutral'\"}, {\"role\": \"user\", \"content\": \"%s\"}]}",
                    sentence);
            System.out.println(payload);
            // Gửi yêu cầu
            connection.setDoOutput(true);
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(payload.getBytes("UTF-8"));
            outputStream.close();

            // Đọc kết quả từ API
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {

                response.append(line);
            }
            reader.close();

            // In kết quả
            System.out.println(response.toString());

            JSONObject jsonResponse = new JSONObject(response.toString());

            JSONArray choicesArray = jsonResponse.getJSONArray("choices");

            JSONObject choiceObject = choicesArray.getJSONObject(0);

            // Truy cập vào trường "message" trong mỗi lựa chọn
            JSONObject messageObject = choiceObject.getJSONObject("message");
            String role = messageObject.getString("role");
            content = messageObject.getString("content");

        } catch (Exception e) {
            e.printStackTrace();
        }
        if (content.equals("positive")) {
            return Sentiment.POSITIVE;
        } else if (content.equals("negative"))
            return Sentiment.NEGATIVE;
        else
            return Sentiment.NEUTRAL;

    }

    public static void main(String[] args) {
        // DocumentCategorizerME classifier = new DocumentCategorizerME()
    }
}

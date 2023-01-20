package com.trinhhungfischer.cointrendy.common.constants;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import opennlp.tools.doccat.DocumentCategorizerME;

public class SentimentWord {


    private static ArrayList<String> positiveWords = new ArrayList<String>(Arrays.asList(



            "upgrade", "upgraded", "long", "buy", "buying", "growth", "good",
            "gained", "well", "great", "nice", "top", "support", "update", "strong",
            "bullish", "bull", "highs", "win", "positive", "profits", "bonus",
            "potential", "success", "winner", "winning", "good"
    ));

    private static ArrayList<String> negativeWords = new ArrayList<String>(Arrays.asList(
            "downgraded",
            "bears",
            "bear",
            "bearish",
            "volatile",
            "short",
            "sell",
            "selling",
            "forget",
            "down",
            "resistance",
            "sold",
            "sellers",
            "negative",
            "selling",
            "blowout",
            "losses",
            "war",
            "lost",
            "loser"
    ));

    public static final Sentiment getTextSentiment(String sentence) {
        int positiveCount = 0;
        int negativeCount = 0;

        String[] words = sentence.split(" ");
        for (String word : words) {
            if (positiveWords.contains(word.toLowerCase())) {
                positiveCount++;
            } else if (negativeWords.contains(word.toLowerCase())) {
                negativeCount++;
            }
        }

        if (positiveCount > negativeCount) return Sentiment.POSITIVE;
        else if (negativeCount > positiveCount) return Sentiment.NEGATIVE;
        else return Sentiment.NEUTRAL;
    }

    public static void main(String[] args) {
//        DocumentCategorizerME classifier = new DocumentCategorizerME()
    }
}

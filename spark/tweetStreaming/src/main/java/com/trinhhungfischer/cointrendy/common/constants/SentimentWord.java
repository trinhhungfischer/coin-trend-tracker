package com.trinhhungfischer.cointrendy.common.constants;

import java.util.ArrayList;
import java.util.Arrays;

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

    public static final int getWordSentiment(String word) {
        if (positiveWords.contains(word)) return 1;
        else if (negativeWords.contains(word)) return -1;
        else return 0;
    }
}

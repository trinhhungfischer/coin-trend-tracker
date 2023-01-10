package com.trinhhungfischer.cointrendy.constant;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class CoinTicker {

    private static final int MAX_CHARS_IN_RULE = 512;

    private static String END_RULE_STR = " has:hashtags lang:en -is:retweet";
    public static ArrayList<String> listCoinTicker;

    public static final ArrayList<String> getListCoinTicker() {
        listCoinTicker = new ArrayList<String>();

        try {
            InputStream input = CoinTicker.class.getClassLoader().getResourceAsStream("coin_id_100_sorted.csv");

            // Open the CSV file
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            // Read the file line by line

            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line into values
                String[] values = line.split(",");

                // Add the coin ticker to the data list
                listCoinTicker.add(values[1]);
            }

            // Close the reader
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return listCoinTicker;
    }

    public static ArrayList<String> getTickerRules() {
        ArrayList<String> rules = new ArrayList<String>();

        String rule = "(";

        listCoinTicker = getListCoinTicker();

        for (int i = 0; i < listCoinTicker.size(); i++) {
            if (rule.length() + listCoinTicker.get(i).length() + 5 +
                    END_RULE_STR.length() >= MAX_CHARS_IN_RULE) {
                rules.add(rule.substring(0, rule.length() - 4) + ")" + END_RULE_STR);
                rule = "(";
            }

            if (i < listCoinTicker.size() - 1) {
                rule += ("#" + listCoinTicker.get(i) + " OR ");
            }
            else rule += ("#" + listCoinTicker.get(i) + ")");
        }
        rules.add(rule + END_RULE_STR);

        return rules;
    }

}

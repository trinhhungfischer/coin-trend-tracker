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
        listCoinTicker = getListCoinTicker();
        String rule = "";
        for (int i = 0; i < 34; i++) {
            if (i < 33) {
                rule += ("%23" + listCoinTicker.get(i) + "%20OR%20");
            } else {
                rule += ("%23" + listCoinTicker.get(i) + "");
            }

        }
        rules.add(rule);

        rule = "";
        for (int i = 34; i < 67; i++) {
            if (i < 66) {
                rule += ("%23" + listCoinTicker.get(i) + "%20OR%20");
            } else {
                rule += ("%23" + listCoinTicker.get(i) + "");
            }

        }
        rules.add(rule);
        rule = "";
        for (int i = 67; i < 100; i++) {
            if (i < 99) {
                rule += ("%23" + listCoinTicker.get(i) + "%20OR%20");
            } else {
                rule += ("%23" + listCoinTicker.get(i) + "");
            }

        }
        rules.add(rule);

        return rules;
    }

}
package com.trinhhungfischer.cointrendy.constant;

import com.trinhhungfischer.cointrendy.PropertyFileReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CoinTicker {

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

    
}

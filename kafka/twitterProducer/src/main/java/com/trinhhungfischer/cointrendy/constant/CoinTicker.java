package com.trinhhungfischer.cointrendy.constant;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class CoinTicker {
    public static void main(String[] args) {
        // The path to the CSV file
        String filePath = "./ coin_id_100_sorted.csv";

        // Create a list to store the data
        List<String[]> data = new ArrayList<>();

        try {
            // Open the CSV file
            BufferedReader reader = new BufferedReader(new FileReader(filePath));

            // Read the file line by line
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line into values
                String[] values = line.split(",");

                // Add the values to the data list
                data.add(values);
            }

            // Close the reader
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Print the data
        for (String[] row : data) {

            for (String value : row) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
    }
}

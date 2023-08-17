package com.trinhhungfischer.cointrendy.common.dto;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HashtagData implements Serializable {
    private List<String> hashtags;

    public List<String> getHashtags() {
        if (this.hashtags != null) {
            return hashtags;
        }

        this.hashtags = new ArrayList();

        try {
            InputStream input = HashtagData.class.getClassLoader().getResourceAsStream("coin_id_100_sorted.csv");

            // Open the CSV file
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            // Read the file line by line

            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line into values
                String[] values = line.split(",");

                // Add the coin ticker to the data list
                this.hashtags.add(values[1]);
            }

            // Close the reader
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }

    public boolean isNeededHashtags(String hashtag) {
        if (this.hashtags == null || this.hashtags.size() == 0) {
            return false;
        }

        return hashtags.contains(hashtag.toLowerCase());
    }

}
//
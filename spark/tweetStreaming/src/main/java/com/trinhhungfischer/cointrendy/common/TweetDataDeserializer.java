package com.trinhhungfischer.cointrendy.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trinhhungfischer.cointrendy.common.dto.TweetData;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Class to deserialize JSON string to TweetData object
 *
 * @author trinhhungfischer
 */
public class TweetDataDeserializer implements Deserializer<TweetData> {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public TweetData fromBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, TweetData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public TweetData deserialize(String s, byte[] bytes) {
        return fromBytes(bytes);
    }

    @Override
    public TweetData deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}

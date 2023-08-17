package com.trinhhungfischer.cointrendy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

/**
 * Class to conver TweetData java object to JSON
 *
 */
public class TweetDataEncoder implements Encoder<TweetData> {

    private static final Logger logger = Logger.getLogger(TweetDataEncoder.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public TweetDataEncoder(VerifiableProperties verifiableProperties) {
        logger.info("Using verifiable properties: " + verifiableProperties);
    }

    public byte[] toBytes(TweetData tweetData) {
        try {
            String msg = objectMapper.writeValueAsString(tweetData);
            logger.info(msg);
            return msg.getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Error in Serialization", e);
        }
        return null;
    }

}

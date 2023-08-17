package com.trinhhungfischer.cointrendy.common.dto;

import java.io.Serializable;

public class AggregateKey implements Serializable {

    private String hashtag;

    public AggregateKey(String hashtag) {
        super();
        this.hashtag = hashtag.toLowerCase();
    }

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag.toLowerCase();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        // Hash Coin Ticker string
        result = prime * result + ((this.hashtag == null) ? 0 : this.hashtag.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof AggregateKey) {
            AggregateKey other = (AggregateKey) obj;
            return other.getHashtag() != null && other.getHashtag().equals(this.hashtag);
        }
        return false;
    }

}
//
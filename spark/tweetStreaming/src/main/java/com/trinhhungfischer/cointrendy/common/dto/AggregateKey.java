package com.trinhhungfischer.cointrendy.common.dto;

import java.io.Serializable;

public class AggregateKey implements Serializable {

    private String coinTicker;

    public AggregateKey(String coinTicker) {
        super();
        this.coinTicker = coinTicker;
    }

    public String getCoinTicker() {
        return coinTicker;
    }

    public void setCoinTicker(String coinTicker) {
        this.coinTicker = coinTicker;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        // Hash Coin Ticker string
        result = prime * result + ((this.coinTicker == null) ? 0 : this.coinTicker.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof AggregateKey) {
            AggregateKey other = (AggregateKey) obj;
            if (other.getCoinTicker() != null && other.getCoinTicker().equals(this.coinTicker)) {
                return true;
            }
        }
        return false;
    }
}

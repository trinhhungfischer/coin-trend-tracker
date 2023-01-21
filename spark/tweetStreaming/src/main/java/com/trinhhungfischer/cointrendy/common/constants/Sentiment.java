package com.trinhhungfischer.cointrendy.common.constants;

public enum Sentiment {
    POSITIVE(1),
    NEGATIVE(-1),
    NEUTRAL(0);

    private final int value;

    Sentiment(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

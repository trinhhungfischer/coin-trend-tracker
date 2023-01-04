package com.trinhhungfischer.cointrendy.common;

import com.trinhhungfischer.cointrendy.common.entity.TweetData;

import java.io.Serializable;
import java.util.Comparator;

public class TweetDataTimestampComparator implements Comparator<TweetData>, Serializable {

    @Override
    public int compare(TweetData o1, TweetData o2) {
        if (o1 == null && o1 == null) {
            return 0;
        } else if (o1 == null && o2.getCreatedAt() == null) {
            return 1;
        } else if (o2 == null && o1.getCreatedAt() == null) {
            return -1;
        } else {
            return o1.getCreatedAt().compareTo(o2.getCreatedAt());
        }
    }
}

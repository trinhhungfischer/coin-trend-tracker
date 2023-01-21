package com.trinhhungfischer.cointrendy;

import java.sql.Timestamp;

public class Test {
    public static void main(String[] args) {
        long currentTIme = System.currentTimeMillis();

        System.out.println(currentTIme);

        long current = 1673065647000L;

        Timestamp timestamp = new Timestamp(current);

        System.out.println(timestamp.toLocalDateTime().getHour());
        System.out.println(timestamp.toLocalDateTime().getDayOfWeek());
        System.out.println(timestamp.toLocalDateTime().getDayOfMonth());

    }
}

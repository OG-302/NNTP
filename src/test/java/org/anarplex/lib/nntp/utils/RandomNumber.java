package org.anarplex.lib.nntp.utils;

import java.util.concurrent.ThreadLocalRandom;

public class RandomNumber {

    public static long generate10DigitNumber() {
        // Generate a random 10-digit number
        return ThreadLocalRandom.current().nextLong(1_000_000_000L, 10_000_000_000L);
    }
}

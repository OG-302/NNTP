package org.anarplex.lib.nntp.utils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.TimeZone;

public class DateAndTime {

    public static final LocalDateTime EPOCH = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC"));

    /**
     * Format the supplied dateTime into a String of form "EEE, dd MMM yyyy HH:mm:ss Z" (the RFC3977 standard)
     */
    public static String format1(LocalDateTime dateTime) {
        ZonedDateTime utcZonedDateTime = dateTime.atZone(ZoneId.of("UTC"));
        return DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss").format(utcZonedDateTime);
    }

    /**
     * Format the supplied dateTime into a String of form "EEE, dd MMM yyyy HH:mm:ss Z" (the RFC3977 standard)
     */
    public static String format3(LocalDateTime dateTime) {
        ZonedDateTime utcZonedDateTime = dateTime.atZone(ZoneId.of("UTC"));
        return DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss").format(utcZonedDateTime);
    }

    /**
     * Format the supplied dateTime into a String of form "yyyyMMdd hhmmss"
     */
    public static String format2(LocalDateTime dateTime) {
        ZonedDateTime utcZonedDateTime = dateTime.atZone(ZoneId.of("UTC"));
        return DateTimeFormatter.ofPattern("yyyyMMdd HHmmss").format(utcZonedDateTime);
    }

    /**
     * Parses a date string in the standard NNTP date format "yyyyMMddHHmmss"
     */
    public static LocalDateTime parse1(String date) throws DateTimeParseException {
        return LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }
}

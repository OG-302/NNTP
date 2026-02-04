package org.anarplex.lib.nntp.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

public class DateAndTime {

    public static final LocalDateTime EPOCH = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC"));

    /**
     * Format the supplied dateTime into a String of form "EEE, dd MMM yyyy HH:mm:ss Z" (the RFC3977 standard)
     */
    public static String formatRFC3977(LocalDateTime dateTime) {
        ZonedDateTime utcZonedDateTime = dateTime.atZone(ZoneId.of("UTC"));
        return rfc3977Formatter.format(utcZonedDateTime);
    }

    /**
     * Parses the date string according to RFC 3977 Date format.
     */
    public static ZonedDateTime parseRFC3977Date(String dateString) throws DateTimeParseException {
        return ZonedDateTime.parse(dateString, rfc3977Formatter);
    }

    /**
     * Format the supplied dateTime into a String of form "yyyyMMdd hhmmss"
     */
    public static String formatTo_yyyyMMdd_hhmmss(LocalDateTime dateTime) {
        ZonedDateTime utcZonedDateTime = dateTime.atZone(ZoneId.of("UTC"));
        return pattern2Formatter.format(utcZonedDateTime);
    }

    /**
     * Parses a date string in the standard NNTP date format "yyyyMMddHHmmss"
     */
    public static LocalDateTime parse_yyyMMddHHmmss(String date) throws DateTimeParseException {
        return LocalDateTime.parse(date, pattern1Formatter);
    }


    private static final DateTimeFormatter pattern1Formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC"));

    private static final DateTimeFormatter pattern2Formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss").withZone(ZoneId.of("UTC"));

    // Create a custom formatter for RFC 3977 (based on RFC 1123 with zone name support) = EEE, dd MMM yyyy HH:mm:ss
    private static final DateTimeFormatter rfc3977Formatter = new DateTimeFormatterBuilder()
            .optionalStart()
            .appendPattern("EEE")
            .optionalStart()
            .appendLiteral(',')
            .optionalEnd()
            .appendPattern(" ")
            .optionalEnd()
            .appendPattern("d MMM ")
            .appendValueReduced(ChronoField.YEAR, 2, 4, 1970)
            .appendPattern(" HH:mm")
            .optionalStart()
            .appendPattern(":ss")
            .optionalEnd()
            .optionalStart()
            .appendPattern(" ")
            .optionalStart().appendZoneOrOffsetId().optionalEnd()
            .optionalStart().appendOffset("+HHMM", "+0000").optionalEnd()
            .optionalEnd()
            .parseDefaulting(ChronoField.OFFSET_SECONDS, 0) // Default to UTC
            .toFormatter();
}

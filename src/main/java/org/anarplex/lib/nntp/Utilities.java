package org.anarplex.lib.nntp;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

class Utilities {
    
    static class DateAndTime {

        /**
         * Format the supplied dateTime into a String of form "EEE, dd MMM yyyy HH:mm:ss Z" (the RFC3977 standard)
         */
        static String formatRFC3977(Instant dateTime) {
            return rfc3977Formatter.format(dateTime);
        }

        /**
         * Format the supplied dateTime into a String of form "yyyyMMdd hhmmss"
         */
        static String formatTo_yyyyMMdd_hhmmss(Instant dateTime) {
            return pattern2Formatter.format(dateTime);
        }

        /**
         * Parses the date string according to RFC 3977 Date format.
         */
        static Instant parseRFC3977Date(String dateString) throws DateTimeParseException {
            return rfc3977Parser.parse(dateString, Instant::from);
        }

        /**
         * Parses a date string in the standard NNTP date format "yyyyMMddHHmmss"
         */
        static Instant parse_yyyMMddHHmmss(String date) throws DateTimeParseException {
            return pattern1Formatter.parse(date, Instant::from);
        }

        private static final DateTimeFormatter pattern1Formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyyMMddHHmmss")
                .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
                .toFormatter()
                .withZone(ZoneId.of("UTC"));


        private static final DateTimeFormatter pattern2Formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyyMMdd HHmmss")
                .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
                .toFormatter()
                .withZone(ZoneId.of("UTC"));

        // Formatter for RFC 3977 output: always prints numeric offset "+0000" (no "UTC")
        private static final DateTimeFormatter rfc3977Formatter = new DateTimeFormatterBuilder()
                .optionalStart()
                .appendPattern("EEE")
                .optionalStart()
                .appendLiteral(',')
                .optionalEnd()
                .appendPattern(" ")
                .optionalEnd()
                .appendPattern("dd MMM ")
                .appendPattern("yyyy")
                .appendPattern(" HH:mm")
                .optionalStart()
                .appendPattern(":ss")
                .optionalEnd()
                .appendPattern(" ")
                .appendOffset("+HHMM", "+0000")
                .parseDefaulting(ChronoField.OFFSET_SECONDS, 0) // Default to UTC
                .toFormatter()
                .withZone(ZoneId.of("UTC"));

        // Parser for RFC 3977 input: accepts either zone name (e.g., "UTC") or numeric offset (e.g., "+0000")
        private static final DateTimeFormatter rfc3977Parser = new DateTimeFormatterBuilder()
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
                .toFormatter()
                .withZone(ZoneId.of("UTC"));
    }
    
    static class Cryptography {
        /**
         * Generates a SHA-256 hash of the given input string and returns its 64-character hexadecimal representation.
         *
         * @param input the input string to hash; if null, the method returns null
         * @return the SHA-256 hash of the input string in lowercase hexadecimal format, or null if the input is null
         * @throws IllegalStateException if the SHA-256 algorithm is not available in the Java environment
         */
        static String sha256(String input) {
            if (input == null) {
                return null;
            }
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
                // Convert to lowercase hex
                StringBuilder sb = new StringBuilder(digest.length * 2);
                for (byte b : digest) {
                    sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
                    sb.append(Character.forDigit(b & 0xF, 16));
                }
                return sb.toString(); // 64 hexadecimal characters
            } catch (NoSuchAlgorithmException e) {
                // SHA-256 is guaranteed to be present in the JRE; rethrow as unchecked if not.
                throw new IllegalStateException("SHA-256 not available", e);
            }
        }
    }
    
}

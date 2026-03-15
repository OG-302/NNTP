package org.anarplex.lib.nntp;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

public class UtilitiesTest {

    // ----- DateAndTime tests -----

    @Test
    public void testFormatRFC3977_exactWithSeconds() {
        Instant t = Instant.parse("2024-01-02T03:04:05Z"); // Tuesday
        String formatted = Utilities.DateAndTime.formatRFC3977(t);
        assertEquals("Tue, 02 Jan 2024 03:04:05 +0000", formatted);
    }

    @Test
    public void testParseRFC3977_withOffset() {
        Instant parsed = Utilities.DateAndTime.parseRFC3977Date("Tue, 02 Jan 2024 03:04:05 +0000");
        assertEquals(Instant.parse("2024-01-02T03:04:05Z"), parsed);
    }

    @Test
    public void testParseRFC3977_withUTCZoneId() {
        Instant parsed = Utilities.DateAndTime.parseRFC3977Date("Tue, 02 Jan 2024 03:04:05 UTC");
        assertEquals(Instant.parse("2024-01-02T03:04:05Z"), parsed);
    }

    @Test
    public void testParseRFC3977_withoutSeconds_defaultsToZeroSeconds() {
        Instant parsed = Utilities.DateAndTime.parseRFC3977Date("Tue, 02 Jan 2024 03:04 +0000");
        assertEquals(Instant.parse("2024-01-02T03:04:00Z"), parsed);
    }

    @Test
    public void testFormatTo_yyyyMMdd_hhmmss() {
        Instant t = Instant.parse("2024-01-02T03:04:05Z");
        String formatted = Utilities.DateAndTime.formatTo_yyyyMMdd_hhmmss(t);
        assertEquals("20240102 030405", formatted);
    }

    @Test
    public void testParse_yyyMMddHHmmss_roundTrip() {
        String s = "20240102030405";
        Instant parsed = Utilities.DateAndTime.parse_yyyMMddHHmmss(s);
        assertEquals(Instant.parse("2024-01-02T03:04:05Z"), parsed);
    }

    // ----- Cryptography tests -----

    @Test
    public void testSha256_emptyString() {
        String hash = Utilities.Cryptography.sha256("");
        assertEquals(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                hash);
    }

    @Test
    public void testSha256_abc() {
        String hash = Utilities.Cryptography.sha256("abc");
        assertEquals(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                hash);
    }

    @Test
    public void testSha256_nullInput() {
        assertNull(Utilities.Cryptography.sha256(null));
    }
}

package org.anarplex.lib.nntp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AUTHINFO command handling (RFC 4643 paths). Uses the same fakes as ProtocolEngineTest where needed.
 */
public class ProtocolEngineAuthTest {

    private final MockIdentityService identity = new MockIdentityService();
    private final MockPolicyService policy = new MockPolicyService();
    private final InMemoryPersistence persistence = new InMemoryPersistence();

    private String run(String input) {
        try {
            IdentityService.Subject subject = identity.newSubject("poster-allowed");
            BufferedReader reader = new BufferedReader(new StringReader(input));
            StringWriter sw = new StringWriter();
            BufferedWriter writer = new BufferedWriter(sw);
            NetworkService.ConnectedClient client = new NetworkService.ConnectedClient(reader, writer, subject);
            ProtocolEngine engine = new ProtocolEngine(persistence, identity, policy, client);
            engine.start();
            client.flush();
            return sw.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String[] lines(String s) {
        return Arrays.stream(s.split("\r?\n")).filter(l -> !l.isEmpty()).toArray(String[]::new);
    }

    @Test
    @DisplayName("AUTHINFO USER unknown -> 481 rejected")
    void unknownUserRejected() {
        String out = run("AUTHINFO USER unknown\r\nQUIT\r\n");
        String[] ls = lines(out);
        assertTrue(Arrays.stream(ls).anyMatch(l -> l.startsWith("481 ")), "Expected 481 for unknown user");
    }

    @Test
    @DisplayName("AUTHINFO USER nopass -> immediate 281 success")
    void userNoPasswordFlow() {
        String out = run("AUTHINFO USER nopass\r\nQUIT\r\n");
        String[] ls = lines(out);
        assertTrue(Arrays.stream(ls).anyMatch(l -> l.startsWith("281 ")), "Expected 281 success without PASS");
    }

    @Test
    @DisplayName("AUTHINFO USER user -> 381, then PASS wrong -> 481")
    void userRequiresPasswordWrongPass() {
        String out = run("AUTHINFO USER user\r\nAUTHINFO PASS wrong\r\nQUIT\r\n");
        String[] ls = lines(out);
        int idx381 = -1, idx481 = -1;
        for (int i = 0; i < ls.length; i++) {
            if (ls[i].startsWith("381 ")) idx381 = i;
            if (ls[i].startsWith("481 ")) idx481 = i;
        }
        assertTrue(idx381 >= 0, "Expected 381 password required");
        assertTrue(idx481 > idx381, "Expected 481 after PASS wrong");
    }

    @Test
    @DisplayName("AUTHINFO USER user -> 381, then PASS pass -> 281")
    void userRequiresPasswordCorrectPass() {
        String out = run("AUTHINFO USER user\r\nAUTHINFO PASS pass\r\nQUIT\r\n");
        String[] ls = lines(out);
        int idx381 = -1, idx281 = -1;
        for (int i = 0; i < ls.length; i++) {
            if (ls[i].startsWith("381 ")) idx381 = i;
            if (ls[i].startsWith("281 ")) idx281 = i;
        }
        assertTrue(idx381 >= 0, "Expected 381 password required");
        assertTrue(idx281 > idx381, "Expected 281 after PASS pass");
    }

    @Test
    @DisplayName("AUTHINFO PASS out of sequence -> 482")
    void passOutOfSequence() {
        String out = run("AUTHINFO PASS something\r\nQUIT\r\n");
        String[] ls = lines(out);
        assertTrue(Arrays.stream(ls).anyMatch(l -> l.startsWith("482 ")), "Expected 482 out of sequence");
    }
}

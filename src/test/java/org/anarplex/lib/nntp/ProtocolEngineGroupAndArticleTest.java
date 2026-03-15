package org.anarplex.lib.nntp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for GROUP selection, LISTGROUP, ARTICLE/HEAD/BODY/STAT and NEXT/LAST navigation
 * using the in-memory persistence and real protocol I/O.
 */
public class ProtocolEngineGroupAndArticleTest {

    private MockIdentityService identity;
    private MockPolicyService policy;
    private InMemoryPersistence store;

    private final String msg1 = "<msg1@test>";
    private final String msg2 = "<msg2@test>";

    @BeforeEach
    void setup() {
        identity = new MockIdentityService();
        policy = new MockPolicyService();
        store = new InMemoryPersistence();

        InMemoryPersistence.TestPublishedNewsgroup group = store.seedGroup("comp.lang.java", "Java discussions", Specification.PostingMode.Allowed);

        Map<String, Set<String>> h1 = minimalHeaders(msg1, "Hello 1");
        Map<String, Set<String>> h2 = minimalHeaders(msg2, "Hello 2");
        store.seedArticle(group, msg1, h1, "First line\r\nSecond line\r\n");
        store.seedArticle(group, msg2, h2, "Body 2\r\n");
    }

    @Test
    @DisplayName("GROUP selects a published group and sets current article to first; STAT reflects it")
    void groupSelectsAndSetsCurrentArticle() {
        String script = "GROUP comp.lang.java\r\nSTAT\r\nQUIT\r\n";
        String out = run(script, identity.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(out);

        // Expect 211 with number, low=1, high=2, and group name
        Optional<String> g = Arrays.stream(lines).filter(l -> l.startsWith("211 ")).findFirst();
        assertTrue(g.isPresent(), "Expected 211 response for GROUP");
        assertTrue(g.get().matches("211 \\d+ 1 2 comp.lang.java"), "Unexpected GROUP metrics: " + g.get());

        // STAT should indicate current article exists with number 1 and msg1 id
        Optional<String> stat = Arrays.stream(lines).filter(l -> l.startsWith("223 ")).findFirst();
        assertTrue(stat.isPresent(), "Expected 223 for STAT");
        assertTrue(stat.get().contains(" 1 "), "STAT should reference article number 1: " + stat.get());
        assertTrue(stat.get().endsWith(msg1), "STAT should return message-id of first article");
    }

    @Test
    @DisplayName("GROUP for non-existent group returns 411")
    void groupNonExistent() {
        String out = run("GROUP no.such\r\nQUIT\r\n", identity.newSubject("poster-allowed"));
        assertTrue(Arrays.stream(nonEmptyLines(out)).anyMatch(l -> l.startsWith("411 ")), "Expected 411 for unknown group");
    }

    @Test
    @DisplayName("ARTICLE by number returns 220 with correct number and message-id and dot-terminated content")
    void articleByNumber() {
        String out = run("GROUP comp.lang.java\r\nARTICLE 2\r\nQUIT\r\n", identity.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(out);
        // Find the 220 line
        String header = Arrays.stream(lines).filter(l -> l.startsWith("220 ")).findFirst().orElse("");
        assertTrue(header.matches("220 2 .*"), "Expected 220 with article number 2: " + header);
        assertTrue(header.endsWith(msg2), "Expected message-id " + msg2 + " in ARTICLE response");
        // Ensure the response contains a terminating dot line
        assertTrue(Arrays.asList(lines).contains("."), "ARTICLE response must be dot-terminated");
    }

    @Test
    @DisplayName("HEAD and BODY operate on current context and return 221/222 respectively")
    void headAndBodyOnCurrent() {
        String out = run("GROUP comp.lang.java\r\nSTAT\r\nHEAD\r\nBODY\r\nQUIT\r\n", identity.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(out);
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("221 ")), "Expected 221 for HEAD");
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("222 ")), "Expected 222 for BODY");
        // Both are multiline and should end with a dot line at least once
        long dotCount = Arrays.stream(lines).filter(l -> l.equals(".")).count();
        assertTrue(dotCount >= 2, "Expected dot terminations for HEAD and BODY");
    }

    @Test
    @DisplayName("NEXT and LAST navigate within the group and handle edges with 421/422")
    void nextAndLastNavigation() {
        String out = run("GROUP comp.lang.java\r\nNEXT\r\nNEXT\r\nLAST\r\nLAST\r\nQUIT\r\n", identity.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(out);
        // First NEXT moves to 2
        String n1 = Arrays.stream(lines).filter(l -> l.startsWith("223 ")).findFirst().orElse("");
        assertTrue(n1.matches("223 2 .*"), "First NEXT should move to article 2: " + n1);
        // Second NEXT has no next
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("421 ")), "Second NEXT should return 421");
        // LAST from 2 moves to 1
        Optional<String> lastOk = Arrays.stream(lines).filter(l -> l.startsWith("223 ")).skip(1).findFirst();
        assertTrue(lastOk.isPresent() && lastOk.get().matches("223 1 .*"), "LAST should move to 1: " + lastOk.orElse(""));
        // LAST from first has no previous
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("422 ")), "LAST from first should return 422");
    }

    // --- helpers ---
    private static Map<String, Set<String>> minimalHeaders(String messageId, String subject) {
        Map<String, Set<String>> h = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        h.put(Specification.NNTP_Standard_Article_Headers.Subject.getValue(), Set.of(subject));
        h.put(Specification.NNTP_Standard_Article_Headers.From.getValue(), Set.of("Tester <test@example.com>"));
        h.put(Specification.NNTP_Standard_Article_Headers.Date.getValue(), Set.of("Wed, 11 Mar 2026 12:00 +0000"));
        h.put(Specification.NNTP_Standard_Article_Headers.MessageID.getValue(), Set.of(messageId));
        h.put(Specification.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Set.of("comp.lang.java"));
        h.put(Specification.NNTP_Standard_Article_Headers.Path.getValue(), Set.of("not-for-mail"));
        return h;
    }

    private String run(String script, IdentityService.Subject subject) {
        try {
            BufferedReader reader = new BufferedReader(new StringReader(script));
            StringWriter out = new StringWriter();
            BufferedWriter writer = new BufferedWriter(out);
            NetworkService.ConnectedClient client = new NetworkService.ConnectedClient(reader, writer, subject);
            ProtocolEngine engine = new ProtocolEngine(store, identity, policy, client);
            engine.start();
            client.flush();
            return out.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String[] nonEmptyLines(String s) {
        return Arrays.stream(s.split("\r?\n")).filter(l -> !l.isEmpty()).toArray(String[]::new);
    }
}

package org.anarplex.lib.nntp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic integration-style tests that exercise {@link ProtocolEngine} over a real pair of in-memory streams
 * using lightweight fake services. Additional test classes will extend coverage of command handlers.
 */
public class ProtocolEngineTest {

    private MockIdentityService identityService;
    private MockPolicyService policyService;
    private InMemoryPersistence persistenceService;

    @BeforeEach
    void setUp() {
        identityService = new MockIdentityService();
        policyService = new MockPolicyService();
        persistenceService = new InMemoryPersistence();
    }

    @Test
    @DisplayName("Sends 200 greeting when posting is allowed; 201 when not allowed")
    void greetingDependsOnPostingPolicy() {
        // subject allowed to post
        TestIO io1 = runWithInput("CAPABILITIES\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String out1 = io1.getOutput();
        assertTrue(out1.startsWith("200 "), "Expected 200 greeting when posting is allowed, got: " + firstLine(out1));

        // subject not allowed to post
        TestIO io2 = runWithInput("CAPABILITIES\r\nQUIT\r\n", identityService.newSubject("poster-denied"));
        String out2 = io2.getOutput();
        assertTrue(out2.startsWith("201 "), "Expected 201 greeting when posting is prohibited, got: " + firstLine(out2));
    }

    @Test
    @DisplayName("Unknown commands respond with 500")
    void unknownCommand() {
        TestIO io = runWithInput("FOOBAR\r\nQUIT\r\n", identityService.newSubject("poster-denied"));
        String out = io.getOutput();
        // First line is greeting (201). Second non-empty line should be 500.
        String[] lines = nonEmptyLines(out);
        assertTrue(lines.length >= 2, "Expected at least two lines of output");
        assertTrue(lines[1].startsWith("500 "), "Expected 500 for unknown command, got: " + lines[1]);
    }

    @Test
    @DisplayName("CAPABILITIES begins with 101 and includes VERSION first, ends with dot line")
    void capabilitiesListsSupportedFeatures() {
        TestIO io = runWithInput("CAPABILITIES\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(lines[0].startsWith("200 ")); // greeting
        assertEquals("101 Current Capabilities:", lines[1]);
        assertEquals("VERSION " + ProtocolEngine.NNTP_VERSION, lines[2]);
        // The multi-line list must be terminated by a single dot
        assertEquals(".", lines[lines.length - 2], "Capabilities list must end with a dot line before QUIT response");
    }

    @Test
    @DisplayName("HELP emits 100 and some help text, terminated by dot line")
    void helpOutputsText() {
        TestIO io = runWithInput("HELP\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("100 ")),
                "Expected initial 100 help response");
        assertTrue(Arrays.asList(lines).contains("."),
                "Expected help text to be terminated by a single dot line");
    }

    @Test
    @DisplayName("DATE returns 111 and a UTC timestamp in yyyyMMddHHmmss format")
    void dateReturnsTimestamp() {
        TestIO io = runWithInput("DATE\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        String dateLine = Arrays.stream(lines)
                .filter(l -> l.startsWith("111 "))
                .findFirst()
                .orElse("");
        assertTrue(dateLine.matches("111 \\d{14}"), "DATE must respond with 111 and a 14-digit UTC timestamp. Got: " + dateLine);
    }

    @Test
    @DisplayName("QUIT closes the session and flushes services without error")
    void quitClosesSession() {
        TestIO io = runWithInput("QUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("205 ")), "Expected 205 goodbye response");
        // Services should be marked closed
        assertTrue(identityService.closed);
        assertTrue(policyService.closed);
        assertTrue(persistenceService.closed);
    }

    @Test
    @DisplayName("LIST with no groups still returns 215 and a terminating dot")
    void listNoGroups() {
        TestIO io = runWithInput("LIST\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        // Greeting then 215 LIST
        assertTrue(lines[0].startsWith("200 ") || lines[0].startsWith("201 "));
        assertEquals("215 LIST ", lines[1]);
        assertEquals(".", lines[2]);
    }

    @Test
    @DisplayName("LIST with wildmat echoes it back in initial 215 line and ends with dot")
    void listWithWildmat() {
        TestIO io = runWithInput("LIST comp.*\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(lines[1].startsWith("215 LIST "));
        assertEquals("215 LIST comp.*", lines[1]);
        assertEquals(".", lines[2]);
    }

    @Test
    @DisplayName("LIST with too many args returns 501")
    void listTooManyArgs() {
        TestIO io = runWithInput("LIST a b\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        // Expect 501 after greeting
        assertTrue(lines[1].startsWith("501 "));
    }

    @Test
    @DisplayName("LIST ACTIVE returns 215 header and dot terminator (no groups)")
    void listActiveEmpty() {
        TestIO io = runWithInput("LIST ACTIVE\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertEquals("215 LIST ACTIVE ", lines[1]);
        assertEquals(".", lines[2]);
    }

    @Test
    @DisplayName("LIST ACTIVE with wildmat echoes it back")
    void listActiveWithWildmat() {
        TestIO io = runWithInput("LIST ACTIVE comp.*\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertEquals("215 LIST ACTIVE comp.*", lines[1]);
        assertEquals(".", lines[2]);
    }

    @Test
    @DisplayName("LIST ACTIVE.TIMES returns 215 header and dot terminator (no groups)")
    void listActiveTimesEmpty() {
        TestIO io = runWithInput("LIST ACTIVE.TIMES\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertEquals("215 LIST ACTIVE.TIMES ", lines[1]);
        assertEquals(".", lines[2]);
    }

    @Test
    @DisplayName("LIST NEWSGROUPS returns 215 header and dot terminator (no groups)")
    void listNewsgroupsEmpty() {
        TestIO io = runWithInput("LIST NEWSGROUPS\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertEquals("215 LIST NEWSGROUPS ", lines[1]);
        assertEquals(".", lines[2]);
    }

    @Test
    @DisplayName("LIST NEWSGROUPS with wildmat echoes it back")
    void listNewsgroupsWithWildmat() {
        TestIO io = runWithInput("LIST NEWSGROUPS comp.*\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertEquals("215 LIST NEWSGROUPS comp.*", lines[1]);
        assertEquals(".", lines[2]);
    }

    @Test
    @DisplayName("LISTGROUP without selected group responds 412")
    void listGroupNoSelection() {
        TestIO io = runWithInput("LISTGROUP\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("412 ")));
    }

    @Test
    @DisplayName("LISTGROUP unknown group responds 411")
    void listGroupUnknownGroup() {
        TestIO io = runWithInput("LISTGROUP no.such.group\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("411 ")));
    }

    @Test
    @DisplayName("MODE READER responds 201 (posting not allowed for null submitter in mock policy)")
    void modeReader() {
        TestIO io = runWithInput("MODE READER\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        // After greeting, expect 201 from MODE READER
        assertTrue(lines[1].startsWith("201 "));
    }

    @Test
    @DisplayName("NEWGROUPS with invalid date/time returns 501")
    void newgroupsInvalid() {
        TestIO io = runWithInput("NEWGROUPS 202601 010203\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("501 ")));
    }

    @Test
    @DisplayName("NEWNEWS with invalid args returns 501")
    void newnewsInvalid() {
        TestIO io = runWithInput("NEWNEWS comp.* bad bad\r\nQUIT\r\n", identityService.newSubject("poster-allowed"));
        String[] lines = nonEmptyLines(io.getOutput());
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.startsWith("501 ")));
    }

    // --- Helpers ---

    private TestIO runWithInput(String clientInput, IdentityService.Subject subject) {
        TestIO io = new TestIO(clientInput, subject);
        NetworkService.ConnectedClient client = io.client;
        ProtocolEngine engine = new ProtocolEngine(persistenceService, identityService, policyService, client);
        engine.start();
        return io;
    }

    private static String firstLine(String s) {
        BufferedReader r = new BufferedReader(new StringReader(s));
        try { return Optional.ofNullable(r.readLine()).orElse(""); } catch (IOException e) { return ""; }
    }

    private static String[] nonEmptyLines(String s) {
        return Arrays.stream(s.split("\\r?\\n"))
                .filter(l -> !l.isEmpty())
                .toArray(String[]::new);
    }

    // --- Test Doubles ---

    private static class TestIO {
        final StringWriter out;
        final NetworkService.ConnectedClient client;

        TestIO(String input, IdentityService.Subject subject) {
            try {
                BufferedReader reader = new BufferedReader(new StringReader(input));
                out = new StringWriter();
                BufferedWriter writer = new BufferedWriter(out);
                this.client = new NetworkService.ConnectedClient(reader, writer, subject);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        String getOutput() {
            try { client.flush(); } catch (Exception ignored) {}
            return out.toString();
        }
    }

    private static class MockIdentityService implements IdentityService {
        private long nextToken = 1L;
        boolean closed = false;

        @Override
        public Subject newSubject(String principal) {
            return new Subject() {
                @Override
                public String getPrincipal() { return principal; }
                @Override
                public long getIdentifier() { return principal.hashCode(); }
            };
        }

        @Override
        public Boolean requiresPassword(Subject subject) {
            if (subject == null) return null;
            if ("nopass".equalsIgnoreCase(subject.getPrincipal())) return false;
            if (subject.getPrincipal() != null) return true;
            return null;
        }

        @Override
        public Long authenticate(Subject subject, String credentials) {
            if (subject == null) return null;
            if ("nopass".equalsIgnoreCase(subject.getPrincipal())) return nextToken++;
            if ("user".equals(subject.getPrincipal()) && "pass".equals(credentials)) return nextToken++;
            return null;
        }

        @Override
        public boolean isValid(long token) {
            return token > 0L && token < nextToken;
        }

        @Override
        public String getHostIdentifier() {
            return "testHost";
        }

        @Override
        public Specification.MessageId createMessageID(Map<String, Set<String>> articleHeaders) {
            // create a deterministic ID for tests
            String seed = String.valueOf(Objects.hashCode(articleHeaders));
            try {
                return new Specification.MessageId('<' + seed + "@test>");
            } catch (Specification.MessageId.InvalidMessageIdException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class MockPolicyService implements PolicyService {
        boolean closed = false;

        @Override
        public boolean isPostingAllowedBy(IdentityService.Subject submitter) {
            return submitter != null && submitter.getPrincipal() != null && submitter.getPrincipal().contains("allowed");
        }

        @Override
        public boolean isIHaveTransferAllowedBy(IdentityService.Subject submitter) {
            return true;
        }

        @Override
        public ArticleReviewOutcome reviewArticle(Specification.Article article, Specification.ArticleSource articleSource, IdentityService.Subject sender) {
            return ArticleReviewOutcome.Allow;
        }

        @Override
        public void reviewPosting(PersistenceService.PendingArticle submission) {
            // accept by default in basic tests
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * Minimal in-memory persistence that satisfies ProtocolEngine interactions used in basic tests.
     * Methods not required by these tests throw UnsupportedOperationException to catch accidental use.
     */
    private static class InMemoryPersistence extends PersistenceService {
        boolean closed = false;

        @Override protected void init() {}
        @Override protected void checkpoint() {}
        @Override protected void rollback() {}
        @Override protected void commit() {}
        @Override protected void close() { closed = true; }

        @Override
        protected PendingArticle addArticle(Specification.Article article, Specification.ArticleSource articleSource, IdentityService.Subject submitter) {
            throw new UnsupportedOperationException("Not used in basic tests");
        }

        @Override
        protected void deleteArticle(Specification.MessageId messageId) { throw new UnsupportedOperationException("Not used"); }

        @Override
        protected StoredArticle getArticle(Specification.MessageId messageId) { throw new UnsupportedOperationException("Not used"); }

        @Override
        protected boolean hasArticle(Specification.MessageId messageId) { return false; }

        @Override
        public Iterator<StoredArticle> listArticles() { return Collections.emptyIterator(); }

        @Override
        protected StoredNewsgroup addNewsgroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, String createdBy) {
            throw new UnsupportedOperationException("Not used");
        }

        @Override
        protected void deleteNewsgroup(StoredNewsgroup newsgroup) { throw new UnsupportedOperationException("Not used"); }

        @Override
        public StoredNewsgroup getNewsgroup(Specification.NewsgroupName name) { return null; }

        @Override
        public Iterator<PublishedNewsgroup> listPublishedGroups() { return Collections.emptyIterator(); }

        @Override
        public Iterator<StoredNewsgroup> listAllGroups() { return Collections.emptyIterator(); }

        @Override
        public Iterator<PublishedNewsgroup> listGroupsAddedSince(Instant since) { return Collections.emptyIterator(); }

        @Override
        public StoredNewsgroup getGroupByName(Specification.NewsgroupName name) { return null; }

        @Override
        public Peer addPeer(String label, String address) throws ExistingPeerException { throw new ExistingPeerException("not used"); }

        @Override
        protected Iterator<Peer> getPeers() { return Collections.emptyIterator(); }

        @Override
        protected void deleteFeed(Feed feed) { throw new UnsupportedOperationException("Not used"); }

        @Override
        protected PendingArticle addAssociation(StoredArticle storedArticle, StoredNewsgroup newsgroup) { throw new UnsupportedOperationException("Not used"); }

        @Override
        protected void deleteAssociation(NewsgroupArticle newsgroupArticle) { throw new UnsupportedOperationException("Not used"); }

        @Override
        protected void insertKey(String key) { /* no-op */ }

        @Override
        protected boolean existsKey(String key) { return false; }

        @Override
        protected void deletePeer(Peer peer) { /* no-op for tests */ }
    }
}

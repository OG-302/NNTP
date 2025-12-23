package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.env.*;
import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolEngineTest {

    private MockPersistenceService persistenceService;
    private MockIdentityService identityService;
    private MockPolicyService policyService;
    private ByteArrayOutputStream serverOutput;

    @BeforeEach
    void setUp() {
        persistenceService = new MockPersistenceService();
        identityService = new MockIdentityService();
        policyService = new MockPolicyService();
        serverOutput = new ByteArrayOutputStream();
    }

    @Test
    @DisplayName("RFC-3977: Initial Handshake and QUIT")
    void testHandshakeAndQuit() throws Exception {
        String input = "QUIT\r\n";
        ProtocolEngine engine = createEngine(input);

        boolean result = engine.start();

        assertTrue(result, "Engine should terminate gracefully on QUIT");
        String output = serverOutput.toString(StandardCharsets.UTF_8);
        assertTrue(output.startsWith("200"), "Server must send 200 greeting (Posting allowed)");
        assertTrue(output.contains("205"), "Server must send 205 on QUIT");
    }

    @Test
    @DisplayName("RFC-3977: GROUP and ARTICLE by Number")
    void testGroupAndArticleFlow() throws Exception {
        // Setup data
        Specification.NewsgroupName groupName = new Specification.NewsgroupName("comp.lang.java");
        persistenceService.addGroup(groupName, "Java news", Specification.PostingMode.Allowed, new Date(), "test", false);

        Specification.MessageId mid = new Specification.MessageId("<test@postus>");
        Map<String, Set<String>> headers = new HashMap<>();
        headers.put("Newsgroups", Collections.singleton(groupName.getValue()));
        headers.put("Subject", Collections.singleton("Hello"));
        headers.put("From", Collections.singleton("tester@example.com"));
        headers.put("Date", Collections.singleton("Fri, 20 Dec 2024 12:00:00 +0000"));
        headers.put("Message-ID", Collections.singleton(mid.getValue()));
        headers.put("Path", Collections.singleton("host!not-for-email"));

        persistenceService.getGroupByName(groupName).addArticle(mid, new Specification.Article.ArticleHeaders(headers), new StringReader("Body content"), false);

        // Sequence: Select group -> Get article 1 -> QUIT
        String input = "GROUP comp.lang.java\r\nARTICLE 1\r\nQUIT\r\n";
        ProtocolEngine engine = createEngine(input);
        engine.start();

        String output = serverOutput.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("211"), "GROUP command should return 211 success");
        assertTrue(output.contains("220 1 <test@postus>"), "ARTICLE command should return 220 with article number and MID");
        assertTrue(output.contains("Body content"), "Response should contain article body");
        assertTrue(output.contains("\r\n.\r\n"), "Multi-line response must end with a single dot line");
    }

    @Test
    @DisplayName("RFC-3977: 411 No Such Newsgroup")
    void testMissingGroup() throws Exception {
        String input = "GROUP non.existent.group\r\nQUIT\r\n";
        ProtocolEngine engine = createEngine(input);
        engine.start();

        String output = serverOutput.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("411"), "Should return 411 for non-existent group");
    }

    @Test
    @DisplayName("RFC-3977: DATE Command")
    void testDateCommand() throws Exception {
        String input = "DATE\r\nQUIT\r\n";
        ProtocolEngine engine = createEngine(input);
        engine.start();

        String output = serverOutput.toString(StandardCharsets.UTF_8);
        // Matches 111 yyyyMMddHHmmss
        assertTrue(output.matches("(?s).*111 \\d{14}.*"), "DATE should return 111 with 14-digit timestamp");
    }

    private ProtocolEngine createEngine(String clientInput) {
        InputStream is = new ByteArrayInputStream(clientInput.getBytes(StandardCharsets.UTF_8));
        NetworkUtils.ProtocolStreams streams = new MockNetworkUtils.MockProtocolStreams(is, serverOutput);
        return new ProtocolEngine(persistenceService, identityService, policyService, streams);
    }
}
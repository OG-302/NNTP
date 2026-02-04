package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.env.MockIdentityService;
import org.anarplex.lib.nntp.env.MockPersistenceService;
import org.anarplex.lib.nntp.env.MockPolicyService;
import org.anarplex.lib.nntp.env.NetworkUtilities;
import org.anarplex.lib.nntp.utils.RandomNumber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void testHandshakeAndQuit() {
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
        Specification.NewsgroupName groupName = new Specification.NewsgroupName("local.test.nntp.g"+ RandomNumber.generate10DigitNumber());
        persistenceService.addGroup(groupName, "Test group", Specification.PostingMode.Allowed, LocalDateTime.now(), "test", false);

        Specification.MessageId mid = new Specification.MessageId("<test@postus>");
        Map<String, Set<String>> headers = new HashMap<>();
        headers.put("Newsgroups", Collections.singleton(groupName.getValue()));
        headers.put("Subject", Collections.singleton("Hello"));
        headers.put("From", Collections.singleton("testcase@example.com"));
        headers.put("Date", Collections.singleton("Thu, 08 Jan 2026 00:01:19"));
        headers.put("Message-ID", Collections.singleton(mid.getValue()));
        headers.put("Path", Collections.singleton("host!not-for-email"));

        persistenceService.getGroupByName(groupName).addArticle(mid, new Specification.Article.ArticleHeaders(headers), "Body content", false);

        // Sequence: Select group -> Get article 1 -> QUIT
        String input = "GROUP "+groupName.getValue() +"\r\nARTICLE 1\r\nQUIT\r\n";
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
    void testMissingGroup() {
        String input = "GROUP non.existent.group\r\nQUIT\r\n";
        ProtocolEngine engine = createEngine(input);
        engine.start();

        String output = serverOutput.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("411"), "Should return 411 for non-existent group");
    }

    @Test
    @DisplayName("RFC-3977: DATE Command")
    void testDateCommand() {
        String input = "DATE\r\nQUIT\r\n";
        ProtocolEngine engine = createEngine(input);
        engine.start();

        String output = serverOutput.toString(StandardCharsets.UTF_8);
        // Matches 111 yyyyMMddHHmmss
        assertTrue(output.matches("(?s).*111 \\d{14}.*"), "DATE should return 111 with 14-digit timestamp");
    }

    private ProtocolEngine createEngine(String clientInput) {
        BufferedReader input = new BufferedReader(new StringReader(clientInput));
        NetworkUtilities.ConnectedClient connectedClient;
        connectedClient = new NetworkUtilities.ConnectedClient(input, new BufferedWriter(new OutputStreamWriter(serverOutput, StandardCharsets.UTF_8)));
        return new ProtocolEngine(persistenceService, identityService, policyService, connectedClient);
    }
}
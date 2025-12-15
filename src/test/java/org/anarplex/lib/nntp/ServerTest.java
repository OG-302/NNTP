package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.ext.PersistenceService;
import org.anarplex.lib.nntp.ext.MockIdentityService;
import org.anarplex.lib.nntp.ext.MockPolicyService;
import org.anarplex.lib.nntp.ext.MockPersistenceService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ServerTest {

    @Test
    void sendResponse() {
        // Create a ByteArrayOutputStream
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        // Wrap the ByteArrayOutputStream with an OutputStreamWriter
        // This converts characters to bytes using the default charset
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(byteArrayOutputStream);

        // Wrap the OutputStreamWriter with a BufferedWriter
        // This adds buffering for efficient writing
        BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);

        // Test the sendResponse method
        byteArrayOutputStream.reset();
        boolean result = Server.sendResponse(bufferedWriter, Spec.NNTP_Response_Code.Code_100);
        assertEquals("100\r\n", byteArrayOutputStream.toString());
        assertTrue(result);

        // Test the sendResponse method with arguments
        byteArrayOutputStream.reset();
        result = Server.sendResponse(bufferedWriter, Spec.NNTP_Response_Code.Code_223, 5, "msg_id_5");
        assertEquals("223 5 msg_id_5\r\n", byteArrayOutputStream.toString());
        assertTrue(result);
    }



    @Test
    void testHandleQuit() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {

            persistenceService.init();

            long l = System.currentTimeMillis();

            // Test 1: QUIT command with no arguments (normal case - should return 205)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT"};
                context.currentGroup = null; // QUIT doesn't require a newsgroup to be selected

                Boolean result = Server.handleQuit(context);
                assertTrue(result, "QUIT should return true");

                String response = responseWriter.toString();
                assertTrue(response.startsWith("205"), "Expected 205 response for QUIT, got: " + response);
                assertEquals("205\r\n", response, "QUIT response should be exactly '205\\r\\n'");
            }

            // Test 2: QUIT command when newsgroup is selected (should still return 205)
            {
                // Create a test newsgroup
                Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".quit.group");
                PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                        testGroupName,
                        "Test group for quit command",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "tester",
                        false
                );
                assertNotNull(newsgroup);

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT"};
                context.currentGroup = newsgroup; // QUIT should work regardless of newsgroup state

                Boolean result = Server.handleQuit(context);
                assertTrue(result, "QUIT should return true even with newsgroup selected");

                String response = responseWriter.toString();
                assertTrue(response.startsWith("205"), "Expected 205 response for QUIT, got: " + response);
                assertEquals("205\r\n", response, "QUIT response should be exactly '205\\r\\n'");
            }

            // Test 3: Verify QUIT response has no additional content
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT"};

                Boolean result = Server.handleQuit(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // QUIT should only return the status line, no multi-line content
                assertEquals(1, lines.length, "QUIT should only return status line");
                assertFalse(response.contains("\r\n.\r\n"), "QUIT should not have dot termination");
            }

            // Test 4: QUIT with extra arguments (implementation may vary - typically should still work)
            // Note: According to RFC 3977, QUIT takes no arguments, but implementations may be lenient
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT", "extra", "arguments"};

                Boolean result = Server.handleQuit(context);
                assertTrue(result, "QUIT should succeed even with extra arguments (lenient implementation)");

                String response = responseWriter.toString();
                // Should still return 205 (most implementations ignore extra arguments for QUIT)
                assertTrue(response.startsWith("205"),
                        "Expected 205 response for QUIT with extra args (lenient), got: " + response);
            }

            // Test 5: Compare QUIT response length with other commands
            {
                // Get QUIT response
                StringWriter quitResponseWriter = new StringWriter();
                Server.ClientContext quitContext = createClientContext(persistenceService, null, quitResponseWriter);
                quitContext.requestArgs = new String[]{"QUIT"};
                Server.handleQuit(quitContext);
                String quitResponse = quitResponseWriter.toString();

                // QUIT response should be very short - just "205\r\n"
                assertEquals("205\r\n", quitResponse, "QUIT should return exactly '205\\r\\n'");

                // Verify it's one of the simplest responses
                assertTrue(quitResponse.length() < 10, "QUIT response should be very short");
            }

            // Test 6: QUIT always succeeds regardless of authentication state
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT"};
                context.authenticationToken = null; // Not authenticated

                Boolean result = Server.handleQuit(context);
                assertTrue(result, "QUIT should succeed even without authentication");

                String response = responseWriter.toString();
                assertEquals("205\r\n", response, "QUIT should return 205 regardless of auth state");
            }

            // Test 7: Multiple QUIT commands (only first should be processed in real scenario)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT"};

                // First QUIT
                Boolean result1 = Server.handleQuit(context);
                assertTrue(result1);
                String response1 = responseWriter.toString();
                assertEquals("205\r\n", response1);

                // Second QUIT (in practice, connection would be closed after first)
                responseWriter.getBuffer().setLength(0); // Clear the buffer
                Boolean result2 = Server.handleQuit(context);
                assertTrue(result2);
                String response2 = responseWriter.toString();
                assertEquals("205\r\n", response2, "QUIT should be idempotent");
            }

            // Test 8: Verify QUIT is case-insensitive (handled by command dispatcher)
            // This test verifies the handler works correctly when called
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"quit"}; // lowercase

                Boolean result = Server.handleQuit(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertEquals("205\r\n", response, "QUIT handler should work regardless of case");
            }

            // Test 9: Verify QUIT doesn't require any specific state
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT"};
                context.currentGroup = null;
                context.authenticationToken = null;

                Boolean result = Server.handleQuit(context);
                assertTrue(result, "QUIT should work with minimal context");

                String response = responseWriter.toString();
                assertEquals("205\r\n", response);
            }

            // Test 10: Verify QUIT returns true (indicating successful processing, not error)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"QUIT"};

                Boolean result = Server.handleQuit(context);

                // QUIT returns true to indicate it processed successfully
                // Note: In NNTP protocol, after QUIT the server closes the connection
                assertTrue(result, "QUIT should return true to indicate successful processing");

                String response = responseWriter.toString();
                assertEquals("205\r\n", response);
            }
        }
    }

    @Test
    void nonExistentCommand() {
        try (
                PersistenceService persistenceService = new MockPersistenceService()
        ) {
            MockIdentityService identityService = new MockIdentityService();
            MockPolicyService policyService = new MockPolicyService();

            String inputString = "NoSuchCommand\r\n";
            InputStream inputStream = new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
            InputStreamReader isr = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8);

            Server server = new Server(persistenceService, identityService, policyService, isr, osw);
            boolean result = server.process();

            assertFalse(result);
            String[] results = os.toString().split("\r\n");
            assertEquals(2, results.length);
            assertTrue(results[0].startsWith("200"));
            assertTrue(results[1].startsWith("500"));
        }
    }

    @Test
    void noCommandInput() {
        try (
                MockIdentityService identityService = new MockIdentityService();
                PersistenceService persistenceService = new MockPersistenceService();
                MockPolicyService policyService = new MockPolicyService()
        ) {

            String inputString = "\r\n";
            InputStream inputStream = new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
            InputStreamReader isr = new InputStreamReader(inputStream, StandardCharsets.UTF_8);

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8);

            Server server = new Server(persistenceService, identityService, policyService, isr, osw);
            boolean result = server.process();

            assertFalse(result);
            String[] results = os.toString().split("\r\n");
            assertEquals(2, results.length);
            assertTrue(results[0].startsWith("200") );
            assertTrue(results[1].startsWith("500"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleArticle() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {
            persistenceService.init();

            long l = System.currentTimeMillis();
            // Create a test newsgroup
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".article.group");
            PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                    testGroupName,
                    "Test group for article retrieval",
                    Spec.PostingMode.Allowed,
                    new Date(),
                    "nntp-test",
                    false
            );
            assertNotNull(newsgroup);

            // Create a test article with all required headers
            Spec.MessageId testMessageId = new Spec.MessageId("<test.article." + l + "@test.com>");
            Map<String, Set<String>> headers = new HashMap<>();
            headers.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(testMessageId.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(testGroupName.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("Test Article Subject"));
            headers.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("test@example.com"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("15 Oct 2025 12:00:00 GMT"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.path"));
            headers.put(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<parent@test>"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("3"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("42"));

            Spec.Article.ArticleHeaders articleHeaders = new Spec.Article.ArticleHeaders(headers);
            String bodyContent = "This is line 1.\r\nThis is line 2.\r\nThis is line 3.";

            PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(
                    testMessageId,
                    articleHeaders,
                    new StringReader(bodyContent),
                    false
            );
            assertNotNull(newsgroupArticle);
            Spec.ArticleNumber n = newsgroup.getArticle(newsgroupArticle.getMessageId());   // move newsgroup article cursor
            assertNotNull(n);
            int articleNumber = n.getValue();

            // Test 1: ARTICLE command without newsgroup selected (should return 412)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE"};
                context.currentGroup = null;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("412"), "Expected 412 when no newsgroup selected, got: " + response);
            }

            // Test 2: ARTICLE command with newsgroup but no current article (should return 420)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE"};

                // Create a newsgroup with no articles
                Spec.NewsgroupName emptyGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".empty.group");
                context.currentGroup = persistenceService.addGroup(
                        emptyGroupName,
                        "Empty test group",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "nntp-test",
                        false
                );

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("420"), "Expected 420 when current article invalid, got: " + response);
            }

            // Test 3: ARTICLE command with current article (should return 220 followed by article)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE"};

                context.currentGroup = newsgroup;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // Check the initial response line
                assertTrue(lines[0].startsWith("220"), "Expected 220 response, got: " + lines[0]);
                assertTrue(lines[0].contains(String.valueOf(articleNumber)), "Response should contain article number");
                assertTrue(lines[0].contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify headers are present
                boolean foundSubject = false;
                boolean foundFrom = false;
                boolean foundMessageId = false;
                int headerEndIndex = 0;

                for (int i = 1; i < lines.length; i++) {
                    if (lines[i].isEmpty()) {
                        headerEndIndex = i;
                        break;
                    }
                    if (lines[i].startsWith("Subject:")) foundSubject = true;
                    if (lines[i].startsWith("From:")) foundFrom = true;
                    if (lines[i].startsWith("Message-Id:")) foundMessageId = true;
                }

                assertTrue(foundSubject, "Subject header should be present");
                assertTrue(foundFrom, "From header should be present");
                assertTrue(foundMessageId, "Message-Id header should be present");
                assertTrue(headerEndIndex > 0, "Should have blank line separating headers from body");

                // Verify termination
                assertTrue(response.endsWith(".\r\n"), "Response should end with dot-termination");
            }

            // Test 4: ARTICLE command with article number (should return 220 followed by article)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE", String.valueOf(articleNumber)};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("220"), "Expected 220 response for valid article number");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");
            }

            // Test 5: ARTICLE command with invalid article number (should return 423)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE", "999999"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423"), "Expected 423 for non-existent article number, got: " + response);
            }

            // Test 6: ARTICLE command with message-id (should return 220 followed by article)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE", testMessageId.getValue()};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("220"), "Expected 220 response for valid message-id");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");
            }

            // Test 7: ARTICLE command with non-existent message-id (should return 430)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE", "<nonexistent@test.com>"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("430"), "Expected 430 for non-existent message-id, got: " + response);
            }

            // Test 8: ARTICLE command with invalid message-id format (should return 430)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE", "invalid-message-id"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423") || response.startsWith("501"),
                        "Expected 423 or 501 for invalid format, got: " + response);
            }

            // Test 9: ARTICLE command with too many arguments (should return 501)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"ARTICLE", "arg1", "arg2"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleArticle(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("501"), "Expected 501 for too many arguments, got: " + response);
            }
        }
    }

    @Test
    void testHandleBody() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {
            persistenceService.init();

            // Create a test newsgroup
            long l = System.currentTimeMillis();
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".body.group");
            PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                    testGroupName,
                    "Test group for body retrieval",
                    Spec.PostingMode.Allowed,
                    new Date(),
                    "nntp-test",
                    false
            );
            assertNotNull(newsgroup);

            // Create a test article with all required headers
            Spec.MessageId testMessageId = new Spec.MessageId("<test.body." + System.currentTimeMillis() + "@test.com>");
            Map<String, Set<String>> headers = new HashMap<>();
            headers.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(testMessageId.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(testGroupName.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("Test Body Subject"));
            headers.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("bodytest@example.com"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("15 Oct 2025 14:00:00 GMT"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.body.path"));
            headers.put(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<ref@test>"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("5"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("100"));

            Spec.Article.ArticleHeaders articleHeaders = new Spec.Article.ArticleHeaders(headers);
            String bodyContent = "First line of body.\r\nSecond line of body.\r\nThird line of body.\r\nFourth line.\r\nFifth line.";

            PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(
                    testMessageId,
                    articleHeaders,
                    new StringReader(bodyContent),
                    false
            );
            assertNotNull(newsgroupArticle);
            Spec.ArticleNumber n = newsgroup.getArticle(newsgroupArticle.getMessageId());  // move newsgroup article cursor
            assertNotNull(n);
            int articleNumber = n.getValue();

            // Test 1: BODY command without newsgroup selected (should return 412)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY"};
                context.currentGroup = null;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("412"), "Expected 412 when no newsgroup selected, got: " + response);
            }

            // Test 2: BODY command with newsgroup but no current article (should return 420)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY"};

                // Create a newsgroup with no articles
                Spec.NewsgroupName emptyGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".empty.body.group");
                PersistenceService.Newsgroup emptyGroup = persistenceService.addGroup(
                        emptyGroupName,
                        "Empty test group for body",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "nntp-test",
                        false
                );
                if (emptyGroup != null) {
                    context.currentGroup = emptyGroup;
                }

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("420"), "Expected 420 when current article invalid, got: " + response);
            }

            // Test 3: BODY command with current article (should return 222 followed by body only)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // Check the initial response line - should be 222 (not 220 like ARTICLE)
                assertTrue(lines[0].startsWith("222"), "Expected 222 response for BODY, got: " + lines[0]);
                assertTrue(lines[0].contains(String.valueOf(articleNumber)), "Response should contain article number");
                assertTrue(lines[0].contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify NO headers are present (body only)
                boolean foundSubject = false;
                boolean foundFrom = false;
                boolean foundMessageId = false;

                for (int i = 1; i < lines.length - 1; i++) { // Skip first (response) and last (terminator) lines
                    if (lines[i].startsWith("Subject:")) foundSubject = true;
                    if (lines[i].startsWith("From:")) foundFrom = true;
                    if (lines[i].startsWith("Message-Id:")) foundMessageId = true;
                }

                assertFalse(foundSubject, "Subject header should NOT be present in BODY response");
                assertFalse(foundFrom, "From header should NOT be present in BODY response");
                assertFalse(foundMessageId, "Message-Id header should NOT be present in BODY response");

                // Verify termination
                assertTrue(response.endsWith(".\r\n"), "Response should end with dot-termination");
            }

            // Test 4: BODY command with article number (should return 222 followed by body only)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY", String.valueOf(articleNumber)};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("222"), "Expected 222 response for valid article number");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify no headers in response
                boolean hasHeaders = false;
                for (int i = 1; i < lines.length - 1; i++) {
                    if (lines[i].contains(":") && (lines[i].startsWith("Subject") ||
                            lines[i].startsWith("From") || lines[i].startsWith("Date"))) {
                        hasHeaders = true;
                        break;
                    }
                }
                assertFalse(hasHeaders, "BODY response should not contain headers");
            }

            // Test 5: BODY command with invalid article number (should return 423)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY", "999999"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423"), "Expected 423 for non-existent article number, got: " + response);
            }

            // Test 6: BODY command with message-id (should return 222 followed by body only)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY", testMessageId.getValue()};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("222"), "Expected 222 response for valid message-id");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");

                // Ensure no headers present
                boolean hasHeaderFormat = false;
                for (int i = 1; i < lines.length - 1; i++) {
                    // Check for typical header format (name: value)
                    if (lines[i].matches("^[A-Za-z-]+:\\s*.+$")) {
                        hasHeaderFormat = true;
                        break;
                    }
                }
                assertFalse(hasHeaderFormat, "BODY response should not have header format lines");
            }

            // Test 7: BODY command with non-existent message-id (should return 430)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY", "<nonexistent.body@test.com>"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("430"), "Expected 430 for non-existent message-id, got: " + response);
            }

            // Test 8: BODY command with invalid message-id format (should return 430 or 423)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY", "invalid-body-id"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423") || response.startsWith("501"),
                        "Expected 423 or 501 for invalid format, got: " + response);
            }

            // Test 9: BODY command with too many arguments (should return 501)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"BODY", "arg1", "arg2"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleBody(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("501"), "Expected 501 for too many arguments, got: " + response);
            }

            // Test 10: Verify BODY returns different response code than ARTICLE
            {
                // First get ARTICLE response
                StringWriter articleResponseWriter = new StringWriter();
                Server.ClientContext articleContext = createClientContext(persistenceService, null, articleResponseWriter);
                articleContext.requestArgs = new String[]{"ARTICLE", String.valueOf(articleNumber)};
                articleContext.currentGroup = newsgroup;
                Server.handleArticle(articleContext);
                String articleResponse = articleResponseWriter.toString();

                // Then get BODY response
                StringWriter bodyResponseWriter = new StringWriter();
                Server.ClientContext bodyContext = createClientContext(persistenceService, null, bodyResponseWriter);
                bodyContext.requestArgs = new String[]{"BODY", String.valueOf(articleNumber)};
                bodyContext.currentGroup = newsgroup;
                Server.handleBody(bodyContext);
                String bodyResponse = bodyResponseWriter.toString();

                // Verify different response codes
                assertTrue(articleResponse.startsWith("220"), "ARTICLE should return 220");
                assertTrue(bodyResponse.startsWith("222"), "BODY should return 222");

                // Verify ARTICLE response is longer (has headers)
                assertTrue(articleResponse.length() > bodyResponse.length(),
                        "ARTICLE response should be longer than BODY response (includes headers)");
            }
        }
    }

    @Test
    void testHandleHead() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {
            persistenceService.init();

            // Create a test newsgroup with unique name
            long l = System.currentTimeMillis();
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".head.group.H");
            PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                    testGroupName,
                    "Test group for head retrieval",
                    Spec.PostingMode.Allowed,
                    new Date(),
                    "nntp-test",
                    false
            );
            assertNotNull(newsgroup);

            // Create a test article with all required headers
            Spec.MessageId testMessageId = new Spec.MessageId("<test.head." + System.currentTimeMillis() + "@test.com>");
            Map<String, Set<String>> headers = new HashMap<>();
            headers.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(testMessageId.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(testGroupName.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("Test Head Subject"));
            headers.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("headtest@example.com"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("15 Oct 2025 16:00:00 GMT"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.head.path"));
            headers.put(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<ref.head@test>"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("4"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("80"));

            Spec.Article.ArticleHeaders articleHeaders = new Spec.Article.ArticleHeaders(headers);
            String bodyContent = "Line one of head test.\r\nLine two of head test.\r\nLine three of head test.\r\nLine four.";

            PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(
                    testMessageId,
                    articleHeaders,
                    new StringReader(bodyContent),
                    false
            );
            assertNotNull(newsgroupArticle);
            Spec.ArticleNumber n = newsgroup.getArticle(newsgroupArticle.getMessageId());  // move newsgroup article cursor
            assertNotNull(n);
            int articleNumber = n.getValue();

            // Test 1: HEAD command without newsgroup selected (should return 412)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD"};
                context.currentGroup = null;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("412"), "Expected 412 when no newsgroup selected, got: " + response);
            }

            // Test 2: HEAD command with newsgroup but no current article (should return 420)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD"};

                // Create a newsgroup with no articles
                Spec.NewsgroupName emptyGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".empty.head.group.E");
                PersistenceService.Newsgroup emptyGroup = persistenceService.addGroup(
                        emptyGroupName,
                        "Empty test group for head",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "nntp-test",
                        false
                );
                if (emptyGroup != null) {
                    context.currentGroup = emptyGroup;
                }

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("420"), "Expected 420 when current article invalid, got: " + response);
            }

            // Test 3: HEAD command with current article (should return 221 followed by headers only)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // Check the initial response line - should be 221 (not 220 like ARTICLE or 222 like BODY)
                assertTrue(lines[0].startsWith("221"), "Expected 221 response for HEAD, got: " + lines[0]);
                assertTrue(lines[0].contains(String.valueOf(articleNumber)), "Response should contain article number");
                assertTrue(lines[0].contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify headers ARE present (headers only)
                boolean foundSubject = false;
                boolean foundFrom = false;
                boolean foundMessageId = false;

                for (int i = 1; i < lines.length - 1; i++) { // Skip first (response) and last (terminator) lines
                    if (lines[i].startsWith("Subject:")) foundSubject = true;
                    if (lines[i].startsWith("From:")) foundFrom = true;
                    if (lines[i].startsWith("Message-Id:")) foundMessageId = true;
                }

                assertTrue(foundSubject, "Subject header SHOULD be present in HEAD response");
                assertTrue(foundFrom, "From header SHOULD be present in HEAD response");
                assertTrue(foundMessageId, "Message-Id header SHOULD be present in HEAD response");

                // Verify NO body content is present - check that body lines are NOT in response
                boolean foundBodyContent = false;
                for (int i = 1; i < lines.length - 1; i++) {
                    if (lines[i].contains("Line one of head test") ||
                            lines[i].contains("Line two of head test")) {
                        foundBodyContent = true;
                        break;
                    }
                }
                assertFalse(foundBodyContent, "Body content should NOT be present in HEAD response");

                // Verify termination
                assertTrue(response.endsWith(".\r\n"), "Response should end with dot-termination");
            }

            // Test 4: HEAD command with article number (should return 221 followed by headers only)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD", String.valueOf(articleNumber)};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("221"), "Expected 221 response for valid article number");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify headers are present
                boolean hasHeaders = false;
                for (int i = 1; i < lines.length - 1; i++) {
                    if (lines[i].contains(":") && (lines[i].startsWith("Subject") ||
                            lines[i].startsWith("From") || lines[i].startsWith("Date"))) {
                        hasHeaders = true;
                        break;
                    }
                }
                assertTrue(hasHeaders, "HEAD response SHOULD contain headers");

                // Verify no body content
                boolean hasBody = false;
                for (int i = 1; i < lines.length - 1; i++) {
                    if (lines[i].contains("Line one of head test")) {
                        hasBody = true;
                        break;
                    }
                }
                assertFalse(hasBody, "HEAD response should NOT contain body");
            }

            // Test 5: HEAD command with invalid article number (should return 423)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD", "999999"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423"), "Expected 423 for non-existent article number, got: " + response);
            }

            // Test 6: HEAD command with message-id (should return 221 followed by headers only)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD", testMessageId.getValue()};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("221"), "Expected 221 response for valid message-id");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify headers present, no body
                boolean hasSubject = Arrays.stream(lines).anyMatch(line -> line.startsWith("Subject:"));
                assertTrue(hasSubject, "HEAD response should contain Subject header");

                boolean hasBodyText = Arrays.stream(lines).anyMatch(line -> line.contains("Line one of head test"));
                assertFalse(hasBodyText, "HEAD response should not contain body text");
            }

            // Test 7: HEAD command with non-existent message-id (should return 430)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD", "<nonexistent.head@test.com>"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("430"), "Expected 430 for non-existent message-id, got: " + response);
            }

            // Test 8: HEAD command with invalid message-id format (should return 430 or 423)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD", "invalid-head-id"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423") || response.startsWith("501"),
                        "Expected 423 or 501 for invalid format, got: " + response);
            }

            // Test 9: HEAD command with too many arguments (should return 501)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"HEAD", "arg1", "arg2"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleHead(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("501"), "Expected 501 for too many arguments, got: " + response);
            }

            // Test 10: Verify HEAD returns different response code than ARTICLE and BODY
            {
                // Get ARTICLE response
                StringWriter articleResponseWriter = new StringWriter();
                Server.ClientContext articleContext = createClientContext(persistenceService, null, articleResponseWriter);
                articleContext.requestArgs = new String[]{"ARTICLE", String.valueOf(articleNumber)};
                articleContext.currentGroup = newsgroup;
                Server.handleArticle(articleContext);
                String articleResponse = articleResponseWriter.toString();

                // Get BODY response
                StringWriter bodyResponseWriter = new StringWriter();
                Server.ClientContext bodyContext = createClientContext(persistenceService, null, bodyResponseWriter);
                bodyContext.requestArgs = new String[]{"BODY", String.valueOf(articleNumber)};
                bodyContext.currentGroup = newsgroup;
                Server.handleBody(bodyContext);
                String bodyResponse = bodyResponseWriter.toString();

                // Get HEAD response
                StringWriter headResponseWriter = new StringWriter();
                Server.ClientContext headContext = createClientContext(persistenceService, null, headResponseWriter);
                headContext.requestArgs = new String[]{"HEAD", String.valueOf(articleNumber)};
                headContext.currentGroup = newsgroup;
                Server.handleHead(headContext);
                String headResponse = headResponseWriter.toString();

                // Verify different response codes
                assertTrue(articleResponse.startsWith("220"), "ARTICLE should return 220");
                assertTrue(bodyResponse.startsWith("222"), "BODY should return 222");
                assertTrue(headResponse.startsWith("221"), "HEAD should return 221");

                // Verify HEAD has headers but no body (shorter than ARTICLE, longer than BODY)
                assertTrue(articleResponse.length() > headResponse.length(),
                        "ARTICLE response should be longer than HEAD response (includes body)");
                assertTrue(headResponse.length() > bodyResponse.length(),
                        "HEAD response should be longer than BODY response (has headers, not body)");
            }
        }
    }

    @Test
    void testHandleStat() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {
            persistenceService.init();

            // Create a test newsgroup with unique name
            long l = System.currentTimeMillis();
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".stat.group.S");
            PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                    testGroupName,
                    "Test group for stat command",
                    Spec.PostingMode.Allowed,
                    new Date(),
                    "nntp-test",
                    false
            );
            assertNotNull(newsgroup);

            // Create a test article with all required headers
            Spec.MessageId testMessageId = new Spec.MessageId("<test.stat." + System.currentTimeMillis() + "@test.com>");
            Map<String, Set<String>> headers = new HashMap<>();
            headers.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(testMessageId.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(testGroupName.getValue()));
            headers.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("Test Stat Subject"));
            headers.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("stattest@example.com"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("15 Oct 2025 18:00:00 GMT"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.stat.path"));
            headers.put(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<ref.stat@test>"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("2"));
            headers.put(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("50"));

            Spec.Article.ArticleHeaders articleHeaders = new Spec.Article.ArticleHeaders(headers);
            String bodyContent = "First line of stat test.\r\nSecond line of stat test.";

            PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(
                    testMessageId,
                    articleHeaders,
                    new StringReader(bodyContent),
                    false
            );
            assertNotNull(newsgroupArticle);
            Spec.ArticleNumber n = newsgroup.getArticle(newsgroupArticle.getMessageId());  // move newsgroup article cursor
            assertNotNull(n);
            int articleNumber = n.getValue();

            // Test 1: STAT command without newsgroup selected (should return 412)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT"};
                context.currentGroup = null;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("412"), "Expected 412 when no newsgroup selected, got: " + response);
            }

            // Test 2: STAT command with newsgroup but no current article (should return 420)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT"};

                // Create a newsgroup with no articles
                Spec.NewsgroupName emptyGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".empty.stat.group.E");
                context.currentGroup = persistenceService.addGroup(
                        emptyGroupName,
                        "Empty test group for stat",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "nntp-test",
                        false
                );

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("420"), "Expected 420 when current article invalid, got: " + response);
            }

            // Test 3: STAT command with current article (should return 223 with NO content)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // Check the response line - should be 223
                assertTrue(lines[0].startsWith("223"), "Expected 223 response for STAT, got: " + lines[0]);
                assertTrue(lines[0].contains(String.valueOf(articleNumber)), "Response should contain article number");
                assertTrue(lines[0].contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify NO headers are present (STAT sends nothing)
                boolean foundHeaders = false;
                for (int i = 1; i < lines.length; i++) {
                    if (lines[i].startsWith("Subject:") || lines[i].startsWith("From:") || lines[i].startsWith("Message-Id:")) {
                        foundHeaders = true;
                        break;
                    }
                }
                assertFalse(foundHeaders, "STAT should NOT send headers");

                // Verify NO body content is present
                boolean foundBodyContent = false;
                for (int i = 1; i < lines.length; i++) {
                    if (lines[i].contains("First line of stat test")) {
                        foundBodyContent = true;
                        break;
                    }
                }
                assertFalse(foundBodyContent, "STAT should NOT send body content");

                // STAT should have only the status line (no termination dot needed since no content follows)
                // Response should be just one line: "223 <articleNum> <messageId>\r\n"
                assertEquals(1, lines.length, "STAT response should contain only the status line");
            }

            // Test 4: STAT command with article number (should return 223 with NO content)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT", String.valueOf(articleNumber)};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("223"), "Expected 223 response for valid article number");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");
                assertTrue(response.contains(String.valueOf(articleNumber)), "Response should contain article number");

                // Verify no content follows the status line
                assertEquals(1, lines.length, "STAT should only return status line, no content");
            }

            // Test 5: STAT command with invalid article number (should return 423)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT", "999999"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423"), "Expected 423 for non-existent article number, got: " + response);
            }

            // Test 6: STAT command with message-id (should return 223 with NO content)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT", testMessageId.getValue()};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("223"), "Expected 223 response for valid message-id");
                assertTrue(response.contains(testMessageId.getValue()), "Response should contain message-id");

                // Verify only status line is returned
                assertEquals(1, lines.length, "STAT should only return status line");

                // Ensure no headers or body
                assertFalse(response.contains("Subject:"), "STAT should not contain headers");
                assertFalse(response.contains("First line of stat test"), "STAT should not contain body");
            }

            // Test 7: STAT command with non-existent message-id (should return 430)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT", "<nonexistent.stat@test.com>"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("430"), "Expected 430 for non-existent message-id, got: " + response);
            }

            // Test 8: STAT command with invalid message-id format (should return 430 or 423)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT", "invalid-stat-id"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("423") || response.startsWith("501"),
                        "Expected 423 or 501 for invalid format, got: " + response);
            }

            // Test 9: STAT command with too many arguments (should return 501)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT", "arg1", "arg2"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("501"), "Expected 501 for too many arguments, got: " + response);
            }

            // Test 10: Verify STAT returns minimal response compared to other commands
            {
                // Get ARTICLE response
                StringWriter articleResponseWriter = new StringWriter();
                Server.ClientContext articleContext = createClientContext(persistenceService, null, articleResponseWriter);
                articleContext.requestArgs = new String[]{"ARTICLE", String.valueOf(articleNumber)};
                articleContext.currentGroup = newsgroup;
                Server.handleArticle(articleContext);
                String articleResponse = articleResponseWriter.toString();

                // Get HEAD response
                StringWriter headResponseWriter = new StringWriter();
                Server.ClientContext headContext = createClientContext(persistenceService, null, headResponseWriter);
                headContext.requestArgs = new String[]{"HEAD", String.valueOf(articleNumber)};
                headContext.currentGroup = newsgroup;
                Server.handleHead(headContext);
                String headResponse = headResponseWriter.toString();

                // Get BODY response
                StringWriter bodyResponseWriter = new StringWriter();
                Server.ClientContext bodyContext = createClientContext(persistenceService, null, bodyResponseWriter);
                bodyContext.requestArgs = new String[]{"BODY", String.valueOf(articleNumber)};
                bodyContext.currentGroup = newsgroup;
                Server.handleBody(bodyContext);
                String bodyResponse = bodyResponseWriter.toString();

                // Get STAT response
                StringWriter statResponseWriter = new StringWriter();
                Server.ClientContext statContext = createClientContext(persistenceService, null, statResponseWriter);
                statContext.requestArgs = new String[]{"STAT", String.valueOf(articleNumber)};
                statContext.currentGroup = newsgroup;
                Server.handleStat(statContext);
                String statResponse = statResponseWriter.toString();

                // Verify different response codes
                assertTrue(articleResponse.startsWith("220"), "ARTICLE should return 220");
                assertTrue(headResponse.startsWith("221"), "HEAD should return 221");
                assertTrue(bodyResponse.startsWith("222"), "BODY should return 222");
                assertTrue(statResponse.startsWith("223"), "STAT should return 223");

                // Verify STAT is the shortest (just status line)
                assertTrue(statResponse.length() < bodyResponse.length(),
                        "STAT response should be shorter than BODY response");
                assertTrue(statResponse.length() < headResponse.length(),
                        "STAT response should be shorter than HEAD response");
                assertTrue(statResponse.length() < articleResponse.length(),
                        "STAT response should be shorter than ARTICLE response");

                // STAT should have no multi-line content (no termination dot)
                assertFalse(statResponse.contains("\r\n.\r\n"),
                        "STAT should not have dot termination (no multi-line content)");

                // Other commands should have termination dots
                assertTrue(articleResponse.contains("\r\n.\r\n"),
                        "ARTICLE should have dot termination");
                assertTrue(headResponse.contains("\r\n.\r\n"),
                        "HEAD should have dot termination");
                assertTrue(bodyResponse.contains("\r\n.\r\n"),
                        "BODY should have dot termination");
            }

            // Test 11: Verify STAT with message-id when article not in current group
            {
                // Create another newsgroup
                Spec.NewsgroupName otherGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".stat.other.group.O");
                PersistenceService.Newsgroup otherGroup = persistenceService.addGroup(
                        otherGroupName,
                        "Other test group for stat",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "nntp-test",
                        false
                );

                // Use STAT with message-id from first newsgroup while in other newsgroup context
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"STAT", testMessageId.getValue()};
                context.currentGroup = otherGroup;

                Boolean result = Server.handleStat(context);
                assertTrue(result);

                String response = responseWriter.toString();
                // Should find article by message-id even if not in current group
                assertTrue(response.startsWith("223"),
                        "STAT with message-id should work across newsgroups, got: " + response);
                assertTrue(response.contains(testMessageId.getValue()),
                        "Response should contain the message-id");
            }
        }
    }

    @Test
    void testHandleDate() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {
            persistenceService.init();

            long l = System.currentTimeMillis();
            // Test 1: DATE command with no arguments (normal case - should return 111 with timestamp)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE"};

                // Record time before calling DATE
                long beforeTime = System.currentTimeMillis();

                Boolean result = Server.handleDate(context);
                assertTrue(result, "DATE should return true");

                // Record time after calling DATE
                long afterTime = System.currentTimeMillis();

                String response = responseWriter.toString();
                assertTrue(response.startsWith("111"), "Expected 111 response for DATE, got: " + response);

                // Parse the response to verify format
                String[] parts = response.trim().split("\\s+");
                assertEquals(2, parts.length, "DATE response should have code and timestamp");
                assertEquals("111", parts[0], "Response code should be 111");

                String timestamp = parts[1];
                assertEquals(14, timestamp.length(), "Timestamp should be 14 characters (yyyyMMddHHmmss)");

                // Verify timestamp is all digits
                assertTrue(timestamp.matches("\\d{14}"), "Timestamp should be 14 digits");

                // Verify timestamp format (yyyyMMddHHmmss)
                int year = Integer.parseInt(timestamp.substring(0, 4));
                int month = Integer.parseInt(timestamp.substring(4, 6));
                int day = Integer.parseInt(timestamp.substring(6, 8));
                int hour = Integer.parseInt(timestamp.substring(8, 10));
                int minute = Integer.parseInt(timestamp.substring(10, 12));
                int second = Integer.parseInt(timestamp.substring(12, 14));

                // Basic sanity checks
                assertTrue(year >= 2025 && year <= 2100, "Year should be reasonable: " + year);
                assertTrue(month >= 1 && month <= 12, "Month should be 1-12: " + month);
                assertTrue(day >= 1 && day <= 31, "Day should be 1-31: " + day);
                assertTrue(hour >= 0 && hour <= 23, "Hour should be 0-23: " + hour);
                assertTrue(minute >= 0 && minute <= 59, "Minute should be 0-59: " + minute);
                assertTrue(second >= 0 && second <= 59, "Second should be 0-59: " + second);
            }

            // Test 2: DATE command with arguments (should return 501 syntax error)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE", "extra"};

                Boolean result = Server.handleDate(context);
                assertTrue(result, "DATE should return true even with error");

                String response = responseWriter.toString();
                assertTrue(response.startsWith("501"), "Expected 501 for DATE with arguments, got: " + response);
            }

            // Test 3: DATE command doesn't require newsgroup selection
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE"};
                context.currentGroup = null; // No newsgroup selected

                Boolean result = Server.handleDate(context);
                assertTrue(result, "DATE should work without newsgroup selection");

                String response = responseWriter.toString();
                assertTrue(response.startsWith("111"), "Expected 111 even without newsgroup, got: " + response);
            }

            // Test 4: DATE command with newsgroup selected (should still work)
            {
                // Create a test newsgroup
                Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".date.group.D");
                PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                        testGroupName,
                        "Test group for date command",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "nntp-test",
                        false
                );

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleDate(context);
                assertTrue(result, "DATE should work with newsgroup selected");

                String response = responseWriter.toString();
                assertTrue(response.startsWith("111"), "Expected 111 with newsgroup, got: " + response);
            }

            // Test 5: Verify DATE response has exactly one line
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE"};

                Boolean result = Server.handleDate(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // DATE should only return the status line with timestamp
                assertEquals(1, lines.length, "DATE should only return one line");
                assertFalse(response.contains("\r\n.\r\n"), "DATE should not have dot termination");
            }

            // Test 6: DATE returns UTC time (verify it's consistent over multiple calls)
            {
                StringWriter responseWriter1 = new StringWriter();
                Server.ClientContext context1 = createClientContext(persistenceService, null, responseWriter1);
                context1.requestArgs = new String[]{"DATE"};

                Server.handleDate(context1);
                String response1 = responseWriter1.toString();
                String timestamp1 = response1.trim().split("\\s+")[1];

                // Wait a short time
                Thread.sleep(1100); // Wait just over 1 second

                StringWriter responseWriter2 = new StringWriter();
                Server.ClientContext context2 = createClientContext(persistenceService, null, responseWriter2);
                context2.requestArgs = new String[]{"DATE"};

                Server.handleDate(context2);
                String response2 = responseWriter2.toString();
                String timestamp2 = response2.trim().split("\\s+")[1];

                // Timestamps should be different (at least 1 second apart)
                assertNotEquals(timestamp1, timestamp2, "Timestamps should differ after 1+ second");

                // Second timestamp should be greater than first
                long time1 = Long.parseLong(timestamp1);
                long time2 = Long.parseLong(timestamp2);
                assertTrue(time2 > time1, "Second timestamp should be later than first");
            }

            // Test 7: DATE with too many arguments
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE", "arg1", "arg2", "arg3"};

                Boolean result = Server.handleDate(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("501"), "Expected 501 for too many arguments, got: " + response);
            }

            // Test 8: Verify DATE timestamp can be parsed as a valid date
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE"};

                Server.handleDate(context);
                String response = responseWriter.toString();
                String timestamp = response.trim().split("\\s+")[1];

                // Parse timestamp back to a Date object
                java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
                dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

                try {
                    Date parsedDate = dateFormat.parse(timestamp);
                    assertNotNull(parsedDate, "Timestamp should be parseable as a date");

                    // Verify the parsed date is close to current time (within 5 seconds)
                    long now = System.currentTimeMillis();
                    long diff = Math.abs(now - parsedDate.getTime());
                    assertTrue(diff < 5000, "Parsed date should be within 5 seconds of current time");
                } catch (java.text.ParseException e) {
                    fail("Timestamp should be parseable: " + timestamp);
                }
            }

            // Test 9: DATE doesn't require authentication
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE"};
                context.authenticationToken = null; // Not authenticated

                Boolean result = Server.handleDate(context);
                assertTrue(result, "DATE should work without authentication");

                String response = responseWriter.toString();
                assertTrue(response.startsWith("111"), "Expected 111 without auth, got: " + response);
            }

            // Test 10: Verify DATE format matches RFC 3977 specification
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"DATE"};

                Server.handleDate(context);
                String response = responseWriter.toString();

                // Response should be: "111 yyyyMMddHHmmss\r\n"
                assertTrue(response.matches("111 \\d{14}\\r\\n"),
                        "DATE response should match pattern '111 yyyyMMddHHmmss\\r\\n', got: " + response);

                // Extract and validate each component
                String timestamp = response.trim().split("\\s+")[1];

                // Year (yyyy)
                String yearStr = timestamp.substring(0, 4);
                int year = Integer.parseInt(yearStr);
                assertTrue(year >= 2025, "Year should be current or future: " + year);

                // Month (MM) - should be 01-12
                String monthStr = timestamp.substring(4, 6);
                int month = Integer.parseInt(monthStr);
                assertTrue(month >= 1 && month <= 12, "Month should be 01-12: " + monthStr);

                // Day (dd) - should be 01-31
                String dayStr = timestamp.substring(6, 8);
                int day = Integer.parseInt(dayStr);
                assertTrue(day >= 1 && day <= 31, "Day should be 01-31: " + dayStr);

                // Hour (HH) - should be 00-23
                String hourStr = timestamp.substring(8, 10);
                int hour = Integer.parseInt(hourStr);
                assertTrue(hour >= 0 && hour <= 23, "Hour should be 00-23: " + hourStr);

                // Minute (mm) - should be 00-59
                String minuteStr = timestamp.substring(10, 12);
                int minute = Integer.parseInt(minuteStr);
                assertTrue(minute >= 0 && minute <= 59, "Minute should be 00-59: " + minuteStr);

                // Second (ss) - should be 00-59
                String secondStr = timestamp.substring(12, 14);
                int second = Integer.parseInt(secondStr);
                assertTrue(second >= 0 && second <= 59, "Second should be 00-59: " + secondStr);
            }
        }
    }

    // Helper method to add a test article
    private void addTestArticle(MockPersistenceService dbSvc, PersistenceService.Newsgroup newsgroup,
                                String messageIdStr, String body) throws Exception {

        if (newsgroup != null && newsgroup.getPostingMode() != Spec.PostingMode.Prohibited) {
            Spec.MessageId messageId = new Spec.MessageId(messageIdStr);

            // Create headers
            Map<String, Set<String>> headers = new HashMap<>();
            headers.put("Message-Id", Set.of(messageIdStr));
            headers.put("From", Set.of("test@example.com"));
            headers.put("Newsgroups", Set.of(newsgroup.getName().getValue()));
            headers.put("Subject", Set.of("Test Subject"));
            headers.put("Date", Set.of("Mon, 01 Jan 2024 12:00:00 +0000"));
            headers.put("Path", Set.of("test.server"));

            Spec.Article.ArticleHeaders articleHeaders = new Spec.Article.ArticleHeaders(headers);
            Reader bodyReader = new StringReader(body);

            newsgroup.addArticle(messageId, articleHeaders, bodyReader, false);
        } else {
            throw new Exception("Cannot add article to prohibited newsgroup");
        }
    }

    @Test
    void testHandleListGroup() {
    }

    @Test
    void testHandleMode() {
    }

    @Test
    void testHandleNewsgroups() {
    }

    @Test
    void testHandleNewNews() {
    }

    @Test
    void testHandleIHave() throws Exception {
        // Set up the database and persistence service
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            // Create a test newsgroup
            long l = System.currentTimeMillis();
            Spec.NewsgroupName groupName = new Spec.NewsgroupName("local.tmp.test."+ l + ".ihave.group");
            PersistenceService.Newsgroup newsgroup = dbSvc.addGroup(
                    groupName,
                    "Test newsgroup for IHAVE",
                    Spec.PostingMode.Allowed,
                    new Date(),
                    "nntp-test",
                    false
            );

            // Test 1: Successful IHAVE - article doesn't exist
            String messageIdStr = "<test-ihave-" + l + "@example.com>";
            String articleContent =
                    "Message-Id: " + messageIdStr + "\r\n" +
                            "From: test@example.com\r\n" +
                            "Newsgroups: local.tmp.none, " + groupName.getValue() + "\r\n" +
                            "Subject: Test IHAVE Article\r\n" +
                            "Date: 01 Jan 2024 12:00:00 GMT\r\n" +
                            "Path: test.server\r\n" +
                            "\r\n" +
                            "This is the body of the test article.\r\n" +
                            ".\r\n";

            StringReader requestReader = new StringReader(articleContent);
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE", messageIdStr};

            // First call should return 335 (send article)
            boolean result = Server.handleIHave(context);
            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.contains("335") && response.contains("235"), "Should request article with 335 code and accept with 235");

            // Verify article was added to the database
            Spec.MessageId messageId = new Spec.MessageId(messageIdStr);
            assertTrue(dbSvc.hasArticle(messageId), "Article should be stored in database");

            // Test 2: IHAVE with article that already exists - should return 435
            requestReader.reset();
            responseWriter = new StringWriter();
            context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE", messageIdStr};

            result = Server.handleIHave(context);
            assertTrue(result);
            response = responseWriter.toString();
            assertTrue(response.startsWith("435"), "Should return 435 for duplicate article");

            // Test 3: IHAVE with invalid message-id format - should return 501
            requestReader.reset();
            responseWriter = new StringWriter();
            context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE", "invalid-message-id"};

            result = Server.handleIHave(context);
            assertTrue(result);
            response = responseWriter.toString();
            assertTrue(response.startsWith("501"), "Should return 501 for invalid message-id");

            // Test 4: IHAVE with no arguments - should return 501
            requestReader.reset();
            responseWriter = new StringWriter();
            context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE"};

            result = Server.handleIHave(context);
            assertTrue(result);
            response = responseWriter.toString();
            assertTrue(response.startsWith("501"), "Should return 501 for missing argument");

            // Test 5: IHAVE with too many arguments - should return 501
            requestReader.reset();
            responseWriter = new StringWriter();
            context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE", "<test@example.com>", "extra"};

            result = Server.handleIHave(context);
            assertTrue(result);
            response = responseWriter.toString();
            assertTrue(response.startsWith("501"), "Should return 501 for too many arguments");

            // Test 6: IHAVE with mismatched Message-ID in headers - should return 437
            String mismatchedMessageId = "<test-ihave-mismatch@example.com>";
            String mismatchedArticle =
                    "Message-Id: <different@example.com>\r\n" +
                            "From: test@example.com\r\n" +
                            "Newsgroups: local.tmp.none, " + groupName.getValue() + "\r\n" +
                            "Subject: Mismatched Message-ID\r\n" +
                            "Date: 01 Jan 2024 12:00:00 GMT\r\n" +
                            "Path: test.server\r\n" +
                            "\r\n" +
                            "This article has a mismatched Message-ID.\r\n" +
                            ".\r\n";

            requestReader = new StringReader(mismatchedArticle);
            responseWriter = new StringWriter();
            context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE", mismatchedMessageId};

            result = Server.handleIHave(context);
            response = responseWriter.toString();
            assertTrue(response.contains("335") && response.contains("437"),
                    "Should request article then reject for mismatch");

            // Test 7: IHAVE with rejected article - should return 435
            String rejectedMessageId = "<test-rejected@example.com>";
            Spec.MessageId rejectedMsgId = new Spec.MessageId(rejectedMessageId);
            dbSvc.rejectArticle(rejectedMsgId);

            requestReader.reset();
            responseWriter = new StringWriter();
            context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE", rejectedMessageId};

            result = Server.handleIHave(context);
            assertTrue(result);
            response = responseWriter.toString();
            assertTrue(response.startsWith("435"), "Should return 435 for rejected article");

            // Test 8: IHAVE with nonexistent newsgroup in headers - should return 437
            String nonexistentGroupMessageId = "<test-nonexistent-group@example.com>";
            String nonexistentGroupArticle =
                    "Message-Id: " + nonexistentGroupMessageId + "\r\n" +
                            "From: test@example.com\r\n" +
                            "Newsgroups: local.tmp.none\r\n" +
                            "Subject: Nonexistent Newsgroup\r\n" +
                            "Date: 01 Jan 2024 12:00:00 GMT\r\n" +
                            "Path: test.server\r\n" +
                            "\r\n" +
                            "This article references a nonexistent newsgroup.\r\n" +
                            ".\r\n";

            requestReader = new StringReader(nonexistentGroupArticle);
            responseWriter = new StringWriter();
            context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"IHAVE", nonexistentGroupMessageId};

            result = Server.handleIHave(context);
            response = responseWriter.toString();
            assertTrue(response.contains("335") && response.contains("437"),
                    "Should request article then reject for nonexistent newsgroup");

        }
    }

    @Test
    void handleList() {
    }

    @Test
    void handleListGroup() {
    }

    @Test
    void handleMode() {

    }

    @Test
    void handleNewsgroups() {
    }

    @Test
    void handleNewNews() {
    }


    @Test
    void handleOverview() {
    }

    @Test
    void handlePost() {
    }

    @Test
    void handleXOver() {
    }

    @Test
    void process() {
    }

    @Test
    @DisplayName("Test GROUP command - successful selection")
    void testHandleGroup() throws Exception {
        // Setup persistence service with a test newsgroup
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            // Create a test newsgroup
            long l = System.currentTimeMillis();
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".group.selection.L");
            dbSvc.addGroup(testGroupName, "Test group for GROUP command",
                    Spec.PostingMode.Allowed, new Date(), "test", false);

            // Add some test articles to the group
            PersistenceService.Newsgroup newsgroup = dbSvc.getGroupByName(testGroupName);
            addTestArticle(dbSvc, newsgroup, "<test1."+l+"@example.com>", "Test article 1");
            addTestArticle(dbSvc, newsgroup, "<test2."+l+"@example.com>", "Test article 2");
            addTestArticle(dbSvc, newsgroup, "<test3."+l+"@example.com>", "Test article 3");

            // Prepare request and response
            StringReader requestReader = new StringReader("GROUP " + testGroupName.getValue() + "\r\n");
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP", testGroupName.getValue()};

            // Execute the GROUP command
            Boolean result = Server.handleGroup(context);

            // Verify the result
            assertTrue(result);

            // Verify the response format: 211 count low high group
            String response = responseWriter.toString();
            assertTrue(response.startsWith("211"));
            assertTrue(response.contains(testGroupName.getValue()));

            // Verify that the current group is set
            assertNotNull(context.currentGroup);
            assertEquals(testGroupName, context.currentGroup.getName());

            // Verify the response contains correct article count and water marks
            String[] responseParts = response.trim().split("\\s+");
            assertEquals(5, responseParts.length); // 211 count low high group
            assertEquals("211", responseParts[0]); // Response code
            assertEquals("3", responseParts[1]); // Article count
            assertEquals("1", responseParts[2]); // Low water mark
            assertEquals("3", responseParts[3]); // High water mark
            assertEquals(testGroupName.getValue(), responseParts[4]); // Group name
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - newsgroup doesn't exist")
    void testHandleGroupNonExistent()  {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();
            String groupName = "local.tmp.test.group.nonexistent";

            StringReader requestReader = new StringReader("GROUP " + groupName + "\r\n");
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP", groupName};

            Boolean result = Server.handleGroup(context);

            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.startsWith("411")); // No such newsgroup

            // Verify current group is not set
            assertNull(context.currentGroup);
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - empty newsgroup")
    void testHandleGroupEmpty() throws Exception {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            // Create an empty newsgroup
            long l = System.currentTimeMillis();
            Spec.NewsgroupName emptyGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".empty.group");
            dbSvc.addGroup(emptyGroupName, "Empty test group",
                    Spec.PostingMode.Allowed, new Date(), "test", false);

            StringReader requestReader = new StringReader("GROUP " + emptyGroupName.getValue() + "\r\n");
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP", emptyGroupName.getValue()};

            Boolean result = Server.handleGroup(context);

            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.startsWith("211"));

            // Verify response for empty group: count=0, low=0, high=-1
            String[] responseParts = response.trim().split("\\s+");
            assertEquals("211", responseParts[0]);
            assertEquals("0", responseParts[1]); // Article count is 0
            assertEquals("0", responseParts[2]); // Low water mark is 0
            assertEquals("-1", responseParts[3]); // High water mark is -1 for empty groups
            assertEquals(emptyGroupName.getValue(), responseParts[4]);

            // Verify current group is set even though it's empty
            assertNotNull(context.currentGroup);
            assertEquals(emptyGroupName, context.currentGroup.getName());
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - syntax error (no arguments)")
    void testHandleGroupNoArguments() {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            StringReader requestReader = new StringReader("GROUP\r\n");
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP"};

            Boolean result = Server.handleGroup(context);

            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.startsWith("501")); // Syntax error
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - syntax error (too many arguments)")
    void testHandleGroupTooManyArguments() {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            StringReader requestReader = new StringReader("GROUP test.group extra\r\n");
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP", "test.group", "extra"};

            Boolean result = Server.handleGroup(context);

            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.startsWith("501")); // Syntax error
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - invalid newsgroup name")
    void testHandleGroupInvalidName() {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            dbSvc.init();

            // Test with invalid newsgroup name (contains invalid characters)
            StringReader requestReader = new StringReader("GROUP invalid..group\r\n");
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP", "invalid..group"};

            Boolean result = Server.handleGroup(context);

            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.startsWith("501")); // Syntax error due to invalid newsgroup name
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - ignored newsgroup")
    void testHandleGroupIgnored() throws Exception {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            // Create a newsgroup and mark it as ignored
            long l = System.currentTimeMillis();
            Spec.NewsgroupName ignoredGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".ignored.group");
            dbSvc.addGroup(ignoredGroupName, "Ignored test group",
                    Spec.PostingMode.Allowed, new Date(), "test", false);

            PersistenceService.Newsgroup newsgroup = dbSvc.getGroupByName(ignoredGroupName);
            newsgroup.setIgnored(true);

            StringReader requestReader = new StringReader("GROUP "+ignoredGroupName.getValue()+"\r\n");
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP", ignoredGroupName.getValue()};

            Boolean result = Server.handleGroup(context);

            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.startsWith("411")); // No such newsgroup (ignored groups are treated as non-existent)
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - switching between groups")
    void testHandleGroupSwitching() throws Exception {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            // Create two test newsgroups
            long l = System.currentTimeMillis();
            Spec.NewsgroupName group1Name = new Spec.NewsgroupName("local.tmp.test."+l+".group.one");
            Spec.NewsgroupName group2Name = new Spec.NewsgroupName("local.tmp.test."+l+".group.two");

            dbSvc.addGroup(group1Name, "Test group 1",
                    Spec.PostingMode.Allowed, new Date(), "test", false);
            dbSvc.addGroup(group2Name, "Test group 2",
                    Spec.PostingMode.Allowed, new Date(), "test", false);

            // Add articles to both groups
            PersistenceService.Newsgroup newsgroup1 = dbSvc.getGroupByName(group1Name);
            addTestArticle(dbSvc, newsgroup1, "<group1-article."+l+"@example.com>", "Group 1 article");

            PersistenceService.Newsgroup newsgroup2 = dbSvc.getGroupByName(group2Name);
            addTestArticle(dbSvc, newsgroup2, "<group2-article."+l+"@example.com>", "Group 2 article");

            // Select first group
            StringWriter responseWriter1 = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc,
                    new StringReader("GROUP "+group1Name.getValue()+"\r\n"),
                    responseWriter1);
            context.requestArgs = new String[]{"GROUP", group1Name.getValue()};

            Boolean result1 = Server.handleGroup(context);
            assertTrue(result1);
            assertNotNull(context.currentGroup);
            assertEquals(group1Name, context.currentGroup.getName());

            // Switch to second group
            StringWriter responseWriter2 = new StringWriter();
            context.responseStream = new BufferedWriter(responseWriter2);
            context.requestArgs = new String[]{"GROUP", group2Name.getValue()};

            Boolean result2 = Server.handleGroup(context);
            assertTrue(result2);
            assertNotNull(context.currentGroup);
            assertEquals(group2Name, context.currentGroup.getName());

            String response2 = responseWriter2.toString();
            assertTrue(response2.startsWith("211 "));
            assertTrue(response2.contains(group2Name.getValue()));
        }
    }
    
    @Test
    @DisplayName("Test GROUP command - case insensitivity")
    void testHandleGroupCaseInsensitive() throws Exception {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {
            dbSvc.init();

            long l = System.currentTimeMillis();
            Spec.NewsgroupName groupName = new Spec.NewsgroupName("local.tmp.test."+l+".case.Insensitive");

            // Create a test newsgroup with lowercase name
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName(groupName.getValue());
            dbSvc.addGroup(testGroupName, "Test case insensitivity",
                    Spec.PostingMode.Allowed, new Date(), "test", false);

            // Try to select it with different case
            StringReader requestReader = new StringReader(groupName.getValue());
            StringWriter responseWriter = new StringWriter();

            Server.ClientContext context = createClientContext(dbSvc, requestReader, responseWriter);
            context.requestArgs = new String[]{"GROUP", groupName.getValue().toLowerCase()};

            Boolean result = Server.handleGroup(context);

            assertTrue(result);
            String response = responseWriter.toString();
            assertTrue(response.startsWith("211 "));

            // Verify the group was found and selected
            assertNotNull(context.currentGroup);
            // Note: newsgroup names are stored in lowercase
            assertEquals(groupName.getValue(), context.currentGroup.getName().getValue());
        }
    }

    @Test
    void testHandleHelp() {
    }

    @Test
    void testHandleLast() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {
            persistenceService.init();

            // Create a test newsgroup with unique name
            long l = System.currentTimeMillis();
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".last.group");
            PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                    testGroupName,
                    "Test group for last command",
                    Spec.PostingMode.Allowed,
                    new Date(),
                    "nntp-test",
                    false
            );
            assertNotNull(newsgroup);

            // Create multiple test articles
            Spec.MessageId[] messageIds = new Spec.MessageId[5];
            for (int i = 0; i < 5; i++) {
                messageIds[i] = new Spec.MessageId("<test.last." + i + "." + System.currentTimeMillis() + "@test.com>");
                Map<String, Set<String>> headers = new HashMap<>();
                headers.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(messageIds[i].getValue()));
                headers.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(testGroupName.getValue()));
                headers.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("Test Last Article " + i));
                headers.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("lasttest@example.com"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("15 Oct 2025 22:00:00 GMT"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.last.path"));
                headers.put(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<ref.last@test>"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("1"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("20"));

                Spec.Article.ArticleHeaders articleHeaders = new Spec.Article.ArticleHeaders(headers);
                String bodyContent = "Article " + i + " body.";

                PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(
                        messageIds[i],
                        articleHeaders,
                        new StringReader(bodyContent),
                        false
                );
                assertNotNull(newsgroupArticle);
            }

            // Test 1: LAST command without newsgroup selected (should return 412)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"LAST"};
                context.currentGroup = null;

                Boolean result = Server.handleLast(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("412"), "Expected 412 when no newsgroup selected, got: " + response);
            }

            // Test 2: LAST command with newsgroup but no current article (should return 420)
            {
                // Create empty newsgroup
                Spec.NewsgroupName emptyGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".empty.last.group");
                PersistenceService.Newsgroup emptyGroup = persistenceService.addGroup(
                        emptyGroupName,
                        "Empty test group for last",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "nntp-test",
                        false
                );

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"LAST"};
                context.currentGroup = emptyGroup;

                Boolean result = Server.handleLast(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("420"), "Expected 420 when no current article, got: " + response);
            }

            // Test 3: LAST at first article returns 422 (no previous article)
            {
                // Position at first article
                newsgroup.getArticle(messageIds[0]);
                PersistenceService.NewsgroupArticle current = newsgroup.getCurrentArticle();
                assertEquals(1, current.getArticleNumber().getValue(), "Should be at first article");

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"LAST"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleLast(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("422"), "Expected 422 when no previous article, got: " + response);

                // Verify cursor stays at first article
                PersistenceService.NewsgroupArticle stillCurrent = newsgroup.getCurrentArticle();
                assertEquals(1, stillCurrent.getArticleNumber().getValue(), "Cursor should remain at first article");
            }

            // Test 4: LAST command moves from second to first article (should return 223)
            {
                // Position at second article
                newsgroup.getArticle(messageIds[1]);
                PersistenceService.NewsgroupArticle current = newsgroup.getCurrentArticle();
                assertEquals(2, current.getArticleNumber().getValue(), "Should be at second article");

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"LAST"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleLast(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("223"), "Expected 223 response for LAST, got: " + response);

                // Parse response: "223 <articleNum> <messageId>"
                String[] parts = lines[0].split("\\s+");
                assertEquals(3, parts.length, "LAST response should have code, article number, and message-id");
                assertEquals("223", parts[0]);

                int articleNum = Integer.parseInt(parts[1]);
                assertEquals(1, articleNum, "Should move to article 1");

                String msgId = parts[2];
                assertEquals(messageIds[0].getValue(), msgId, "Should return first article's message-id");

                // Verify cursor moved
                PersistenceService.NewsgroupArticle newCurrent = newsgroup.getCurrentArticle();
                assertEquals(1, newCurrent.getArticleNumber().getValue());
                assertEquals(messageIds[0], newCurrent.getMessageId());
            }

            // Test 5: Sequential LAST commands traverse backwards through all articles
            {
                // Position at last article (5)
                newsgroup.getArticle(messageIds[4]);
                assertEquals(5, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                for (int i = 0; i < 4; i++) { // We have 5 articles, so 4 LAST commands to reach first
                    StringWriter responseWriter = new StringWriter();
                    Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                    context.requestArgs = new String[]{"LAST"};
                    context.currentGroup = newsgroup;

                    Boolean result = Server.handleLast(context);
                    assertTrue(result);

                    String response = responseWriter.toString();
                    assertTrue(response.startsWith("223"), "Expected 223 for LAST " + (i + 1));

                    String[] parts = response.trim().split("\\s+");
                    int expectedArticleNum = 5 - (i + 1); // Previous article
                    assertEquals(expectedArticleNum, Integer.parseInt(parts[1]),
                            "LAST " + (i + 1) + " should move to article " + expectedArticleNum);
                    assertEquals(messageIds[expectedArticleNum - 1].getValue(), parts[2]);
                }

                // Should now be at first article
                assertEquals(1, newsgroup.getCurrentArticle().getArticleNumber().getValue());
            }

            // Test 6: LAST with arguments (should be ignored, LAST takes no arguments)
            {
                // Position at third article
                newsgroup.getArticle(messageIds[2]);

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"LAST", "extra", "arguments"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleLast(context);
                assertTrue(result);

                String response = responseWriter.toString();
                // LAST ignores arguments and proceeds normally
                assertTrue(response.startsWith("223"), "LAST should ignore extra arguments, got: " + response);
                assertTrue(response.contains(" 2 "), "Should move to article 2");
            }

            // Test 7: LAST response format (223 with article number and message-id only)
            {
                // Position at second article
                newsgroup.getArticle(messageIds[1]);

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"LAST"};
                context.currentGroup = newsgroup;

                Server.handleLast(context);
                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // LAST should only return status line (no multi-line content)
                assertEquals(1, lines.length, "LAST should only return status line");
                assertFalse(response.contains("\r\n.\r\n"), "LAST should not have dot termination");

                // Verify format: "223 <num> <msgid>\r\n"
                assertTrue(response.matches("223 \\d+ <.+>\\r\\n"),
                        "LAST response should match '223 <num> <msgid>\\r\\n'");
            }

            // Test 8: LAST updates current article pointer
            {
                // Position at last article (5)
                newsgroup.getArticle(messageIds[4]);
                assertEquals(5, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // First LAST
                StringWriter responseWriter1 = new StringWriter();
                Server.ClientContext context1 = createClientContext(persistenceService, null, responseWriter1);
                context1.requestArgs = new String[]{"LAST"};
                context1.currentGroup = newsgroup;
                Server.handleLast(context1);

                assertEquals(4, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Current article should be 4 after first LAST");

                // Second LAST
                StringWriter responseWriter2 = new StringWriter();
                Server.ClientContext context2 = createClientContext(persistenceService, null, responseWriter2);
                context2.requestArgs = new String[]{"LAST"};
                context2.currentGroup = newsgroup;
                Server.handleLast(context2);

                assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Current article should be 3 after second LAST");
            }

            // Test 9: LAST and NEXT are complementary operations
            {
                // Position at middle article (3)
                newsgroup.getArticle(messageIds[2]);
                assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // Go forward with NEXT
                StringWriter nextResponseWriter = new StringWriter();
                Server.ClientContext nextContext = createClientContext(persistenceService, null, nextResponseWriter);
                nextContext.requestArgs = new String[]{"NEXT"};
                nextContext.currentGroup = newsgroup;
                Server.handleNext(nextContext);
                assertEquals(4, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // Go back with LAST
                StringWriter lastResponseWriter = new StringWriter();
                Server.ClientContext lastContext = createClientContext(persistenceService, null, lastResponseWriter);
                lastContext.requestArgs = new String[]{"LAST"};
                lastContext.currentGroup = newsgroup;
                Server.handleLast(lastContext);
                assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "LAST should undo NEXT");
            }

            // Test 10: Multiple LAST commands at beginning stay at first article and return 422
            {
                // Position at first article
                newsgroup.getArticle(messageIds[0]);
                assertEquals(1, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // First LAST at beginning
                StringWriter responseWriter1 = new StringWriter();
                Server.ClientContext context1 = createClientContext(persistenceService, null, responseWriter1);
                context1.requestArgs = new String[]{"LAST"};
                context1.currentGroup = newsgroup;
                Server.handleLast(context1);

                String response1 = responseWriter1.toString();
                assertTrue(response1.startsWith("422"), "First LAST at beginning should return 422");
                assertEquals(1, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Should stay at article 1");

                // Second LAST at beginning
                StringWriter responseWriter2 = new StringWriter();
                Server.ClientContext context2 = createClientContext(persistenceService, null, responseWriter2);
                context2.requestArgs = new String[]{"LAST"};
                context2.currentGroup = newsgroup;
                Server.handleLast(context2);

                String response2 = responseWriter2.toString();
                assertTrue(response2.startsWith("422"), "Second LAST at beginning should also return 422");
                assertEquals(1, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Should still stay at article 1");
            }

            // Test 11: LAST works after using STAT on middle article
            {
                // Use STAT to jump to middle article (article 4)
                StringWriter statResponseWriter = new StringWriter();
                Server.ClientContext statContext = createClientContext(persistenceService, null, statResponseWriter);
                statContext.requestArgs = new String[]{"STAT", "4"};
                statContext.currentGroup = newsgroup;
                Server.handleStat(statContext);

                assertEquals(4, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // Now LAST should go to article 3
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"LAST"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleLast(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("223"));
                assertTrue(response.contains(" 3 "), "Should move to article 3");
                assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue());
            }

            // Test 12: LAST and NEXT traverse in opposite directions
            {
                // Reset to first article
                newsgroup.getArticle(messageIds[0]);

                // Use NEXT to go forward to article 3
                for (int i = 0; i < 2; i++) {
                    StringWriter nextWriter = new StringWriter();
                    Server.ClientContext nextContext = createClientContext(persistenceService, null, nextWriter);
                    nextContext.requestArgs = new String[]{"NEXT"};
                    nextContext.currentGroup = newsgroup;
                    Server.handleNext(nextContext);
                }
                assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // Use LAST to go backward to article 1
                for (int i = 0; i < 2; i++) {
                    StringWriter lastWriter = new StringWriter();
                    Server.ClientContext lastContext = createClientContext(persistenceService, null, lastWriter);
                    lastContext.requestArgs = new String[]{"LAST"};
                    lastContext.currentGroup = newsgroup;
                    Server.handleLast(lastContext);
                }
                assertEquals(1, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "LAST should traverse backwards");
            }

            // Test 13: LAST response is same format as NEXT and STAT (all return 223)
            {
                // Position at third article
                newsgroup.getArticle(messageIds[2]);

                // Get LAST response
                StringWriter lastResponseWriter = new StringWriter();
                Server.ClientContext lastContext = createClientContext(persistenceService, null, lastResponseWriter);
                lastContext.requestArgs = new String[]{"LAST"};
                lastContext.currentGroup = newsgroup;
                Server.handleLast(lastContext);
                String lastResponse = lastResponseWriter.toString();

                // Get STAT response for same article
                StringWriter statResponseWriter = new StringWriter();
                Server.ClientContext statContext = createClientContext(persistenceService, null, statResponseWriter);
                statContext.requestArgs = new String[]{"STAT"};
                statContext.currentGroup = newsgroup;
                Server.handleStat(statContext);
                String statResponse = statResponseWriter.toString();

                // Both should start with 223
                assertTrue(lastResponse.startsWith("223"), "LAST should return 223");
                assertTrue(statResponse.startsWith("223"), "STAT should return 223");

                // Both should have same format (single line with article num and message-id)
                assertEquals(1, lastResponse.split("\r\n").length, "LAST should be single line");
                assertEquals(1, statResponse.split("\r\n").length, "STAT should be single line");

                // Both should have same structure
                String[] lastParts = lastResponse.trim().split("\\s+");
                String[] statParts = statResponse.trim().split("\\s+");
                assertEquals(3, lastParts.length, "LAST should have 3 parts");
                assertEquals(3, statParts.length, "STAT should have 3 parts");
            }
        }
    }

    @Test
    @DisplayName("Test LIST command - basic (LIST ACTIVE)")
    void testHandleList() throws Exception {
        // Create mock persistence services
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            // Add some test newsgroups
            long l = System.currentTimeMillis();
            Spec.NewsgroupName group1 = new Spec.NewsgroupName("local.tmp.test." + l+ ".comp.lang.java");
            Spec.NewsgroupName group2 = new Spec.NewsgroupName("local.tmp.test." + l+ ".alt.test");
            Spec.NewsgroupName group3 = new Spec.NewsgroupName("local.tmp.test." + l + ".misc.test");

            Date now = new Date();
            PersistenceService.Newsgroup ng1 = dbSvc.addGroup(group1, "Java programming",
                    Spec.PostingMode.Allowed, now, "nntp-test", false);
            addTestArticle(dbSvc, ng1, "<Message-ID-1."+l+">", "Test article 1");
            addTestArticle(dbSvc, ng1, "<Message-ID-2."+l+">", "Test article 2");
            PersistenceService.Newsgroup ng2 = dbSvc.addGroup(group2, "Alt testing",
                    Spec.PostingMode.Prohibited, now, "nntp-test", false);
            PersistenceService.Newsgroup ng3 = dbSvc.addGroup(group3, "Misc testing",
                    Spec.PostingMode.Moderated, now, "nntp-test", false);
            addTestArticle(dbSvc, ng3, "<Message-ID-3."+l+">", "Test article 3");

            // Create client context
            StringWriter responseWriter = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc, null, responseWriter);
            context.requestArgs = new String[]{"LIST"};

            // Execute the handler
            Boolean result = Server.handleList(context);

            // Verify the result
            assertTrue(result);

            String response = responseWriter.toString();

            // Check for 215 response code (information follows)
            assertTrue(response.startsWith("215"), "Should start with 215 response code");

            // Check that all newsgroups are present
            assertTrue(response.contains(group1.getValue()), "Should contain comp.lang.java");
            assertTrue(response.contains(group2.getValue()), "Should contain alt.test");
            assertTrue(response.contains(group3.getValue()), "Should contain misc.test");

            // Check posting status flags
            assertTrue(response.contains(" y\r\n") || response.contains(" y "),
                    "Should contain 'y' for allowed posting");
            assertTrue(response.contains(" n\r\n") || response.contains(" n "),
                    "Should contain 'n' for prohibited posting");
            assertTrue(response.contains(" m\r\n") || response.contains(" m "),
                    "Should contain 'm' for moderated posting");

            // Check for termination line
            assertTrue(response.endsWith(".\r\n"), "Should end with dot terminator");

            // Verify format: groupname high low status
            String[] lines = response.split("\r\n");
            assertTrue(lines.length >= 4, "Should have at least response code + 3 groups + terminator");
        }
    }

    // @Test
    @DisplayName("Test LIST ACTIVE command (explicit)")
    void testHandleListActive() throws Exception {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            // Add test newsgroup
            long l = System.currentTimeMillis();
            Spec.NewsgroupName group = new Spec.NewsgroupName("local.tmp.test." + l + ".comp.lang.c");
            Date now = new Date();
            PersistenceService.Newsgroup ng = dbSvc.addGroup(group, "C programming",
                    Spec.PostingMode.Allowed, now, "nntp-test", false);

            addTestArticle(dbSvc, ng, "<Message-ID-Test-" + l + ">", "Test article");

            // Create client context with LIST ACTIVE
            StringWriter responseWriter = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc, null, responseWriter);
            context.requestArgs = new String[]{"LIST", "ACTIVE"};

            // Execute the handler
            Boolean result = Server.handleList(context);

            assertTrue(result);

            String response = responseWriter.toString();
            assertTrue(response.startsWith("215"));
            assertTrue(response.contains(group.getValue()));
            assertTrue(response.endsWith(".\r\n"));
        }
    }

    @Test
    @DisplayName("Test LIST NEWSGROUPS command")
    void testHandleListNewsgroups() throws Exception {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            // Add test newsgroups with descriptions
            long l = System.currentTimeMillis();
            Spec.NewsgroupName group1 = new Spec.NewsgroupName("local.tmp.test." + l + ".comp.lang.java");
            Spec.NewsgroupName group2 = new Spec.NewsgroupName("local.tmp.test." + l + ".alt.test");

            Date now = new Date();
            dbSvc.addGroup(group1, "Discussion about Java programming", Spec.PostingMode.Allowed, now, "nntp-test", false);
            dbSvc.addGroup(group2, "Alternative testing newsgroup", Spec.PostingMode.Allowed, now, "nntp-test", false);

            // Create client context with LIST NEWSGROUPS
            StringWriter responseWriter = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc, null, responseWriter);
            context.requestArgs = new String[]{"LIST", "NEWSGROUPS"};

            // Execute the handler
            Boolean result = Server.handleList(context);

            // For now, this should return 503 (feature not supported) if not implemented
            // Or 215 if implemented
            assertNotNull(result);

            String response = responseWriter.toString();

            // Check if implemented or not
            if (response.startsWith("215")) {
                // If implemented, should have format: groupname description
                assertTrue(response.contains(group1.getValue()));
                assertTrue(response.contains("Discussion about Java programming") ||
                        response.contains("Java programming"));
                assertTrue(response.endsWith(".\r\n"));
            } else {
                // If not implemented, should return 503
                assertTrue(response.startsWith("503"), "Should return 503 if not implemented");
            }
        }
    }

    @Test
    @DisplayName("Test LIST with too many arguments")
    void testHandleListTooManyArguments() {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            StringWriter responseWriter = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc, null, responseWriter);
            context.requestArgs = new String[]{"LIST", "ACTIVE", "EXTRA"};

            Boolean result = Server.handleList(context);

            assertTrue(result);

            String response = responseWriter.toString();
            assertTrue(response.startsWith("501"), "Should return 501 syntax error");
        }
    }

    @Test
    @DisplayName("Test LIST with unsupported variant")
    void testHandleListUnsupportedVariant() {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            StringWriter responseWriter = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc, null, responseWriter);
            context.requestArgs = new String[]{"LIST", "OVERVIEW.FMT"};

            Boolean result = Server.handleList(context);

            assertTrue(result);

            String response = responseWriter.toString();
            assertTrue(response.startsWith("503"), "Should return 503 feature not supported");
        }
    }

    @Test
    @DisplayName("Test LIST with empty newsgroup list")
    void testHandleListEmptyNewsgroupList() {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            // Don't add any newsgroups, but do know how many newsgroups there are
            Iterator<PersistenceService.Newsgroup> i = dbSvc.listAllGroups(false, false);
            int numNewsgroups = 0;
            while (i.hasNext()) {
                i.next();
                numNewsgroups++;
            }

            StringWriter responseWriter = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc, null, responseWriter);
            context.requestArgs = new String[]{"LIST"};

            Boolean result = Server.handleList(context);

            assertTrue(result);

            String response = responseWriter.toString();
            assertTrue(response.startsWith("215"));
            assertTrue(response.endsWith(".\r\n"), "Should still have terminator");

            // Should have all newsgroups in the response plus the terminator
            String[] lines = response.split("\r\n");
            assertEquals(2 + numNewsgroups, lines.length, "Should have response code and terminator only");
        }
    }

    @Test
    @DisplayName("Test LIST format with article numbers")
    void testHandleListFormat() throws Exception {
        try (MockPersistenceService dbSvc = new MockPersistenceService()) {

            // Add newsgroup with multiple articles
            long l = System.currentTimeMillis();
            Spec.NewsgroupName group = new Spec.NewsgroupName("local.tmp.test."+l+".group.L-");
            Date now = new Date();
            PersistenceService.Newsgroup ng = dbSvc.addGroup(group, "Test group",
                    Spec.PostingMode.Allowed, now, "nntp-test", false);

            // Add articles to create a range
            addTestArticle(dbSvc, ng, "<Message-ID-1."+l+">", "Article 1");
            addTestArticle(dbSvc, ng, "<Message-ID-2."+l+">", "Article 2");
            addTestArticle(dbSvc, ng, "<Message-ID-3."+l+">", "Article 3");

            StringWriter responseWriter = new StringWriter();
            Server.ClientContext context = createClientContext(dbSvc, null, responseWriter);
            context.requestArgs = new String[]{"LIST"};

            Boolean result = Server.handleList(context);
            assertTrue(result);

            String response = responseWriter.toString();

            // Parse the response to verify format
            String[] lines = response.split("\r\n");

            // Find the test.group line
            String groupLine = null;
            for (String line : lines) {
                if (line.startsWith(group.getValue())) {
                    groupLine = line;
                    break;
                }
            }

            assertNotNull(groupLine, "Should find test.group in response");

            // Format should be: groupname high low status
            String[] parts = groupLine.split("\\s+");
            assertEquals(4, parts.length, "Should have 4 parts: name high low status");
            assertEquals(group.getValue(), parts[0]);

            // Verify high >= low
            int high = Integer.parseInt(parts[1]);
            int low = Integer.parseInt(parts[2]);
            if (1 <= high || 1 <= low) {
                assertTrue(high >= low, "High article number should be >= low");
            }

            // Verify status is valid
            assertTrue(parts[3].matches("[ynm]"), "Status should be y, n, or m");
        }
    }

    @Test
    void testHandleNext() throws Exception {
        // Set up test data
        try (MockPersistenceService persistenceService = new MockPersistenceService()) {
            persistenceService.init();

            // Create a test newsgroup with unique name
            long l = System.currentTimeMillis();
            Spec.NewsgroupName testGroupName = new Spec.NewsgroupName("local.tmp.test."+l+".next.group.N");
            PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                    testGroupName,
                    "Test group for next command",
                    Spec.PostingMode.Allowed,
                    new Date(),
                    "tester",
                    false
            );
            assertNotNull(newsgroup);

            // Create multiple test articles
            Spec.MessageId[] messageIds = new Spec.MessageId[5];
            for (int i = 0; i < 5; i++) {
                messageIds[i] = new Spec.MessageId("<test.next." + i + "." + System.currentTimeMillis() + "@test.com>");
                Map<String, Set<String>> headers = new HashMap<>();
                headers.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(messageIds[i].getValue()));
                headers.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(testGroupName.getValue()));
                headers.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("Test Next Article " + i));
                headers.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("nexttest@example.com"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("15 Oct 2025 20:00:00 GMT"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.next.path"));
                headers.put(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<ref.next@test>"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("1"));
                headers.put(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("20"));

                Spec.Article.ArticleHeaders articleHeaders = new Spec.Article.ArticleHeaders(headers);
                String bodyContent = "Article " + i + " body.";

                PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(
                        messageIds[i],
                        articleHeaders,
                        new StringReader(bodyContent),
                        false
                );
                assertNotNull(newsgroupArticle);
            }

            // Position cursor at first article
            Spec.ArticleNumber firstArticleNum = newsgroup.getArticle(messageIds[0]);
            assertNotNull(firstArticleNum);

            // Test 1: NEXT command without newsgroup selected (should return 412)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"NEXT"};
                context.currentGroup = null;

                Boolean result = Server.handleNext(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("412"), "Expected 412 when no newsgroup selected, got: " + response);
            }

            // Test 2: NEXT command with newsgroup but no current article (should return 420)
            {
                // Create empty newsgroup
                Spec.NewsgroupName emptyGroupName = new Spec.NewsgroupName("test.empty."+l+".next.group");
                PersistenceService.Newsgroup emptyGroup = persistenceService.addGroup(
                        emptyGroupName,
                        "Empty test group for next",
                        Spec.PostingMode.Allowed,
                        new Date(),
                        "tester",
                        false
                );

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"NEXT"};
                context.currentGroup = emptyGroup;

                Boolean result = Server.handleNext(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("420"), "Expected 420 when no current article, got: " + response);
            }

            // Test 3: NEXT command moves from first to second article (should return 223)
            {
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"NEXT"};
                context.currentGroup = newsgroup;

                // Current article should be first (article number 1)
                PersistenceService.NewsgroupArticle current = newsgroup.getCurrentArticle();
                assertNotNull(current);
                assertEquals(1, current.getArticleNumber().getValue(), "Should start at first article");
                assertEquals(messageIds[0], current.getMessageId());

                Boolean result = Server.handleNext(context);
                assertTrue(result);

                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                assertTrue(response.startsWith("223"), "Expected 223 response for NEXT, got: " + response);

                // Parse response: "223 <articleNum> <messageId>"
                String[] parts = lines[0].split("\\s+");
                assertEquals(3, parts.length, "NEXT response should have code, article number, and message-id");
                assertEquals("223", parts[0]);

                int articleNum = Integer.parseInt(parts[1]);
                assertEquals(2, articleNum, "Should move to article 2");

                String msgId = parts[2];
                assertEquals(messageIds[1].getValue(), msgId, "Should return second article's message-id");

                // Verify cursor moved
                PersistenceService.NewsgroupArticle newCurrent = newsgroup.getCurrentArticle();
                assertEquals(2, newCurrent.getArticleNumber().getValue());
                assertEquals(messageIds[1], newCurrent.getMessageId());
            }

            // Test 4: Sequential NEXT commands traverse all articles
            {
                // Reset to first article
                newsgroup.getArticle(messageIds[0]);

                for (int i = 0; i < 4; i++) { // We have 5 articles, so 4 NEXT commands
                    StringWriter responseWriter = new StringWriter();
                    Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                    context.requestArgs = new String[]{"NEXT"};
                    context.currentGroup = newsgroup;

                    Boolean result = Server.handleNext(context);
                    assertTrue(result);

                    String response = responseWriter.toString();
                    assertTrue(response.startsWith("223"), "Expected 223 for NEXT " + (i + 1));

                    String[] parts = response.trim().split("\\s+");
                    int expectedArticleNum = i + 2; // Next article after (i+1)
                    assertEquals(expectedArticleNum, Integer.parseInt(parts[1]),
                            "NEXT " + (i + 1) + " should move to article " + expectedArticleNum);
                    assertEquals(messageIds[i + 1].getValue(), parts[2]);
                }
            }

            // Test 5: NEXT at last article returns 421 (no next article)
            {
                // Current should be at last article (5)
                PersistenceService.NewsgroupArticle current = newsgroup.getCurrentArticle();
                assertEquals(5, current.getArticleNumber().getValue(), "Should be at last article");

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"NEXT"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleNext(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("421"), "Expected 421 when no next article, got: " + response);

                // Verify cursor stays at last article
                PersistenceService.NewsgroupArticle stillCurrent = newsgroup.getCurrentArticle();
                assertEquals(5, stillCurrent.getArticleNumber().getValue(), "Cursor should remain at last article");
            }

            // Test 6: NEXT with arguments (should be ignored, NEXT takes no arguments)
            {
                // Reset to first article
                newsgroup.getArticle(messageIds[0]);

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"NEXT", "extra", "arguments"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleNext(context);
                assertTrue(result);

                String response = responseWriter.toString();
                // NEXT ignores arguments and proceeds normally
                assertTrue(response.startsWith("223"), "NEXT should ignore extra arguments, got: " + response);
            }

            // Test 7: NEXT response format (223 with article number and message-id only)
            {
                // Reset to first article
                newsgroup.getArticle(messageIds[0]);

                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"NEXT"};
                context.currentGroup = newsgroup;

                Server.handleNext(context);
                String response = responseWriter.toString();
                String[] lines = response.split("\r\n");

                // NEXT should only return status line (no multi-line content)
                assertEquals(1, lines.length, "NEXT should only return status line");
                assertFalse(response.contains("\r\n.\r\n"), "NEXT should not have dot termination");

                // Verify format: "223 <num> <msgid>\r\n"
                assertTrue(response.matches("223 \\d+ <.+>\\r\\n"),
                        "NEXT response should match '223 <num> <msgid>\\r\\n'");
            }

            // Test 8: NEXT updates current article pointer
            {
                // Reset to first article
                newsgroup.getArticle(messageIds[0]);
                assertEquals(1, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // First NEXT
                StringWriter responseWriter1 = new StringWriter();
                Server.ClientContext context1 = createClientContext(persistenceService, null, responseWriter1);
                context1.requestArgs = new String[]{"NEXT"};
                context1.currentGroup = newsgroup;
                Server.handleNext(context1);

                assertEquals(2, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Current article should be 2 after first NEXT");

                // Second NEXT
                StringWriter responseWriter2 = new StringWriter();
                Server.ClientContext context2 = createClientContext(persistenceService, null, responseWriter2);
                context2.requestArgs = new String[]{"NEXT"};
                context2.currentGroup = newsgroup;
                Server.handleNext(context2);

                assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Current article should be 3 after second NEXT");
            }

            // Test 9: NEXT works after using STAT on middle article
            {
                // Use STAT to jump to middle article (article 3)
                StringWriter statResponseWriter = new StringWriter();
                Server.ClientContext statContext = createClientContext(persistenceService, null, statResponseWriter);
                statContext.requestArgs = new String[]{"STAT", "3"};
                statContext.currentGroup = newsgroup;
                Server.handleStat(statContext);

                assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // Now NEXT should go to article 4
                StringWriter responseWriter = new StringWriter();
                Server.ClientContext context = createClientContext(persistenceService, null, responseWriter);
                context.requestArgs = new String[]{"NEXT"};
                context.currentGroup = newsgroup;

                Boolean result = Server.handleNext(context);
                assertTrue(result);

                String response = responseWriter.toString();
                assertTrue(response.startsWith("223"));
                assertTrue(response.contains(" 4 "), "Should move to article 4");
                assertEquals(4, newsgroup.getCurrentArticle().getArticleNumber().getValue());
            }

            // Test 10: Multiple NEXT commands at end stay at last article and return 421
            {
                // Position at last article
                newsgroup.getArticle(messageIds[4]);
                assertEquals(5, newsgroup.getCurrentArticle().getArticleNumber().getValue());

                // First NEXT at end
                StringWriter responseWriter1 = new StringWriter();
                Server.ClientContext context1 = createClientContext(persistenceService, null, responseWriter1);
                context1.requestArgs = new String[]{"NEXT"};
                context1.currentGroup = newsgroup;
                Server.handleNext(context1);

                String response1 = responseWriter1.toString();
                assertTrue(response1.startsWith("421"), "First NEXT at end should return 421");
                assertEquals(5, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Should stay at article 5");

                // Second NEXT at end
                StringWriter responseWriter2 = new StringWriter();
                Server.ClientContext context2 = createClientContext(persistenceService, null, responseWriter2);
                context2.requestArgs = new String[]{"NEXT"};
                context2.currentGroup = newsgroup;
                Server.handleNext(context2);

                String response2 = responseWriter2.toString();
                assertTrue(response2.startsWith("421"), "Second NEXT at end should also return 421");
                assertEquals(5, newsgroup.getCurrentArticle().getArticleNumber().getValue(),
                        "Should still stay at article 5");
            }

            // Test 11: NEXT response is same format as STAT (both return 223)
            {
                // Reset to first article
                newsgroup.getArticle(messageIds[0]);

                // Get NEXT response
                StringWriter nextResponseWriter = new StringWriter();
                Server.ClientContext nextContext = createClientContext(persistenceService, null, nextResponseWriter);
                nextContext.requestArgs = new String[]{"NEXT"};
                nextContext.currentGroup = newsgroup;
                Server.handleNext(nextContext);
                String nextResponse = nextResponseWriter.toString();

                // Get STAT response for same article
                StringWriter statResponseWriter = new StringWriter();
                Server.ClientContext statContext = createClientContext(persistenceService, null, statResponseWriter);
                statContext.requestArgs = new String[]{"STAT"};
                statContext.currentGroup = newsgroup;
                Server.handleStat(statContext);
                String statResponse = statResponseWriter.toString();

                // Both should start with 223
                assertTrue(nextResponse.startsWith("223"), "NEXT should return 223");
                assertTrue(statResponse.startsWith("223"), "STAT should return 223");

                // Both should have same format (single line with article num and message-id)
                assertEquals(1, nextResponse.split("\r\n").length, "NEXT should be single line");
                assertEquals(1, statResponse.split("\r\n").length, "STAT should be single line");

                // Both should have same structure
                String[] nextParts = nextResponse.trim().split("\\s+");
                String[] statParts = statResponse.trim().split("\\s+");
                assertEquals(3, nextParts.length, "NEXT should have 3 parts");
                assertEquals(3, statParts.length, "STAT should have 3 parts");
            }
        }
    }

    @Test
    void testHandleOverview() {
    }

    @Test
    void testHandlePost() {
    }

    @Test
    void testHandleXOver() {
    }

    @Test
    void testSendResponse() {
    }

    // Helper method to create a ClientContext for testing
    private Server.ClientContext createClientContext(PersistenceService persistenceService, StringReader requestReader, StringWriter responseWriter) {
        BufferedReader requestStream = (requestReader != null ? new BufferedReader(requestReader) : null);
        BufferedWriter responseStream = (responseWriter != null ? new BufferedWriter(responseWriter) : null);

        return new Server.ClientContext(
                persistenceService,
                new MockIdentityService(),
                new MockPolicyService(),
                requestStream,
                responseStream
        );
    }

    @AfterAll
    static void cleanup() {
        MockPersistenceService persistenceService = new MockPersistenceService();
        persistenceService.init();
        persistenceService.close();
    }

}
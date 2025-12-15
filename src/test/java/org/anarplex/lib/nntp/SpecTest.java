package org.anarplex.lib.nntp;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SpecTest {
    @Test
    void newsgroupNameValid() {
        assertTrue(Spec.NewsgroupName.isValid("test"));     // top-level groups aren't good, but also not forbidden
        assertTrue(Spec.NewsgroupName.isValid("test.test"));
        assertTrue(Spec.NewsgroupName.isValid("test.a-test"));
        assertTrue(Spec.NewsgroupName.isValid("test.a+test"));
        assertTrue(Spec.NewsgroupName.isValid("test.a_test"));
        assertTrue(Spec.NewsgroupName.isValid("test.a1test"));
        assertTrue(Spec.NewsgroupName.isValid("test.aPtest"));
        assertTrue(Spec.NewsgroupName.isValid("test.test2"));
        assertTrue(Spec.NewsgroupName.isValid("test.test_"));
        assertTrue(Spec.NewsgroupName.isValid("test.test+"));
        assertTrue(Spec.NewsgroupName.isValid("test.test-"));
        assertTrue(Spec.NewsgroupName.isValid("test.test2"));
    }

    @Test
    void newsgroupNameInvalid() {
        assertFalse(Spec.NewsgroupName.isValid(null));      // not null
        assertFalse(Spec.NewsgroupName.isValid(""));        // not empty
        assertFalse(Spec.NewsgroupName.isValid(".test"));   // does not start with dot
        assertFalse(Spec.NewsgroupName.isValid("test."));   // does not end with dot
        assertFalse(Spec.NewsgroupName.isValid("test..test"));  // does not contain two consecutive dots
        assertFalse(Spec.NewsgroupName.isValid("test@test"));   // this punctuation character not allowed
        assertFalse(Spec.NewsgroupName.isValid("test!test"));   // this punctuation character not allowed
        assertFalse(Spec.NewsgroupName.isValid("test$test"));   // this punctuation character not allowed
        assertFalse(Spec.NewsgroupName.isValid("test^test"));   // this punctuation character not allowed
        assertFalse(Spec.NewsgroupName.isValid("test&test"));   // this punctuation character not allowed
        assertFalse(Spec.NewsgroupName.isValid("test\\A.test"));   // no escape characters allowed
    }

    @Test
    void postingMode() {
        for (Spec.PostingMode mode : Spec.PostingMode.values()) {
            assertNotNull(mode);
        }
        for (int i = 0; i < Spec.PostingMode.values().length; i++) {
            assertEquals(Spec.PostingMode.values()[i], Spec.PostingMode.valueOf(i));
        }
    }

    @Test
    void articleNumberValid() throws Spec.ArticleNumber.InvalidArticleNumberException {
        Spec.ArticleNumber n;

        assertTrue(Spec.ArticleNumber.isValid(1));
        assertTrue(Spec.ArticleNumber.isValid(1000));
        assertTrue(Spec.ArticleNumber.isValid(Integer.MAX_VALUE));

        n = new Spec.ArticleNumber(1000);
        assertEquals(1000, n.getValue());
    }

    @Test
    void articleNumberInvalid() {
        assertTrue(Spec.ArticleNumber.isValid(Spec.NoArticlesHighestNumber));
        assertTrue(Spec.ArticleNumber.isValid(Spec.NoArticlesLowestNumber));
        assertFalse(Spec.ArticleNumber.isValid(-20));
    }

    @Test
    void messageIdValid() {
        assertTrue(Spec.MessageId.isValid("<1234567890>")); // valid
        assertTrue(Spec.MessageId.isValid("<0>"));  // suspicious but valid
    }

    @Test
    void messageIdInvalid() {
        assertFalse(Spec.MessageId.isValid(null));    // empty string
        assertFalse(Spec.MessageId.isValid(""));    // empty string
        assertFalse(Spec.MessageId.isValid("<>"));    // empty string

        assertFalse(Spec.MessageId.isValid("1234567890"));  // missing start delimiter
        assertFalse(Spec.MessageId.isValid("<9023"));   // missing end delimiter
        assertFalse(Spec.MessageId.isValid("<0123>>"));  // too many end delimiters
        assertFalse(Spec.MessageId.isValid("<0123>3"));  // end delimiter not at the end
        assertFalse(Spec.MessageId.isValid("<\t>"));    // non-printable characters
    }

    @Test
    void articleStandardHeadersValid() {
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton("<1234567890>")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("test subject")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("somebody")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("Thu, 1 Jan 1970 00:00:00")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton("test.test")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<1234567890>")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.test")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("12345")));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("12345")));
    }

    @Test
    void articleStandardHeadersInValid() {
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton("<>")));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("")));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.From.getValue(), null));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("1 JAN 1970 00:00:00")));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton("test.test.")));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<>")));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Path.getValue(), null));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("five")));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderField(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("nine")));
    }

    @Test
    void allHeadersValid() throws Spec.Article.ArticleHeaders.InvalidArticleHeaderException {
        Map<String, Set<String>> testHeaders = new HashMap<>();
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Set.of("<1234567890>"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Set.of("test subject"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Set.of("test person"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Set.of("Thu, 1 Jan 1970 00:00:00"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Set.of("test.group"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.References.getValue(), Set.of("<1234567891>"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Set.of("test.path"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.Bytes.getValue(), Set.of("12345"));
        testHeaders.put(Spec.NNTP_Standard_Article_Headers.Lines.getValue(), Set.of("12345"));
        testHeaders.put("custom header", Set.of("custom value1", "custom value2"));

        assertEquals(testHeaders, Spec.Article.ArticleHeaders.validateHeaderFields(testHeaders));
    }

    @Test
    void articleHeaderNameValid() {
        // all standard header names are valid
        for (Spec.NNTP_Standard_Article_Headers header : Spec.NNTP_Standard_Article_Headers.values()) {
            assertNotNull(header);
            assertNotNull(header.getValue());
            assertTrue(Spec.Article.ArticleHeaders.isValidHeaderName(header.getValue()));
        }

        // custom header names are valid
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderName("custom-id"));
    }

    @Test
    void articleHeaderNameInvalid() {
        // assertFalse(Spec.Article.isValidHeaderName(null));       // empty string
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderName(""));            // empty string
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderName(" "));           // trailing space
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderName("testname "));   // trailing space
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderName("testname:"));   // can't use delimiter
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderName("test\tname"));  // can't contain a non-printable character
    }

    @Test
    void articleHeaderValueValid() {
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderValue(" "));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderValue("1"));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderValue("$"));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderValue("_"));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderValue("test"));
        assertTrue(Spec.Article.ArticleHeaders.isValidHeaderValue("test test"));
    }

    @Test
    void articleHeaderValueInvalid() {
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderValue(null));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderValue(""));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderValue("test\ttest"));
        assertFalse(Spec.Article.ArticleHeaders.isValidHeaderValue("test\r\ntest"));
    }

    @Test
    void isDateValid() {
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 1970 00:00:00"));       // canonical form

        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 1970 00:00:00 +0000")); // timezone offset
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 1970 00:00:00 -0000")); // timezone offset
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 1970 00:00:00 GMT"));   // timezone offset
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 1970 00:00:00 UTC"));   // timezone offset
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 1970 00:00:00 Z"));     // timezone offset

        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 70 00:00:00"));         // short year.  obsolete
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu, 1 Jan 70 00:00"));            // no seconds
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("1 Jan 70 00:00:00"));              // no day of week
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("1 Jan 70 00:00:00 "));             // trailing space

        assertTrue(Spec.Article.ArticleHeaders.isValidDate("1 Jan 1900 00:00:00"));            // from 1900
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("1 Jan 2050 00:00:00"));            // past 2035
        assertTrue(Spec.Article.ArticleHeaders.isValidDate("1 Jan 2769 00:00:00"));            // OG is seven hundred years old this year

        assertTrue(Spec.Article.ArticleHeaders.isValidDate("Thu 1 Jan 1970 00:00:00 +0001"));  // comma after DoW is not mandatory
    }

    @Test
    void isDateInvalid() {
        assertFalse(Spec.Article.ArticleHeaders.isValidDate(null));
        assertFalse(Spec.Article.ArticleHeaders.isValidDate(""));
        assertFalse(Spec.Article.ArticleHeaders.isValidDate("Th, 1 Jan 1970 00:00:"));     // field separator for seconds but no value
        assertFalse(Spec.Article.ArticleHeaders.isValidDate("Th, 1 Jan 1970 00:00"));      // not a valid day of week
        assertFalse(Spec.Article.ArticleHeaders.isValidDate("THU, 1 Jan 1970 00:00"));     // not a valid day of week
        assertFalse(Spec.Article.ArticleHeaders.isValidDate(", 1 Jan 1970 00:00"));        // no comma needed if DoW absent
        assertFalse(Spec.Article.ArticleHeaders.isValidDate("1 JAN 1970 00:00"));          // no comma needed if DoW absent
    }

    @Test
    void isPathValid() {
        // Single component paths (tail-entry only)
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("not-for-mail"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("localhost"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news.example.com"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server-1"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server_1"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server.host.domain"));

        // Multiple component paths (path-identity!path-identity!...!tail-entry)
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news.example.com!not-for-mail"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server1!server2!not-for-mail"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news.example.com!news.example.org!not-for-mail"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("host1!host2!host3"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server-a!server-b!server-c"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news1.example.com!news2.example.com!news3.example.com"));

        // Paths with underscores and hyphens
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server_1!server_2"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server-1!server-2"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("my_server!your-server!their.server"));

        // Paths with numbers
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server1"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("123!456"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news1!news2!news3"));

        // Complex realistic paths
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news.example.com"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("nntp.example.org!news.example.com!not-for-mail"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("reader.example.net!feeder.example.org!backbone.example.com"));

        // Paths with whitespace (should be trimmed)
        assertTrue(Spec.Article.ArticleHeaders.isValidPath(" not-for-mail "));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath(" server1!server2 "));

        // strange but true
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server!host."));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server1!server..2"));
    }

    @Test
    void isPathInvalid() {
        // Null and empty paths
        assertFalse(Spec.Article.ArticleHeaders.isValidPath(null));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath(""));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("   "));

        // Paths starting or ending with dot
        assertFalse(Spec.Article.ArticleHeaders.isValidPath(".server"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server."));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath(".server!host"));

        // Paths with consecutive dots
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server..host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("news..example..com"));


        // Paths with empty components
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("!"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server1!!server2"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("!server"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server!"));

        // Paths with invalid characters (special characters not allowed in path-identity)
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server@host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server#host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server$host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server%host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server&host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server*host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server+host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server=host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server/host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server\\host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server:host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server;host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server<host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server>host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server?host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server[host]"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server{host}"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server|host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server~host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server`host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server'host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server\"host"));

        // Paths with spaces (not allowed in path-identity)
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server host"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server 1!server 2"));

        // Paths with control characters
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server\thost"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server\nhost"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("server\rhost"));

        // Paths with non-ASCII characters (may be valid in diagnostic but we're restrictive)
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("servér"));
        assertFalse(Spec.Article.ArticleHeaders.isValidPath("sërvër"));
    }

    @Test
    void isPathEdgeCases() {
        // Very long paths (should be valid if format is correct)
        String longPath = "server1!server2!server3!server4!server5!server6!server7!server8!server9!server10";
        assertTrue(Spec.Article.ArticleHeaders.isValidPath(longPath));

        // Path with many dots
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("a.b.c.d.e.f.g"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news.sub1.sub2.example.com"));

        // Path with many hyphens
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server-a-b-c"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("my-news-server!your-news-server"));

        // Path with many underscores
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server_a_b_c"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("my_news_server!your_news_server"));

        // Mixed valid characters
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("server1.example-test_host"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news-1.example_org!host-2.test_com"));

        // Single character components
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("a"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("a!b!c"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("1!2!3"));
    }

    @Test
    void isPathRFC5536Examples() {
        // Examples from RFC 5536 and common usage patterns

        // RFC 5536 recommends "not-for-mail" as tail-entry for posted articles
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("not-for-mail"));

        // Common server path patterns
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news.example.com!not-for-mail"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("reader.example.net!news.example.com!not-for-mail"));

        // UUCP-style paths (legacy but still valid)
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("site1!site2!site3"));

        // Modern FQDN-style paths
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("news1.example.org!news2.example.org"));
        assertTrue(Spec.Article.ArticleHeaders.isValidPath("nntp.example.com!news.example.net!backbone.example.org"));
    }
}
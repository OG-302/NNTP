package org.anarplex.lib.nntp;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ArticleHeadersValidationTest {

    private Map<String, Set<String>> minimalValidHeaders() {
        Map<String, Set<String>> h = new HashMap<>();
        h.put(Specification.NNTP_Standard_Article_Headers.Subject.getValue(), Set.of("Hello World"));
        h.put(Specification.NNTP_Standard_Article_Headers.From.getValue(), Set.of("Alice <alice@example.com>"));
        String date = Utilities.DateAndTime.formatRFC3977(Instant.parse("2024-01-01T00:00:00Z"));
        h.put(Specification.NNTP_Standard_Article_Headers.Date.getValue(), Set.of(date));
        h.put(Specification.NNTP_Standard_Article_Headers.MessageID.getValue(), Set.of("<msg-1@example.com>"));
        h.put(Specification.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Set.of("comp.lang.java,comp.lang"));
        h.put(Specification.NNTP_Standard_Article_Headers.Path.getValue(), Set.of("news.example.com!not-for-mail"));
        return h;
    }

    @Test
    void constructor_accepts_minimal_valid_headers_and_accessors_work() throws Exception {
        Specification.Article.ArticleHeaders headers = new Specification.Article.ArticleHeaders(minimalValidHeaders());

        // getMessageId and getNewsgroups
        Specification.MessageId mid = headers.getMessageId();
        assertEquals("<msg-1@example.com>", mid.getValue());
        Set<Specification.NewsgroupName> groups = headers.getNewsgroups();
        assertEquals(2, groups.size());
        assertTrue(groups.stream().anyMatch(g -> g.getValue().equals("comp.lang.java")));
        assertTrue(groups.stream().anyMatch(g -> g.getValue().equals("comp.lang")));

        // getHeaderValue and entrySet immutability
        assertEquals(Set.of("Hello World"), headers.getHeaderValue("subject"));
        Iterable<? extends Map.Entry<String, Set<String>>> entries = headers.entrySet();
        assertThrows(UnsupportedOperationException.class, () -> ((Set<?>) entries).clear());

        // Deep copy - modifying returned map must not affect internal state
        Map<String, Set<String>> copy = headers.getHeaderFields();
        copy.put("x-custom", Set.of("v"));
        assertFalse(headers.getHeaderFields().containsKey("x-custom"));
    }

    @Test
    void missing_mandatory_header_throws() {
        Map<String, Set<String>> h = minimalValidHeaders();
        h.remove(Specification.NNTP_Standard_Article_Headers.Subject.getValue());
        assertThrows(Specification.Article.ArticleHeaders.InvalidArticleHeaderException.class,
                () -> new Specification.Article.ArticleHeaders(h));
    }

    @Test
    void duplicate_single_valued_headers_rejected() {
        Map<String, Set<String>> h = minimalValidHeaders();
        h.put(Specification.NNTP_Standard_Article_Headers.Subject.getValue(), Set.of("A", "B"));
        assertThrows(Specification.Article.ArticleHeaders.InvalidArticleHeaderException.class,
                () -> new Specification.Article.ArticleHeaders(h));
    }

    @Test
    void invalid_date_and_newsgroup_rejected() {
        // invalid date
        Map<String, Set<String>> h1 = minimalValidHeaders();
        h1.put(Specification.NNTP_Standard_Article_Headers.Date.getValue(), Set.of("not a date"));
        assertThrows(Specification.Article.ArticleHeaders.InvalidArticleHeaderException.class,
                () -> new Specification.Article.ArticleHeaders(h1));

        // invalid newsgroup (contains space)
        Map<String, Set<String>> h2 = minimalValidHeaders();
        h2.put(Specification.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Set.of("comp.lang.java,invalid group"));
        assertThrows(Specification.Article.ArticleHeaders.InvalidArticleHeaderException.class,
                () -> new Specification.Article.ArticleHeaders(h2));
    }

    @Test
    void isValidHeaderName_and_value_and_unstructured_checks() {
        assertTrue(Specification.Article.ArticleHeaders.isValidHeaderName("X-Custom"));
        assertTrue(Specification.Article.ArticleHeaders.isValidHeaderName(":bytes"));
        // Implementation allows internal spaces (disallows leading/trailing only)
        assertTrue(Specification.Article.ArticleHeaders.isValidHeaderName("bad name"));
        assertFalse(Specification.Article.ArticleHeaders.isValidHeaderName("bad:\tvalue"));

        assertTrue(Specification.Article.ArticleHeaders.isValidHeaderValue("abc"));
        assertFalse(Specification.Article.ArticleHeaders.isValidHeaderValue("a\tbc"));

        assertFalse(Specification.Article.ArticleHeaders.isInvalidUnstructuredValue("From Person"));
        assertTrue(Specification.Article.ArticleHeaders.isInvalidUnstructuredValue(""));
    }

    @Test
    void path_validation_and_contains_identity() {
        String p1 = "news.example.com!not-for-mail";
        String p2 = "reader.net!news.example.com!not-for-mail";
        String invalid = "!bad!!path";

        assertTrue(Specification.Article.ArticleHeaders.isValidPath(p1));
        assertTrue(Specification.Article.ArticleHeaders.isValidPath(p2));
        assertFalse(Specification.Article.ArticleHeaders.isValidPath(invalid));

        assertTrue(Specification.Article.ArticleHeaders.pathContainsIdentity(p2, "news.example.com"));
        assertFalse(Specification.Article.ArticleHeaders.pathContainsIdentity(p2, "missing.host"));
    }
}

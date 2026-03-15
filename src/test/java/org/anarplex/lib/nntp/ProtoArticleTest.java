package org.anarplex.lib.nntp;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ProtoArticleTest {

    private String validArticleText() {
        return "Subject: Hello\r\n" +
                "From: Alice <alice@example.com>\r\n" +
                "Date: Mon, 01 Jan 2024 00:00 +0000\r\n" +
                "Message-Id: <msg-1@example.com>\r\n" +
                "Newsgroups: comp.lang.java\r\n" +
                "Path: not-for-mail\r\n" +
                "\r\n" + // blank line between headers and body
                "line1\r\n" +
                Specification.CRLF_DOT_CRLF;
    }

    @Test
    void fromString_parses_headers_to_lowercase_keys_and_body_kept() throws Exception {
        Specification.ProtoArticle p = Specification.ProtoArticle.fromString(validArticleText());

        Map<String, Set<String>> map = p.getHeadersLowerCase();
        // Keys are lowercased (without the trailing colon)
        assertTrue(map.containsKey("subject"));
        assertTrue(map.containsKey("message-id"));
        assertEquals(Set.of("Hello"), map.get("subject"));

        String body = p.getBodyText();
        assertTrue(body.startsWith("line1\r\n"));
        assertTrue(body.endsWith(Specification.CRLF_DOT_CRLF));
    }

    @Test
    void addFieldIfNotPresent_adds_missing_standard_fields() throws Exception {
        Specification.ProtoArticle p = Specification.ProtoArticle.fromString(validArticleText());

        // Ensure :lines is added if missing
        assertFalse(p.getHeadersLowerCase().containsKey( Specification.NNTP_Standard_Article_Headers.Lines.getValue()));
        p.addFieldIfNotPresent(Specification.NNTP_Standard_Article_Headers.Lines, "1");
        assertTrue(p.getHeadersLowerCase().containsKey( Specification.NNTP_Standard_Article_Headers.Lines.getValue()));
        assertEquals(Set.of("1"), p.getHeadersLowerCase().get(Specification.NNTP_Standard_Article_Headers.Lines.getValue()));
    }

    @Test
    void malformed_inputs_throw() {
        // No separator between headers and body
        String noSep = "Subject: X\r\nMessage-Id: <a@b>\r\n" + "line1\r\n" + Specification.CRLF_DOT_CRLF;
        assertThrows(Specification.Article.InvalidArticleFormatException.class,
                () -> Specification.ProtoArticle.fromString(noSep));

        // Body not ending with CRLF.DOT.CRLF
        String badBody = "Subject: X\r\nMessage-Id: <a@b>\r\n\r\nline1\r\n";
        assertThrows(Specification.Article.InvalidArticleFormatException.class,
                () -> Specification.ProtoArticle.fromString(badBody));
    }
}

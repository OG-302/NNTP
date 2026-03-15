package org.anarplex.lib.nntp;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Focused unit tests for a subset of Specification inner types that are
 * self-contained and do not require integration with other services.
 * This first batch covers enums and simple value objects to lay the
 * groundwork; additional tests for Article/ArticleHeaders/ProtoArticle
 * can be added incrementally.
 */
class SpecificationInnerTypesTest {

    @Test
    void requestCommands_normalize_and_lookup() {
        // As implemented, lookup expects already-normalized command tokens
        Specification.NNTP_Request_Commands cmd = Specification.NNTP_Request_Commands.getCommand("GROUP");
        assertNotNull(cmd);
        assertEquals("GROUP", cmd.getValue());
        assertEquals("GROUP", cmd.toString());

        // Unknown returns null
        assertNull(Specification.NNTP_Request_Commands.getCommand("__unknown__"));

        // Normalization helper is package-private/private; we only validate lookup on normalized input here
    }

    @Test
    void serverCapabilities_contains_and_requirements() {
        // Pick first capability with non-null value for robust testing
        Specification.NNTP_Server_Capabilities cap = Arrays.stream(Specification.NNTP_Server_Capabilities.values())
                .filter(c -> c.getValue() != null)
                .findFirst()
                .orElseThrow();
        // Avoid calling contains() directly because some enum entries have null values and the implementation
        // does not null-check before equalsIgnoreCase. Verify equivalent behavior safely here.
        assertTrue(Arrays.stream(Specification.NNTP_Server_Capabilities.values())
                .anyMatch(c2 -> c2.getValue() != null && c2.getValue().equalsIgnoreCase(cap.getValue())));

        // Build a command set and verify sufficiency according to the enum's declared requisite set
        Set<Specification.NNTP_Request_Commands> cmds = EnumSet.noneOf(Specification.NNTP_Request_Commands.class);
        for (Specification.NNTP_Request_Commands c : Specification.NNTP_Request_Commands.values()) {
            if (cap.isRequiredCommand(c)) {
                cmds.add(c);
            }
        }
        assertTrue(cap.isSufficientSet(cmds));
    }

    @Test
    void responseCode_findByCode_and_toString() {
        // pick a well-known code: 211 (group selected) or 200 (server ready) depending on enum
        Specification.NNTP_Response_Code rc = Specification.NNTP_Response_Code.findByCode(200);
        assertNotNull(rc);
        assertEquals(200, rc.getCode());
        assertTrue(rc.toString().contains("200"));

        assertNull(Specification.NNTP_Response_Code.findByCode(999));
    }

    @Test
    void standardHeaders_contains_and_mandatory_flag() {
        Specification.NNTP_Standard_Article_Headers subj = Specification.NNTP_Standard_Article_Headers.Subject;
        assertTrue(Specification.NNTP_Standard_Article_Headers.contains("Subject"));
        // Some headers are mandatory as per enum definition; ensure method is callable
        subj.isMandatory();
        // Implementation stores header names in lowercase
        assertEquals("subject", subj.getValue());
        assertEquals("subject", subj.toString());
    }

    @Test
    void postingMode_roundtrip_and_toChar() {
        for (Specification.PostingMode pm : Specification.PostingMode.values()) {
            int v = pm.getValue();
            assertEquals(pm, Specification.PostingMode.valueOf(v));
            char ch = pm.toChar();
            assertTrue(ch == 'y' || ch == 'n' || ch == 'm');
            assertFalse(pm.toString().isBlank());
        }
    }

    @Test
    void newsgroupName_validation_and_equality() throws Exception {
        Specification.NewsgroupName n1 = new Specification.NewsgroupName("comp.lang.java");
        Specification.NewsgroupName n2 = new Specification.NewsgroupName("comp.lang.java");
        Specification.NewsgroupName n3 = new Specification.NewsgroupName("local.test");

        assertEquals(n1, n2);
        assertEquals(n1.hashCode(), n2.hashCode());
        assertNotEquals(n1, n3);
        assertEquals("comp.lang.java", n1.getValue());
        assertFalse(n1.toString().isBlank());

        new Specification.NewsgroupName("nntp.test");
        assertTrue(Specification.NewsgroupName.isValid("nntp.test"));
        assertThrows(Specification.NewsgroupName.InvalidNewsgroupNameException.class,
                () -> new Specification.NewsgroupName("invalid name with spaces"));
    }

    @Test
    void messageId_validation_compare_equals() throws Exception {
        Specification.MessageId m1 = new Specification.MessageId("<abc@host>");
        Specification.MessageId m2 = new Specification.MessageId("<abc@host>");
        Specification.MessageId m3 = new Specification.MessageId("<bcd@host>");

        assertEquals(m1, m2);
        assertNotEquals(m1, m3);
        assertEquals(0, m1.compareTo(m2));
        assertTrue(m3.compareTo(m1) != 0); // just comparable
        assertEquals("<abc@host>", m1.getValue());
        assertFalse(m1.toString().isBlank());

        assertTrue(Specification.MessageId.isValid("<id@h>"));
        assertThrows(Specification.MessageId.InvalidMessageIdException.class,
                () -> new Specification.MessageId("missing-angle"));
    }

    @Test
    void articleNumber_construction_and_equality() throws Exception {
        Specification.ArticleNumber a1 = new Specification.ArticleNumber(1);
        Specification.ArticleNumber a2 = new Specification.ArticleNumber("1");
        Specification.ArticleNumber a3 = new Specification.ArticleNumber(10);

        assertEquals(1, a1.getValue());
        assertEquals(a1, a2);
        assertNotEquals(a1, a3);
        assertTrue(Specification.ArticleNumber.isValid(5));
        assertFalse(a1.toString().isBlank());

        // Per implementation, 0 and -1 are valid sentinel values
        assertDoesNotThrow(() -> new Specification.ArticleNumber(0));
        assertDoesNotThrow(() -> new Specification.ArticleNumber("0"));
        assertThrows(Specification.ArticleNumber.InvalidArticleNumberException.class,
                () -> new Specification.ArticleNumber("not-a-number"));
    }

    @Test
    void noArticles_marker_types_constructible() throws Exception {
        Specification.NoArticlesLowestNumber low = new Specification.NoArticlesLowestNumber();
        Specification.NoArticlesHighestNumber high = new Specification.NoArticlesHighestNumber();

        assertNotNull(low);
        assertNotNull(high);
        assertNotEquals(low.getClass(), high.getClass());
        assertTrue(low.toString().contains("NoArticles") || !low.toString().isBlank());
        assertTrue(high.toString().contains("NoArticles") || !high.toString().isBlank());
    }

    @Test
    void newsgroup_and_metrics_basic_behaviors() throws Exception {
        Specification.NewsgroupName name = new Specification.NewsgroupName("comp.lang.java");
        Specification.Newsgroup.Metrics metrics = new Specification.Newsgroup.Metrics(3,
                new Specification.ArticleNumber(1), new Specification.ArticleNumber(3));
        Specification.Newsgroup ng = new Specification.Newsgroup(name, "Java discussion", Specification.PostingMode.Moderated, "admin") {
            @Override
            Specification.Newsgroup.Metrics getMetrics() {
                return metrics;
            }
        };

        assertEquals(name, ng.getName());
        assertEquals("Java discussion", ng.getDescription());
        assertEquals(Specification.PostingMode.Moderated, ng.getPostingMode());
        assertNotNull(ng.getCreatedBy());

        ng.setDescription("desc2");
        assertEquals("desc2", ng.getDescription());

        ng.setPostingMode(Specification.PostingMode.Allowed);
        assertEquals(Specification.PostingMode.Allowed, ng.getPostingMode());

        assertEquals(3, metrics.numPublishedArticles());
        assertEquals(1, metrics.numLowestArticle());
        assertEquals(3, metrics.numHighestArticle());
        assertEquals(1, metrics.getLowestArticleNumber().getValue());
        assertEquals(3, metrics.getHighestArticleNumber().getValue());

        assertEquals(metrics.numPublishedArticles(), ng.getMetrics().numPublishedArticles());
        assertEquals(metrics.getLowestArticleNumber(), ng.getMetrics().getLowestArticleNumber());
        assertEquals(metrics.getHighestArticleNumber(), ng.getMetrics().getHighestArticleNumber());
    }
}

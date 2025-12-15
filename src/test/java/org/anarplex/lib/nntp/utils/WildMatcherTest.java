package org.anarplex.lib.nntp.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class WildMatcherTest {

    // Basic wildcard tests
    
    @Test
    @DisplayName("Asterisk matches zero or more characters")
    void testAsteriskWildcard() {
        WildMatcher matcher = new WildMatcher("comp.*");
        assertTrue(matcher.matches("comp."));
        assertTrue(matcher.matches("comp.lang"));
        assertTrue(matcher.matches("comp.lang.java"));
        assertTrue(matcher.matches("comp.sys.unix"));
        assertFalse(matcher.matches("alt.test"));
        assertFalse(matcher.matches("comp"));
    }

    @Test
    @DisplayName("Question mark matches exactly one character")
    void testQuestionMarkWildcard() {
        WildMatcher matcher = new WildMatcher("alt.??");
        assertTrue(matcher.matches("alt.tv"));
        assertTrue(matcher.matches("alt.os"));
        assertFalse(matcher.matches("alt.t"));
        assertFalse(matcher.matches("alt."));
        assertFalse(matcher.matches("alt.test"));
    }

    @Test
    @DisplayName("Mixed wildcards")
    void testMixedWildcards() {
        WildMatcher matcher = new WildMatcher("comp.*.?");
        assertTrue(matcher.matches("comp.lang.c"));
        assertTrue(matcher.matches("comp.sys.x"));
        assertFalse(matcher.matches("comp.lang.java"));
        assertFalse(matcher.matches("comp.lang."));
    }

    // Character class tests

    @Test
    @DisplayName("Character class matches one character from set")
    void testCharacterClass() {
        WildMatcher matcher = new WildMatcher("comp.lang.[cj]*");
        assertTrue(matcher.matches("comp.lang.c"));
        assertTrue(matcher.matches("comp.lang.java"));
        assertTrue(matcher.matches("comp.lang.javascript"));
        assertFalse(matcher.matches("comp.lang.python"));
    }

    @Test
    @DisplayName("Character class with range")
    void testCharacterClassRange() {
        WildMatcher matcher = new WildMatcher("test[0-9]");
        assertTrue(matcher.matches("test0"));
        assertTrue(matcher.matches("test5"));
        assertTrue(matcher.matches("test9"));
        assertFalse(matcher.matches("testa"));
        assertFalse(matcher.matches("test"));
    }

    @Test
    @DisplayName("Negated character class with !")
    void testNegatedCharacterClassExclamation() {
        WildMatcher matcher = new WildMatcher("comp.lang.[!jp]*");
        assertTrue(matcher.matches("comp.lang.c"));
        assertTrue(matcher.matches("comp.lang.rust"));
        assertFalse(matcher.matches("comp.lang.java"));
        assertFalse(matcher.matches("comp.lang.python"));
    }

    @Test
    @DisplayName("Negated character class with range")
    void testNegatedCharacterClassRange() {
        WildMatcher matcher = new WildMatcher("test[!0-5]");
        assertTrue(matcher.matches("test6"));
        assertTrue(matcher.matches("test9"));
        assertTrue(matcher.matches("testa"));
        assertFalse(matcher.matches("test0"));
        assertFalse(matcher.matches("test3"));
    }

    // Multiple patterns (comma-separated)

    @Test
    @DisplayName("Multiple patterns with OR logic")
    void testMultiplePatterns() {
        WildMatcher matcher = new WildMatcher("comp.*,alt.*");
        assertTrue(matcher.matches("comp.lang.java"));
        assertTrue(matcher.matches("alt.test"));
        assertFalse(matcher.matches("misc.test"));
    }

    @Test
    @DisplayName("Multiple patterns with three options")
    void testThreePatterns() {
        WildMatcher matcher = new WildMatcher("comp.*,alt.*,misc.*");
        assertTrue(matcher.matches("comp.lang.java"));
        assertTrue(matcher.matches("alt.test"));
        assertTrue(matcher.matches("misc.test"));
        assertFalse(matcher.matches("news.test"));
    }

    // Negation tests

    @Test
    @DisplayName("Simple negation excludes pattern")
    void testSimpleNegation() {
        WildMatcher matcher = new WildMatcher("!misc.*");
        assertTrue(matcher.matches("comp.lang.java"));
        assertTrue(matcher.matches("alt.test"));
        assertFalse(matcher.matches("misc.test"));
        assertFalse(matcher.matches("misc."));
    }

    @Test
    @DisplayName("Combined inclusive and exclusive patterns")
    void testCombinedInclusiveExclusive() {
        WildMatcher matcher = new WildMatcher("comp.*,!comp.lang.java");
        assertTrue(matcher.matches("comp.sys.unix"));
        assertTrue(matcher.matches("comp.lang.c"));
        assertFalse(matcher.matches("comp.lang.java"));
        assertFalse(matcher.matches("alt.test"));
    }

    @Test
    @DisplayName("Multiple exclusions")
    void testMultipleExclusions() {
        WildMatcher matcher = new WildMatcher("comp.*,!comp.lang.java,!comp.sys.unix");
        assertTrue(matcher.matches("comp.lang.c"));
        assertTrue(matcher.matches("comp.text.editor"));
        assertFalse(matcher.matches("comp.lang.java"));
        assertFalse(matcher.matches("comp.sys.unix"));
    }

    // Escape character tests

    @Test
    @DisplayName("Escaped special characters")
    void testEscapedCharacters() {
        WildMatcher matcher = new WildMatcher("test\\*value");
        assertTrue(matcher.matches("test*value"));
        assertFalse(matcher.matches("testvalue"));
        assertFalse(matcher.matches("testanyvalue"));
    }

    @Test
    @DisplayName("Escaped question mark")
    void testEscapedQuestionMark() {
        WildMatcher matcher = new WildMatcher("test\\?");
        assertTrue(matcher.matches("test?"));
        assertFalse(matcher.matches("testa"));
        assertFalse(matcher.matches("test"));
    }

    @Test
    @DisplayName("Escaped comma in pattern")
    void testEscapedComma() {
        WildMatcher matcher = new WildMatcher("test\\,value");
        assertTrue(matcher.matches("test,value"));
        assertFalse(matcher.matches("testvalue"));
    }

    // Case sensitivity tests

    @Test
    @DisplayName("Case sensitive matching (default)")
    void testCaseSensitive() {
        WildMatcher matcher = new WildMatcher("comp.lang.*");
        assertTrue(matcher.matches("comp.lang.Java"));
        assertFalse(matcher.matches("COMP.LANG.JAVA"));
        assertFalse(matcher.matches("Comp.Lang.Java"));
    }

    @Test
    @DisplayName("Case insensitive matching")
    void testCaseInsensitive() {
        WildMatcher matcher = new WildMatcher("comp.lang.*", false);
        assertTrue(matcher.matches("comp.lang.java"));
        assertTrue(matcher.matches("COMP.LANG.JAVA"));
        assertTrue(matcher.matches("Comp.Lang.Java"));
        assertTrue(matcher.matches("CoMp.LaNg.JaVa"));
    }

    // Static utility method tests

    @Test
    @DisplayName("Static matches method")
    void testStaticMatches() {
        assertTrue(WildMatcher.matches("comp.lang.java", "comp.*"));
        assertFalse(WildMatcher.matches("alt.test", "comp.*"));
    }

    @Test
    @DisplayName("Static matches with case sensitivity")
    void testStaticMatchesWithCase() {
        assertTrue(WildMatcher.matches("COMP.LANG.JAVA", "comp.*", false));
        assertFalse(WildMatcher.matches("COMP.LANG.JAVA", "comp.*", true));
    }

    // Accessor method tests

    @Test
    @DisplayName("Get inclusive expressions")
    void testGetInclusiveExprs() {
        WildMatcher matcher = new WildMatcher("comp.*,alt.*");
        String[] inclusive = matcher.getInclusiveExprs();
        assertEquals(2, inclusive.length);
        assertArrayEquals(new String[]{"comp.*", "alt.*"}, inclusive);
    }

    @Test
    @DisplayName("Get exclusive expressions")
    void testGetExclusiveExprs() {
        WildMatcher matcher = new WildMatcher("comp.*,!misc.*,!alt.*");
        String[] exclusive = matcher.getExclusiveExprs();
        assertEquals(2, exclusive.length);
        assertArrayEquals(new String[]{"misc.*", "alt.*"}, exclusive);
    }

    @Test
    @DisplayName("Get original expression")
    void testGetExpr() {
        WildMatcher matcher = new WildMatcher("comp.*,!misc.*");
        assertEquals("comp.*,!misc.*", matcher.getExpr());
    }

    @Test
    @DisplayName("Check if pattern is negated")
    void testIsNegated() {
        WildMatcher negated = new WildMatcher("!misc.*");
        WildMatcher notNegated = new WildMatcher("comp.*");
        assertTrue(negated.isNegated());
        assertFalse(notNegated.isNegated());
    }

    @Test
    @DisplayName("Check if pattern is inclusive")
    void testIsInclusive() {
        WildMatcher inclusive = new WildMatcher("comp.*,alt.*");
        WildMatcher notInclusive = new WildMatcher("!misc.*");
        assertTrue(inclusive.isInclusive());
        assertFalse(notInclusive.isInclusive());
    }

    @Test
    @DisplayName("Check if pattern has wildcard")
    void testHasWildcard() {
        WildMatcher withWildcard = new WildMatcher("comp.*");
        WildMatcher withQuestion = new WildMatcher("alt.??");
        WildMatcher withoutWildcard = new WildMatcher("comp.lang.java");
        
        assertTrue(withWildcard.hasWildcard());
        assertTrue(withQuestion.hasWildcard());
        assertFalse(withoutWildcard.hasWildcard());
    }

    @Test
    @DisplayName("Check case sensitivity flags")
    void testCaseSensitivityFlags() {
        WildMatcher sensitive = new WildMatcher("comp.*", true);
        WildMatcher insensitive = new WildMatcher("comp.*", false);
        
        assertTrue(sensitive.isCaseSensitive());
        assertFalse(sensitive.isCaseInsensitive());
        
        assertFalse(insensitive.isCaseSensitive());
        assertTrue(insensitive.isCaseInsensitive());
    }

    // Edge cases and error handling

    @Test
    @DisplayName("Null expression throws exception")
    void testNullExpression() {
        assertThrows(IllegalArgumentException.class, () -> new WildMatcher(null));
    }

    @Test
    @DisplayName("Empty expression throws exception")
    void testEmptyExpression() {
        assertThrows(IllegalArgumentException.class, () -> new WildMatcher(""));
    }

    @Test
    @DisplayName("Null text doesn't match")
    void testNullText() {
        WildMatcher matcher = new WildMatcher("comp.*");
        assertFalse(matcher.matches(null));
    }

    @Test
    @DisplayName("Empty pattern list with exclusion")
    void testEmptyPatternWithExclusion() {
        WildMatcher matcher = new WildMatcher("!misc.*");
        assertTrue(matcher.matches("comp.lang.java"));
        assertFalse(matcher.matches("misc.test"));
    }

    @Test
    @DisplayName("Exact match without wildcards")
    void testExactMatch() {
        WildMatcher matcher = new WildMatcher("comp.lang.java");
        assertTrue(matcher.matches("comp.lang.java"));
        assertFalse(matcher.matches("comp.lang.javascript"));
        assertFalse(matcher.matches("comp.lang.jav"));
    }

    // Complex real-world scenarios

    @ParameterizedTest
    @CsvSource({
        "comp.lang.java, comp.*, true",
        "comp.lang.java, comp.lang.*, true",
        "comp.lang.java, comp.*.java, true",
        "comp.lang.java, alt.*, false",
        "alt.binaries.test, alt.binaries.*, true",
        "news.announce, news.*, true",
        "news.announce, !news.*, false"
    })
    @DisplayName("Parameterized newsgroup pattern matching")
    void testNewsgroupPatterns(String newsgroup, String pattern, boolean expected) {
        assertEquals(expected, WildMatcher.matches(newsgroup, pattern));
    }

    @Test
    @DisplayName("Complex NNTP-style pattern")
    void testComplexNNTPPattern() {
        // Pattern: match comp.* and alt.* but exclude comp.lang.java and alt.binaries.*
        WildMatcher matcher = new WildMatcher("comp.*,alt.*,!comp.lang.java,!alt.binaries.*");
        
        assertTrue(matcher.matches("comp.sys.unix"));
        assertTrue(matcher.matches("alt.test"));
        assertTrue(matcher.matches("comp.lang.c"));
        
        assertFalse(matcher.matches("comp.lang.java"));
        assertFalse(matcher.matches("alt.binaries.test"));
        assertFalse(matcher.matches("misc.test"));
    }

    @Test
    @DisplayName("Pattern with special regex characters")
    void testSpecialRegexCharacters() {
        WildMatcher matcher = new WildMatcher("test.value");
        assertTrue(matcher.matches("test.value"));
        assertFalse(matcher.matches("testXvalue"));
    }

    @Test
    @DisplayName("Pattern with parentheses")
    void testParentheses() {
        WildMatcher matcher = new WildMatcher("test(*)value");
        assertTrue(matcher.matches("test(*)value"));
        assertTrue(matcher.matches("test()value"));
    }

    @Test
    @DisplayName("Empty string matching")
    void testEmptyString() {
        WildMatcher matchAll = new WildMatcher("*");
        WildMatcher matchNone = new WildMatcher("test");
        
        assertTrue(matchAll.matches(""));
        assertFalse(matchNone.matches(""));
    }

    @Test
    @DisplayName("Pattern with only asterisk")
    void testOnlyAsterisk() {
        WildMatcher matcher = new WildMatcher("*");
        assertTrue(matcher.matches("anything"));
        assertTrue(matcher.matches("comp.lang.java"));
        assertTrue(matcher.matches(""));
    }

    @Test
    @DisplayName("ToString provides meaningful output")
    void testToString() {
        WildMatcher matcher = new WildMatcher("comp.*,!misc.*");
        String result = matcher.toString();
        assertTrue(result.contains("comp.*,!misc.*"));
        assertTrue(result.contains("inclusive=1"));
        assertTrue(result.contains("exclusive=1"));
    }
}
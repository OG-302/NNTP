package org.anarplex.lib.nntp.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * WildMatcher converts wildmat expressions into Java regular expressions for matching.
 * Wildmat format is defined by Salz, R., "Manual Page for wildmat(3) from the INN 1.4 distribution, 
 * Revision 1.10", April 1992.
 * Wildmat syntax:
 * - * matches zero or more characters
 * - ? matches exactly one character
 * - [abc] matches one character from the set
 * - [!abc] or [^abc] matches one character NOT in the set
 * - [a-z] matches one character in the range
 * - \c escapes special characters
 * - Comma separates multiple patterns (logical OR)
 * - ! prefix negates the pattern (exclusion)
 * Examples:
 * - "comp.*" matches "comp.lang.java", "comp.sys.unix"
 * - "alt.??" matches "alt.tv", "alt.os" but not "alt.test"
 * - "!misc.*" excludes anything starting with "misc."
 * - "comp.*,alt.*" matches either comp.* or alt.*
 */
public class WildMatcher {
    protected static final String EXCLUDED_CHARACTERS = "\\][";
    
    private final String originalExpr;
    private final List<Pattern> inclusivePatterns;
    private final List<Pattern> exclusivePatterns;
    private final List<String> inclusiveExprs;
    private final List<String> exclusiveExprs;
    private final boolean caseSensitive;

    /**
     * Creates a new WildMatcher from a wildmat expression.
     * Wildmat expressions may contain:
     * - Multiple patterns separated by commas: "comp.*,alt.*"
     * - Negated patterns with ! prefix: "!misc.*"
     * - Wildcards: * (zero or more), ? (exactly one)
     * - Character classes: [abc], [!abc], [a-z]
     * 
     * @param expr the wildmat expression
     * @throws IllegalArgumentException if the expression is invalid
     */
    public WildMatcher(String expr) {
        this(expr, true);
    }

    /**
     * Creates a new WildMatcher with specified case sensitivity.
     * 
     * @param expr the wildmat expression
     * @param caseSensitive true for case-sensitive matching, false for case-insensitive
     * @throws IllegalArgumentException if the expression is invalid
     */
    public WildMatcher(String expr, boolean caseSensitive) {
        if (expr == null || expr.isEmpty()) {
            throw new IllegalArgumentException("Wildmat expression cannot be null or empty");
        }

        this.originalExpr = expr;
        this.caseSensitive = caseSensitive;
        this.inclusivePatterns = new ArrayList<>();
        this.exclusivePatterns = new ArrayList<>();
        this.inclusiveExprs = new ArrayList<>();
        this.exclusiveExprs = new ArrayList<>();

        parseExpression(expr);
    }

    /**
     * Parses the wildmat expression and builds regex patterns.
     */
    private void parseExpression(String expr) {
        // Split on commas (but not escaped commas)
        String[] patterns = splitOnUnescapedComma(expr);

        for (String pattern : patterns) {
            pattern = pattern.trim();
            if (pattern.isEmpty()) {
                continue;
            }

            boolean isNegated = false;
            
            // Check for negation prefix
            if (pattern.startsWith("!")) {
                isNegated = true;
                pattern = pattern.substring(1);
            }

            // Convert wildmat to regex
            String regex = wildmatToRegex(pattern);
            
            try {
                int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
                Pattern compiledPattern = Pattern.compile(regex, flags);
                
                if (isNegated) {
                    exclusivePatterns.add(compiledPattern);
                    exclusiveExprs.add(pattern);
                } else {
                    inclusivePatterns.add(compiledPattern);
                    inclusiveExprs.add(pattern);
                }
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid wildmat pattern: " + pattern, e);
            }
        }

        // If no inclusive patterns, default to match all
        if (inclusivePatterns.isEmpty() && !exclusivePatterns.isEmpty()) {
            inclusivePatterns.add(Pattern.compile(".*"));
            inclusiveExprs.add("*");
        }
    }

    /**
     * Splits expression on commas, but not escaped commas.
     */
    private String[] splitOnUnescapedComma(String expr) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;

        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);

            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
                current.append(c);
            } else if (c == ',') {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts.toArray(new String[0]);
    }

    /**
     * Converts a wildmat pattern to a Java regular expression.
     * Wildmat rules:
     * - * → .* (zero or more of any character)
     * - ? → . (exactly one character)
     * - [abc] → [abc] (character class - preserved)
     * - [!abc] or [^abc] → [^abc] (negated character class)
     * - [a-z] → [a-z] (character range - preserved)
     * - Other characters are escaped for regex
     * 
     * @param wildmat the wildmat pattern
     * @return the equivalent Java regex pattern
     */
    private String wildmatToRegex(String wildmat) {
        StringBuilder regex = new StringBuilder("^");
        boolean escaped = false;
        boolean inCharClass = false;

        for (int i = 0; i < wildmat.length(); i++) {
            char c = wildmat.charAt(i);

            if (escaped) {
                // Escape special regex characters
                regex.append(Pattern.quote(String.valueOf(c)));
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '[' && !inCharClass) {
                inCharClass = true;
                regex.append('[');
                
                // Handle negation in character class
                if (i + 1 < wildmat.length() && wildmat.charAt(i + 1) == '!') {
                    regex.append('^');
                    i++; // Skip the !
                }
            } else if (c == ']' && inCharClass) {
                inCharClass = false;
                regex.append(']');
            } else if (inCharClass) {
                // Inside character class, preserve as-is (ranges, etc.)
                regex.append(c);
            } else if (c == '*') {
                regex.append(".*");
            } else if (c == '?') {
                regex.append('.');
            } else {
                // Escape special regex characters
                if ("(){}+|^$.".indexOf(c) >= 0) {
                    regex.append('\\');
                }
                regex.append(c);
            }
        }

        regex.append('$');
        return regex.toString();
    }

    /**
     * Tests whether the given string matches this wildmat expression.
     * Matching logic:
     * 1. String must match at least one inclusive pattern (if any exist)
     * 2. String must NOT match any exclusive pattern
     * 
     * @param text the string to test
     * @return true if the string matches, false otherwise
     */
    public boolean matches(String text) {
        if (text == null) {
            return false;
        }

        // Check exclusive patterns first (negations)
        for (Pattern pattern : exclusivePatterns) {
            if (pattern.matcher(text).matches()) {
                return false; // Explicitly excluded
            }
        }

        // Check inclusive patterns
        if (inclusivePatterns.isEmpty()) {
            return true; // No inclusive patterns means match all (except exclusions)
        }

        for (Pattern pattern : inclusivePatterns) {
            if (pattern.matcher(text).matches()) {
                return true; // Matched at least one inclusive pattern
            }
        }

        return false; // Didn't match any inclusive pattern
    }

    /**
     * Static utility method to check if a string matches a wildmat pattern.
     * 
     * @param text the string to test
     * @param wildmatExpr the wildmat expression
     * @return true if the text matches the wildmat expression
     */
    public static boolean matches(String text, String wildmatExpr) {
        return new WildMatcher(wildmatExpr).matches(text);
    }

    /**
     * Static utility method with case sensitivity option.
     * 
     * @param text the string to test
     * @param wildmatExpr the wildmat expression
     * @param caseSensitive true for case-sensitive matching
     * @return true if the text matches the wildmat expression
     */
    public static boolean matches(String text, String wildmatExpr, boolean caseSensitive) {
        return new WildMatcher(wildmatExpr, caseSensitive).matches(text);
    }

    // Accessor methods

    public String[] getInclusiveExprs() {
        return inclusiveExprs.toArray(new String[0]);
    }

    public String[] getExclusiveExprs() {
        return exclusiveExprs.toArray(new String[0]);
    }

    public String getExpr() {
        return originalExpr;
    }

    public boolean isNegated() {
        return !exclusivePatterns.isEmpty() && inclusivePatterns.size() == 1 
               && inclusivePatterns.getFirst().pattern().equals(".*");
    }

    public boolean isInclusive() {
        return !inclusivePatterns.isEmpty() && exclusivePatterns.isEmpty();
    }

    public boolean isExclusive() {
        return inclusivePatterns.isEmpty() || isNegated();
    }

    public boolean hasWildcard() {
        return originalExpr.contains("*") || originalExpr.contains("?");
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public boolean isCaseInsensitive() {
        return !caseSensitive;
    }

    @Override
    public String toString() {
        return "WildMatcher{" +
                "expr='" + originalExpr + '\'' +
                ", inclusive=" + inclusiveExprs.size() +
                ", exclusive=" + exclusiveExprs.size() +
                ", caseSensitive=" + caseSensitive +
                '}';
    }
}

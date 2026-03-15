package org.anarplex.lib.nntp;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Public test double for IdentityService used across ProtocolEngine tests.
 */
public class MockIdentityService implements IdentityService {
    private long nextToken = 1L;
    public boolean closed = false;

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
        String p = subject.getPrincipal();
        if (p == null) return null;
        if ("nopass".equalsIgnoreCase(p)) return false;
        if ("user".equals(p)) return true;
        return null; // unknown user
    }

    @Override
    public Long authenticate(Subject subject, String credentials) {
        if (subject == null) return null;
        String p = subject.getPrincipal();
        if ("nopass".equalsIgnoreCase(p)) return nextToken++;
        if ("user".equals(p) && "pass".equals(credentials)) return nextToken++;
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

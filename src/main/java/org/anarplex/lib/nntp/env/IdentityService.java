package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;

import java.util.Map;
import java.util.Set;

public interface IdentityService extends AutoCloseable {

    interface Subject {
        String getPrincipal();
    }

    /**
     * Some Authentication variants of RFC4643 only require the Username, no password.
     * This method returns true if the supplied Subject requires a password, false if not, and null if no such User.
     */
    Boolean requiresPassword(Subject subject);


    /**
     * Authenticate authenticates the supplied identity using the supplied credentials and returns a token for
     * later Authentication and Authorization tests.  A valid token is never 0.
     * @return a valid token or nil if authentication fails
     */
    Long authenticate(Subject subject, String credentials);

    /**
     * IsValid returns true if the supplied authenticationToken is still valid.
     * @return true if the supplied authenticationToken is still valid, false otherwise.
     */
    boolean isValid(long token);

    /**
     * GetHostIdentifier returns a string that (as far as possible) uniquely identifies this host within the set of
     * all other NNTP hosts to which this host will be exchanging articles.  The value must not only be unique in that set
     * but also immutable to ensure this host does not inadvertently receive articles which it has already processed.
     * Used during Path construction of IHAVE and POST commands.
     * @return a string that (as far as possible) uniquely identifies this host within the set of all other NNTP hosts to which this host will be exchanging articles.
     */
    String getHostIdentifier();

    /**
     * CreateMessageID creates a Message-ID conforming to RFC-5536 Section 3.1.3
     * @return a Message-ID conforming to RFC-5536 Section 3.1.3
     */
    Specification.MessageId createMessageID(Map<String, Set<String>> articleHeaders);
}

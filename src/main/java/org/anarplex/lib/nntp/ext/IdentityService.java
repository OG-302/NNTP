package org.anarplex.lib.nntp.ext;

import org.anarplex.lib.nntp.Spec;

import java.util.Map;
import java.util.Set;

public interface IdentityService extends AutoCloseable {

    /**
     * Authenticate authenticates the supplied identity using the supplied credentials and returns a token for
     * later Authentication and Authorization tests.  A valid token is never 0.
     * @param identity
     * @param credentials
     * @return a valid token or nil if authentication fails
     */
    Long authenticate(String identity, String credentials);

    /**
     * IsValid returns true if the supplied authenticationToken is still valid.
     * @param token
     * @return true if the supplied authenticationToken is still valid, false otherwise.
     */
    boolean isValid(long token);

    /**
     * AuthorizedToPost determines whether the entity identified (and authenticated previously) by the supplied token is
     * allowed to Post to the supplied Newsgroup.
     * Precondition: token must be valid. i.e. (isValid(token) == true)
     * @param token
     * @param newsgroup
     * @return true if the entity identified by the supplied token is authorized to Post to the supplied Newsgroup, false otherwise.
     */
    boolean authorizedToPost(long token, PersistenceService.Newsgroup newsgroup);

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
     * @param articleHeaders
     * @return a Message-ID conforming to RFC-5536 Section 3.1.3
     */
    Spec.MessageId createMessageID(Map<String, Set<String>> articleHeaders);
}

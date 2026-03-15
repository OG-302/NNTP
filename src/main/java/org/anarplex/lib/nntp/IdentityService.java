package org.anarplex.lib.nntp;

import java.util.Map;
import java.util.Set;

/**
 * The IdentityService interface provides an abstraction for managing authentication and the two other identity-related
 * tasks: Unique Host Identification and MessageIds.
 * - Authentication is based on credentials supplied by the subject (client).  Once authenticated, a token is returned
 * for later validation.  Actions related to NNTP requests which require authorization (such as Posting) will reference
 * the client via this validated subject.
 * - Unique Host Identification is needed when sharing articles between NNTP peers and appears in the mandatory
 * StoredArticle Header - Path. Obtaining this value for this host is done via the getHostIdentifier() method.
 * - MessageIds uniquely identify messages within the NNTP protocol, and articles submitted to the NNTP server via
 * the POST request (from a client) will likely lack a messageId.  Hence, a new MessageId will be sought from the
 * createMessageID() method which can use the supplied article headers to generate a unique MessageId.
 * <p/>
 * To use this NNTP library, an implementation of this interface will need to be provided.
 */
public interface IdentityService extends AutoCloseable {

    /**
     * A Subject is what is authenticated by this IdentityService.  All Subjects have a Principal associated with them.
     */
    interface Subject {
        /**
         * GetPrincipal returns the Principal associated with this Subject.
         */
        String getPrincipal();

        /**
         * An immutable and unique identifier for this Subject.
         */
        long getIdentifier();
    }

    Subject newSubject(String principal);

    /**
     * Some Authentication variants of RFC4643 only require the Username, no password.
     * @return true if the supplied Subject requires a password for authentication, false if not, and null if no such Subject exists.
     */
    Boolean requiresPassword(Subject subject);


    /**
     * Authenticate authenticates the supplied Subject using the supplied Credentials and returns a token for
     * later reauthentication. A valid token is never 0L.
     * @return a valid token or null if authentication fails
     */
    Long authenticate(Subject subject, String credentials);

    /**
     * IsValid returns true if the supplied authenticationToken is valid.
     * @return true if the supplied authenticationToken is valid, false otherwise.
     */
    boolean isValid(long token);

    /**
     * GetHostIdentifier returns a value that is unique among all the other NNTP hosts to which this host will be
     * exchanging articles and which is also immutable.  This value is used in the PATH field of article headers to
     * ensure this host does not inadvertently receive articles which it has already processed.
     * It is used during Path construction of IHAVE and POST commands.
     * @return a string that (as far as possible) uniquely identifies this host within the set of all other NNTP hosts
     * to which this host will be exchanging articles and which does not change over time.
     */
    String getHostIdentifier();

    /**
     * Creates a Message-ID conforming to the messageID syntax of RFC-5536 Section 3.1.3.  The new Message-ID SHOULD
     * be unique for a given set of article headers.
     * @return a Message-ID
     */
    Specification.MessageId createMessageID(Map<String, Set<String>> articleHeaders);
}

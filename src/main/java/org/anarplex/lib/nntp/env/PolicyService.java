package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;

import javax.security.auth.Subject;
import java.io.StringReader;
import java.util.Map;
import java.util.Set;

/**
 * The PolicyService interface provides a simple API for determining whether a given article or newsgroup is allowed
 * to be posted.  Posting can be via the POST command or via IHAVE.
 * The PolicyService is also consulted to determine whether a new newsgroup should be created or not.
 */
public interface PolicyService extends AutoCloseable {

    /**
     * isPostingAllowed determines whether the specified submitter is allowed to submit (new) articles via the POST
     * command.
     *
     * @param submitter or null if the submitter is unknown or anonymous
     * @return true if article Posting by this submitter is allowed, false otherwise
     */
    boolean isPostingAllowed(Subject submitter);

    /**
     * isIHaveAllowed determines whether the specified article which is being submitted via IHAVE by the specified
     * submitter is to be allowed into the specified newsgroup, false otherwise.
     *

     * @param submitter
     * @return true if the article Posting to the newsgroup is allowed, false otherwise
     */
    boolean isIHaveTransferAllowed(Subject submitter);

    /**
     * isNewsgroupAllowed indicates whether the specified newsgroup is allowed or not.  If the newsgroup is allowed,
     * then articles found posted to this newsgroup on other Peers will be fetched.  If not allowed, then such articles
     * will be ignored.  In either case, a record of this newsgroup will be created in the database so that later
     * encounters of this newsgroup will follow the same policy.
     *
     * @param newsgroup
     * @param postingMode
     * @param estNumArticles
     * @param advertisingPeer the subject of the advertising peer
     * @return true if the newsgroup is allowed, false otherwise
     */
    boolean isNewsgroupAllowed(Specification.NewsgroupName newsgroup, Specification.PostingMode postingMode, int estNumArticles, Subject advertisingPeer);

    /**
     * isArticleAllowed determines whether the specified article which is being obtained from the specified submitter
     * is to be allowed into the specified newsgroup, false otherwise.
     *
     * @param messageId
     * @param headerMap
     * @param bodyReader
     * @param destination
     * @param postingMode
     * @param submitter
     * @return true if the article is allowed, false otherwise
     */
    boolean isArticleAllowed(Specification.MessageId messageId, Map<String, Set<String>> headerMap, StringReader bodyReader, Specification.NewsgroupName destination, Specification.PostingMode postingMode, Subject submitter);

}

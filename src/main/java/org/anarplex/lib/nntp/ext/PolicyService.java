package org.anarplex.lib.nntp.ext;

import org.anarplex.lib.nntp.Spec;

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
     * @param submitter
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
     * isNewsgroupAllowed determines whether the specified newsgroup is to be allowed, false if not.
     *
     * @param newsgroup
     * @param postingMode
     * @param estNumArticles
     * @param advertisingPeer the subject of the advertising peer
     * @return true if the newsgroup is allowed, false otherwise
     */
    boolean isNewsgroupAllowed(Spec.NewsgroupName newsgroup, Spec.PostingMode postingMode, int estNumArticles, Subject advertisingPeer);

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
    boolean isArticleAllowed(Spec.MessageId messageId, Map<String, Set<String>> headerMap, StringReader bodyReader, Spec.NewsgroupName destination, Spec.PostingMode postingMode, Subject submitter);

}

package org.anarplex.lib.nntp;

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
    boolean isPostingAllowedBy(IdentityService.Subject submitter);

    /**
     * isIHaveAllowed determines whether the specified article which is being submitted via IHAVE by the specified
     * submitter is to be allowed into the specified newsgroup, false otherwise.
     *
     * @param submitter or null if the submitter is unknown or anonymous
     * @return true if the article Posting to the newsgroup is allowed, false otherwise
     */
    boolean isIHaveTransferAllowedBy(IdentityService.Subject submitter);


    /**
     * This method is called when a new Article is received from a client or a Peer.  The implementer should
     * indicate via the return value whether the article should be kept for subsequent review (Allow), ignored and
     * thus not processed (Ignore) or banned such that any Article with this messageId will never be accepted in the
     * future (Ban).
     * This method presents implementations with the opportunity to examine the article and its headers, and from that
     * determine whether this is a "legitimate" article or not.  Exactly what constitutes a "legitimate" article is
     * up to the implementer to determine, but it could, for instance, include a valid signed hash of the Article's body.
     */
    ArticleReviewOutcome reviewArticle(Specification.Article article,
                                       Specification.ArticleSource articleSource,
                                       IdentityService.Subject sender);

    enum ArticleReviewOutcome {
        Allow,
        Ignore,
        Ban
    }

    /**
     * Provides the implementer with an Article-Newsgroup association which has already been stored in the
     * datastore, according to the Article's Newsgroups header field.
     * The implementer should decide whether the article should be allowed to be posted to the newsgroup, or rejected,
     * or the decision deferred for a later review.  The article can be posted or rejected via the calls
     * PendingNewsgroupArticle.publish() and PendingNewsgroupArticle.reject() respectively.  Furthermore, the entire
     * article (which may appear in more than one Newsgroup) can be banned from all Newsgroups via the call
     * StoredArticle.ban().
     */
    void reviewPosting(PersistenceService.PendingArticle submission);

}

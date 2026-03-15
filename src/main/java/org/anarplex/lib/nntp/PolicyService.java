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


    ArticleReviewOutcome reviewArticle(Specification.Article article,
                                       Specification.ArticleSource articleSource,
                                       IdentityService.Subject sender);

    enum ArticleReviewOutcome {
        Allow,
        Ignore,
        Ban
    }

    /**
     * Provides the implementer with a supplied Article Newsgroup association which has already been stored in the
     * datastore and which has already had its NewsgroupArticle associations created, according to the Article's
     * Newsgroups header field.
     * The implementer should then decide whether the article should be allowed to be posted to the newsgroup, rejected
     * or the decision deferred until the next review.  The article can be posted or rejected via the calls
     * PendingNewsgroupArticle.publish() and PendingNewsgroupArticle.reject().  Furthermore, the entire article can be
     * banned via the call StoredArticle.ban().
     */
    void reviewPosting(PersistenceService.PendingArticle submission);

}

package org.anarplex.lib.nntp;

/**
 * Public test double for PolicyService used across ProtocolEngine tests.
 */
public class MockPolicyService implements PolicyService {
    public boolean closed = false;

    @Override
    public boolean isPostingAllowedBy(IdentityService.Subject submitter) {
        return submitter != null && submitter.getPrincipal() != null && submitter.getPrincipal().contains("allowed");
    }

    @Override
    public boolean isIHaveTransferAllowedBy(IdentityService.Subject submitter) {
        return true;
    }

    @Override
    public ArticleReviewOutcome reviewArticle(Specification.Article article, Specification.ArticleSource articleSource, IdentityService.Subject sender) {
        return ArticleReviewOutcome.Allow;
    }

    @Override
    public void reviewPosting(PersistenceService.PendingArticle submission) {
        // accept by default in basic tests
    }

    @Override
    public void close() {
        closed = true;
    }
}

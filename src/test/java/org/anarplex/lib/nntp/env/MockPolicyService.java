package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;

import javax.security.auth.Subject;
import java.io.StringReader;
import java.util.Map;
import java.util.Set;

public class MockPolicyService implements PolicyService {
    private boolean closed;

    public MockPolicyService() {
        closed = false;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isPostingAllowed(Subject submitter) {
        return true;
    }

    @Override
    public boolean isIHaveTransferAllowed(Subject submitter) {
        return true;
    }

    @Override
    public boolean isNewsgroupAllowed(Specification.NewsgroupName newsgroup, Specification.PostingMode postingMode, int estNumArticles, Subject advertisingPeer) {
        return true;
    }

    @Override
    public boolean isArticleAllowed(Specification.MessageId messageId, Map<String, Set<String>> headerMap, StringReader bodyReader, Specification.NewsgroupName destination, Specification.PostingMode postingMode, Subject submitter) {
        return true;
    }
}

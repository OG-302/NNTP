package org.anarplex.lib.nntp;

import java.time.Instant;
import java.util.*;

/**
 * Public in-memory PersistenceService for ProtocolEngine tests.
 * Provides just enough behavior to exercise protocol commands around groups and
 * article retrieval/navigation. Not for production use.
 */
public class InMemoryPersistence extends PersistenceService {
    public boolean closed = false;

    private final Map<Specification.NewsgroupName, TestPublishedNewsgroup> groups = new LinkedHashMap<>();
    private final Map<Specification.MessageId, TestStoredArticle> articles = new HashMap<>();
    private final Set<String> savedIds = new HashSet<>();
    private final Map<String, String> savedValues = new HashMap<>();

    @Override protected void init() {}
    @Override protected void checkpoint() {}
    @Override protected void rollback() {}
    @Override protected void commit() {}
    @Override protected void close() { closed = true; }

    // ---- Test helpers ----
    public TestPublishedNewsgroup seedGroup(String name, String description, Specification.PostingMode mode) {
        try {
            Specification.NewsgroupName nn = new Specification.NewsgroupName(name);
            TestPublishedNewsgroup g = new TestPublishedNewsgroup(nn, description, mode, "tester");
            groups.put(nn, g);
            return g;
        } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public TestPublishedArticle seedArticle(TestPublishedNewsgroup group, String messageId, Map<String, Set<String>> headers, String body) {
        try {
            Specification.MessageId mid = new Specification.MessageId(messageId);
            Specification.Article.ArticleHeaders ah = new Specification.Article.ArticleHeaders(headers);
            TestStoredArticle sa = new TestStoredArticle(mid, Specification.ArticleSource.Posting, ah, body, null);
            articles.put(mid, sa);
            return group.publish(sa);
        } catch (Specification.MessageId.InvalidMessageIdException | Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // ---- Articles ----
    @Override
    protected PendingArticle addArticle(Specification.Article article, Specification.ArticleSource articleSource, IdentityService.Subject submitter) {
        if (!(article instanceof Specification.Article a)) throw new IllegalArgumentException("article type");
        TestStoredArticle sa = new TestStoredArticle(a.getMessageId(), articleSource, a.getAllHeaders(), a.getBody(), submitter);
        articles.put(a.getMessageId(), sa);
        return new TestPendingArticle(sa, null);
    }

    @Override
    protected void deleteArticle(Specification.MessageId messageId) {
        articles.remove(messageId);
        // also remove from groups
        for (TestPublishedNewsgroup g : groups.values()) {
            g.removeArticle(messageId);
        }
    }

    @Override
    protected StoredArticle getArticle(Specification.MessageId messageId) { return articles.get(messageId); }

    @Override
    protected boolean hasArticle(Specification.MessageId messageId) { return articles.containsKey(messageId); }

    @Override
    public Iterator<StoredArticle> listArticles() {
        List<StoredArticle> list = new ArrayList<>();
        list.addAll(articles.values());
        return list.iterator();
    }

    // ---- Groups ----
    @Override
    protected StoredNewsgroup addNewsgroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, String createdBy) {
        TestPublishedNewsgroup g = new TestPublishedNewsgroup(name, description, postingMode, createdBy);
        groups.put(name, g);
        return g;
    }

    @Override
    protected void deleteNewsgroup(StoredNewsgroup newsgroup) {
        if (newsgroup != null) groups.remove(newsgroup.getName());
    }

    @Override
    public StoredNewsgroup getNewsgroup(Specification.NewsgroupName name) { return groups.get(name); }

    @Override
    public Iterator<PublishedNewsgroup> listPublishedGroups() {
        List<PublishedNewsgroup> list = new ArrayList<>();
        list.addAll(groups.values());
        return list.iterator();
    }

    @Override
    public Iterator<StoredNewsgroup> listAllGroups() {
        List<StoredNewsgroup> list = new ArrayList<>();
        list.addAll(groups.values());
        return list.iterator();
    }

    @Override
    public Iterator<PublishedNewsgroup> listGroupsAddedSince(Instant since) { return listPublishedGroups(); }

    @Override
    public StoredNewsgroup getGroupByName(Specification.NewsgroupName name) { return groups.get(name); }

    // ---- Peers ----
    @Override
    public Peer addPeer(String label, String address) throws ExistingPeerException { throw new ExistingPeerException("not used"); }

    @Override
    protected Iterator<Peer> getPeers() { return Collections.emptyIterator(); }

    // ---- Associations ----
    @Override
    protected PendingArticle addAssociation(StoredArticle storedArticle, StoredNewsgroup newsgroup) {
        if (!(newsgroup instanceof TestPublishedNewsgroup g) || !(storedArticle instanceof TestStoredArticle sa))
            throw new IllegalArgumentException("types");
        return new TestPendingArticle(sa, g);
    }

    @Override
    protected void deleteAssociation(NewsgroupArticle newsgroupArticle) { /* not needed */ }

    // ---- Bans ----
    @Override
    protected void saveId(String key) { savedIds.add(key); }

    @Override
    protected boolean existsId(String key) { return savedIds.contains(key); }

    /**
     * Persists the supplied value in the database.  The value is associated with the supplied key.
     * If the key already exists, then the value is updated.  If the key does not exist, then it is created.
     * If the value is null, then the key and its value are deleted.
     */
    @Override
    protected void saveValue(String key, String value) {
        savedValues.put(key, value);
    }

    /**
     * Retrieves the value associated with the supplied key.  Returns null if no such key exists.
     */
    @Override
    protected String fetchValue(String key) {
        return savedValues.get(key);
    }

    @Override
    protected void deletePeer(Peer peer) { /* no-op for tests */ }

    // ===== Implementation classes for tests =====
    public static class TestStoredArticle extends StoredArticle {
        private final ArticleHeaders headers;
        private final String body;
        private final IdentityService.Subject submitter;
        private final Instant insertedAt = Instant.now();
        private final Set<Specification.Newsgroup> published = new LinkedHashSet<>();

        protected TestStoredArticle(Specification.MessageId messageId,
                                    Specification.ArticleSource source,
                                    ArticleHeaders headers,
                                    String body,
                                    IdentityService.Subject submitter) {
            super(messageId, source);
            this.headers = headers;
            this.body = body;
            this.submitter = submitter;
        }

        @Override public Instant getInsertionTime() { return insertedAt; }
        @Override public IdentityService.Subject getSubmitter() { return submitter; }
        @Override public Set<Specification.Newsgroup> getPublished() { return Collections.unmodifiableSet(published); }
        @Override public NewsgroupArticle getNewsgroup(Specification.Newsgroup newsgroup) { return null; }
        @Override public void ban() { /* not needed */ }
        @Override public boolean isPublished() { return !published.isEmpty(); }

        @Override public ArticleHeaders getAllHeaders() { return headers; }
        @Override public String getBody() { return body; }
    }

    public static class TestPublishedNewsgroup extends PublishedNewsgroup {
        private final List<TestPublishedArticle> pub = new ArrayList<>();
        private int nextNumber = 1;

        protected TestPublishedNewsgroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, String createdBy) {
            super(name, description, postingMode, createdBy);
        }

        TestPublishedArticle publish(TestStoredArticle sa) {
            try {
                TestPublishedArticle pa = new TestPublishedArticle(this, sa, new Specification.ArticleNumber(nextNumber++), Instant.now());
                pub.add(pa);
                sa.published.add(this);
                return pa;
            } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                throw new RuntimeException(e);
            }
        }

        void removeArticle(Specification.MessageId id) {
            pub.removeIf(p -> p.article.getMessageId().equals(id));
        }

        @Override protected Feed addFeed(Peer peer) { return null; }
        @Override protected void deleteFeed(Feed feed) { /* not needed */ }
        @Override public Feed[] getFeeds() { return new Feed[0]; }
        @Override public int numPendingArticles() { return 0; }
        @Override public Iterable<? extends PendingArticle> getPendingArticles() { return List.of(); }

        @Override
        protected Iterator<PublishedArticle> getArticlesSince(Instant insertionTime) {
            List<PublishedArticle> list = new ArrayList<>();
            list.addAll(pub);
            return list.iterator();
        }

        @Override
        protected PublishedArticle getArticleNumbered(Specification.ArticleNumber articleNumber) {
            return pub.stream().filter(p -> p.number.equals(articleNumber)).findFirst().orElse(null);
        }

        @Override
        protected Iterator<PublishedArticle> getArticlesNumbered(Specification.ArticleNumber lowerBound, Specification.ArticleNumber upperBound) {
            int low = (lowerBound != null ? lowerBound.getValue() : Integer.MIN_VALUE);
            int high = (upperBound != null ? upperBound.getValue() : Integer.MAX_VALUE);
            List<PublishedArticle> list = new ArrayList<>();
            for (TestPublishedArticle p : pub) {
                int n = p.number.getValue();
                if (low <= n && n <= high) list.add(p);
            }
            return list.iterator();
        }

        @Override
        protected PublishedArticle getFirstArticle() {
            return pub.isEmpty() ? null : pub.get(0);
        }

        @Override
        protected PublishedArticle getNextArticle(Specification.ArticleNumber currentArticle) {
            for (int i = 0; i < pub.size(); i++) {
                if (pub.get(i).number.equals(currentArticle)) {
                    return (i + 1 < pub.size() ? pub.get(i + 1) : null);
                }
            }
            return null;
        }

        @Override
        protected PublishedArticle getPreviousArticle(Specification.ArticleNumber currentArticle) {
            for (int i = 0; i < pub.size(); i++) {
                if (pub.get(i).number.equals(currentArticle)) {
                    return (i - 1 >= 0 ? pub.get(i - 1) : null);
                }
            }
            return null;
        }

        @Override
        public PublishedArticle getPublishedArticle(Specification.MessageId messageId) {
            return pub.stream().filter(p -> p.article.getMessageId().equals(messageId)).findFirst().orElse(null);
        }

        @Override
        protected Metrics getMetrics() {
            try {
                if (pub.isEmpty()) {
                    return new Metrics(0, null, null);
                }
                Specification.ArticleNumber low = new Specification.ArticleNumber(pub.get(0).number.getValue());
                Specification.ArticleNumber high = new Specification.ArticleNumber(pub.get(pub.size() - 1).number.getValue());
                return new Metrics(pub.size(), low, high);
            } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class TestPublishedArticle extends PublishedArticle {
        private final TestPublishedNewsgroup group;
        private final TestStoredArticle article;
        private final Specification.ArticleNumber number;
        private final Instant publishedAt;

        public TestPublishedArticle(TestPublishedNewsgroup group, TestStoredArticle article, Specification.ArticleNumber number, Instant publishedAt) {
            this.group = group;
            this.article = article;
            this.number = number;
            this.publishedAt = publishedAt;
        }

        @Override protected void reject() { /* no-op */ }
        @Override public Specification.ArticleNumber getArticleNumber() { return number; }
        @Override public Instant getPublicationTime() { return publishedAt; }
        @Override protected StoredNewsgroup getNewsgroup() { return group; }
        @Override protected StoredArticle getArticle() { return article; }
    }

    static class TestPendingArticle extends PendingArticle {
        private final TestStoredArticle article;
        private final TestPublishedNewsgroup group;

        TestPendingArticle(TestStoredArticle article, TestPublishedNewsgroup group) {
            this.article = article; this.group = group;
        }

        @Override protected void publish() {
            if (group != null) group.publish(article);
        }

        @Override protected void reject() { /* no-op */ }

        @Override protected StoredNewsgroup getNewsgroup() { return group; }
        @Override protected StoredArticle getArticle() { return article; }
    }
}

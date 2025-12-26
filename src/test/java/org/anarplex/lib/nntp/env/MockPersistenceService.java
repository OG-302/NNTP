package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;

import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * InMemory is an in-memory no-sql datastore implementation of PersistenceService.
 * All data is stored in memory using concurrent collections for thread safety.
 */
public class MockPersistenceService implements PersistenceService {

    private final Map<Specification.MessageId, ArticleImpl> articles = new ConcurrentHashMap<>();
    private final Map<Specification.MessageId, Date> rejectedArticles = new ConcurrentHashMap<>();
    private final Map<Specification.NewsgroupName, NewsgroupImpl> newsgroups = new ConcurrentHashMap<>();
    private final Map<Integer, PeerImpl> peers = new ConcurrentHashMap<>();
    private final AtomicInteger peerIdCounter = new AtomicInteger(1);

    @Override
    public void init() {
        // No initialization needed for in-memory implementation
    }

    @Override
    public void commit() {
        // No commit needed for in-memory implementation
    }

    @Override
    public void close() {
        articles.clear();
        rejectedArticles.clear();
        newsgroups.clear();
        peers.clear();
    }

    @Override
    public boolean hasArticle(Specification.MessageId messageId) {
        return articles.containsKey(messageId) || rejectedArticles.containsKey(messageId);
    }

    @Override
    public Article getArticle(Specification.MessageId messageId) {
        return articles.get(messageId);
    }

    @Override
    public Iterator<Specification.MessageId> getArticleIdsAfter(Date after) {
        return articles.values().stream()
                .filter(article -> article.insertionTime.after(after))
                .map(ArticleImpl::getMessageId)
                .collect(Collectors.toList())
                .iterator();
    }

    @Override
    public void rejectArticle(Specification.MessageId messageId) {
        rejectedArticles.put(messageId, new Date());
    }

    @Override
    public Boolean isRejectedArticle(Specification.MessageId messageId) {
        return rejectedArticles.containsKey(messageId);
    }

    @Override
    public Newsgroup addGroup(Specification.NewsgroupName name, String description,
                              Specification.PostingMode postingMode, Date createdAt,
                              String createdBy, boolean toBeIgnored) throws ExistingNewsgroupException {
        if (newsgroups.containsKey(name)) {
            throw new ExistingNewsgroupException("Newsgroup already exists: " + name);
        }
        NewsgroupImpl newsgroup = new NewsgroupImpl(name, description, postingMode, createdAt, createdBy, toBeIgnored);
        newsgroups.put(name, newsgroup);
        return newsgroup;
    }

    @Override
    public Iterator<Newsgroup> listAllGroups(boolean subscribedOnly, boolean includeIgnored) {
            return newsgroups.values().stream()
                    .filter(ng -> includeIgnored || !ng.isIgnored())
                    .map(ng -> (Newsgroup) ng)
                    .toList()
                    .iterator();
        }

    @Override
    public Iterator<Newsgroup> listAllGroupsAddedSince(Date insertedTime) {
        return newsgroups.values().stream()
                .filter(ng -> ng.getCreatedAt().after(insertedTime))
                .map(ng -> (Newsgroup) ng)
                .collect(Collectors.toList())
                .iterator();
    }

    @Override
    public Newsgroup getGroupByName(Specification.NewsgroupName name) {
        return newsgroups.get(name);
    }

    @Override
    public Peer addPeer(String label, String address) throws ExistingPeerException {
        for (PeerImpl peer : peers.values()) {
            if (peer.getLabel().equals(label) || peer.getAddress().equals(address)) {
                throw new ExistingPeerException("Peer already exists with label or address");
            }
        }
        int peerId = peerIdCounter.getAndIncrement();
        PeerImpl peer = new PeerImpl(peerId, label, address);
        peers.put(peerId, peer);
        return peer;
    }

    @Override
    public void removePeer(Peer peer) {
        peers.remove(peer.getPk());
        // Remove all feeds associated with this peer
        for (NewsgroupImpl newsgroup : newsgroups.values()) {
            newsgroup.removeFeedForPeer(peer);
        }
    }

    @Override
    public Iterator<Peer> getPeers() {
        return new ArrayList<Peer>(peers.values()).iterator();
    }

    // Inner class implementations

    private static class ArticleImpl implements Article {
        private final Specification.MessageId messageId;
        private final Specification.Article.ArticleHeaders headers;
        private final String bodyContent;
        private final Date insertionTime;

        ArticleImpl(Specification.MessageId messageId, Specification.Article.ArticleHeaders headers, Reader body) {
            this.messageId = messageId;
            this.headers = headers;
            this.bodyContent = readBody(body);
            this.insertionTime = new Date();
        }

        private String readBody(Reader body) {
            StringBuilder sb = new StringBuilder();
            try {
                char[] buffer = new char[1024];
                int read;
                while ((read = body.read(buffer)) != -1) {
                    sb.append(buffer, 0, read);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read body", e);
            }
            return sb.toString();
        }

        @Override
        public Specification.MessageId getMessageId() {
            return messageId;
        }

        @Override
        public Specification.Article.ArticleHeaders getAllHeaders() {
            return headers;
        }

        @Override
        public Reader getBody() {
            return new StringReader(bodyContent);
        }
    }

    private class NewsgroupImpl implements Newsgroup {
        private final Specification.NewsgroupName name;
        private final String description;
        private Specification.PostingMode postingMode;
        private final Date createdAt;
        private final String createdBy;
        private boolean ignored;
        private final Map<Specification.MessageId, NewsgroupArticleImpl> articles = new ConcurrentHashMap<>();
        private final Map<Specification.ArticleNumber, NewsgroupArticleImpl> articlesByNumber = new ConcurrentHashMap<>();
        private final Map<Integer, FeedImpl> feeds = new ConcurrentHashMap<>();
        private final AtomicInteger articleNumberCounter = new AtomicInteger(1);

        NewsgroupImpl(Specification.NewsgroupName name, String description,
                      Specification.PostingMode postingMode, Date createdAt,
                      String createdBy, boolean ignored) {
            this.name = name;
            this.description = description;
            this.postingMode = postingMode;
            this.createdAt = createdAt;
            this.createdBy = createdBy;
            this.ignored = ignored;
        }

        @Override
        public Specification.NewsgroupName getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public Specification.PostingMode getPostingMode() {
            return postingMode;
        }

        @Override
        public void setPostingMode(Specification.PostingMode postingMode) {
            this.postingMode = postingMode;
        }

        @Override
        public Date getCreatedAt() {
            return createdAt;
        }

        @Override
        public String getCreatedBy() {
            return createdBy;
        }

        @Override
        public NewsgroupMetrics getMetrics() {
            return new NewsgroupMetricsImpl(this);
        }

        @Override
        public boolean isIgnored() {
            return ignored;
        }

        @Override
        public void setIgnored(boolean isIgnored) {
            this.ignored = isIgnored;
        }

        @Override
        public Feed addFeed(Peer peer) throws ExistingFeedException {
            if (feeds.containsKey(peer.getPk())) {
                throw new ExistingFeedException("Feed already exists for peer: " + peer.getLabel());
            }
            FeedImpl feed = new FeedImpl(peer);
            feeds.put(peer.getPk(), feed);
            return feed;
        }

        @Override
        public Feed[] getFeeds() {
            return feeds.values().toArray(new Feed[0]);
        }

        void removeFeedForPeer(Peer peer) {
            feeds.remove(peer.getPk());
        }

        @Override
        public NewsgroupArticle addArticle(Specification.MessageId messageId,
                                           Specification.Article.ArticleHeaders headers,
                                           Reader body, boolean isRejected) throws ExistingArticleException {
            if (articles.containsKey(messageId)) {
                throw new ExistingArticleException("Article already exists in newsgroup: " + messageId);
            }

            // Create or get the article
            ArticleImpl article = MockPersistenceService.this.articles.get(messageId);
            if (article == null) {
                article = new ArticleImpl(messageId, headers, body);
                MockPersistenceService.this.articles.put(messageId, article);
            }

            if (isRejected) {
                MockPersistenceService.this.rejectArticle(messageId);
            }

            // Add to newsgroup
            Specification.ArticleNumber articleNumber;
            try {
                articleNumber = new Specification.ArticleNumber(articleNumberCounter.getAndIncrement());
            } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                throw new RuntimeException(e);
            }
            NewsgroupArticleImpl ngArticle = new NewsgroupArticleImpl(articleNumber, article, this);
            articles.put(messageId, ngArticle);
            articlesByNumber.put(articleNumber, ngArticle);

            return ngArticle;
        }

        @Override
        public Specification.ArticleNumber includeArticle(NewsgroupArticle article) throws ExistingArticleException {
            Specification.MessageId messageId = article.getMessageId();
            if (articles.containsKey(messageId)) {
                throw new ExistingArticleException("Article already exists in newsgroup: " + messageId);
            }

            Specification.ArticleNumber articleNumber;
            try {
                articleNumber = new Specification.ArticleNumber(articleNumberCounter.getAndIncrement());
            } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                throw new RuntimeException(e);
            }
            NewsgroupArticleImpl ngArticle = new NewsgroupArticleImpl(articleNumber, (ArticleImpl) article, this);
            articles.put(messageId, ngArticle);
            articlesByNumber.put(articleNumber, ngArticle);

            return articleNumber;
        }

        @Override
        public NewsgroupArticle getFirstArticle() {
            return articlesByNumber.values().stream()
                    .min(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .orElse(null);
        }

        @Override
        public Specification.ArticleNumber getArticle(Specification.MessageId messageId) {
            NewsgroupArticleImpl article = articles.get(messageId);
            return article != null ? article.getArticleNumber() : null;
        }

        @Override
        public NewsgroupArticle getArticleNumbered(Specification.ArticleNumber articleNumber) {
            return articlesByNumber.get(articleNumber);
        }

        @Override
        public Iterator<NewsgroupArticle> getArticlesNumbered(Specification.ArticleNumber lowerBound,
                                                               Specification.ArticleNumber upperBound) {
            return articlesByNumber.values().stream()
                    .filter(article -> {
                        long num = article.getArticleNumber().getValue();
                        return num >= lowerBound.getValue() && num <= upperBound.getValue();
                    })
                    .sorted(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .map(article -> (NewsgroupArticle) article)
                    .collect(Collectors.toList())
                    .iterator();
        }

        @Override
        public Iterator<NewsgroupArticle> getArticlesSince(Date insertionTime) {
            return articles.values().stream()
                    .filter(article -> article.article.insertionTime.after(insertionTime))
                    .sorted(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .map(article -> (NewsgroupArticle) article)
                    .collect(Collectors.toList())
                    .iterator();
        }



        @Override
        public NewsgroupArticle gotoNextArticle(Specification.ArticleNumber currentArticleNumber) {
            if (currentArticleNumber == null) {
                return null;
            }

            return articlesByNumber.values().stream()
                    .filter(article -> article.getArticleNumber().getValue() > currentArticleNumber.getValue())
                    .min(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .orElse(null);
        }

        @Override
        public NewsgroupArticle gotoPreviousArticle(Specification.ArticleNumber currentArticleNumber) {
            if (currentArticleNumber == null) {
                return null;
            }

            return articlesByNumber.values().stream()
                    .filter(article -> article.getArticleNumber().getValue() < currentArticleNumber.getValue())
                    .max(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .orElse(null);

        }
    }

    private static class NewsgroupArticleImpl extends ArticleImpl implements NewsgroupArticle {
        private final Specification.ArticleNumber articleNumber;
        private final ArticleImpl article;
        private final NewsgroupImpl newsgroup;

        NewsgroupArticleImpl(Specification.ArticleNumber articleNumber, ArticleImpl article, NewsgroupImpl newsgroup) {
            super(article.getMessageId(), article.headers, article.getBody());
            this.articleNumber = articleNumber;
            this.article = article;
            this.newsgroup = newsgroup;
        }

        @Override
        public Specification.ArticleNumber getArticleNumber() {
            return articleNumber;
        }

        @Override
        public Newsgroup getNewsgroup() {
            return newsgroup;
        }
    }

    private static class NewsgroupMetricsImpl implements NewsgroupMetrics {
        private final int numberOfArticles;
        private final Specification.ArticleNumber lowestArticleNumber;
        private final Specification.ArticleNumber highestArticleNumber;

        NewsgroupMetricsImpl(NewsgroupImpl newsgroup) {
            this.numberOfArticles = newsgroup.articlesByNumber.size();

            try {
                if (numberOfArticles == 0) {
                    this.lowestArticleNumber = new Specification.NoArticlesLowestNumber();
                    this.highestArticleNumber = new Specification.NoArticlesHighestNumber();
                } else {
                    this.lowestArticleNumber = newsgroup.articlesByNumber.keySet().stream()
                            .min(Comparator.comparing(Specification.ArticleNumber::getValue))
                            .orElse(new Specification.NoArticlesLowestNumber());
                    this.highestArticleNumber = newsgroup.articlesByNumber.keySet().stream()
                            .max(Comparator.comparing(Specification.ArticleNumber::getValue))
                            .orElse(new Specification.NoArticlesHighestNumber());
                }
            } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int getNumberOfArticles() {
            return numberOfArticles;
        }

        @Override
        public Specification.ArticleNumber getLowestArticleNumber() {
            return lowestArticleNumber;
        }

        @Override
        public Specification.ArticleNumber getHighestArticleNumber() {
            return highestArticleNumber;
        }
    }

    private static class FeedImpl implements Feed {
        private final Peer peer;
        private Date lastSyncTime;
        private Specification.ArticleNumber lastSyncArticleNum;

        FeedImpl(Peer peer) {
            this.peer = peer;
        }

        @Override
        public Specification.ArticleNumber getLastSyncArticleNum() {
            return lastSyncArticleNum;
        }

        @Override
        public Date getLastSyncTime() {
            return lastSyncTime;
        }

        @Override
        public void setLastSyncTime(Date time) {
            this.lastSyncTime = time;
        }

        @Override
        public void setLastSyncArticleNum(Specification.ArticleNumber num) {
            this.lastSyncArticleNum = num;
        }

        @Override
        public Peer GetPeer() {
            return peer;
        }
    }

    private static class PeerImpl implements Peer {
        private final int pk;
        private final String label;
        private final String address;
        private boolean disabled;
        private Date listLastFetched;

        PeerImpl(int pk, String label, String address) {
            this.pk = pk;
            this.label = label;
            this.address = address;
            this.disabled = false;
        }

        @Override
        public int getPk() {
            return pk;
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public String getLabel() {
            return label;
        }

        @Override
        public boolean getDisabledStatus() {
            return disabled;
        }

        @Override
        public void setDisabledStatus(boolean disabled) {
            this.disabled = disabled;
        }

        @Override
        public Date getListLastFetched() {
            return listLastFetched;
        }

        @Override
        public void setListLastFetched(Date lastFetched) {
            this.listLastFetched = lastFetched;
        }
    }
}

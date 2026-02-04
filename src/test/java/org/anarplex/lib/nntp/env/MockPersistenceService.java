package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * InMemory is an in-memory no-sql datastore implementation of PersistenceService.
 * All data is stored in memory using concurrent collections for thread safety.
 * Data is persisted to disk on close() and restored on init().
 */
public class MockPersistenceService implements PersistenceService {
    private static final Logger logger = LoggerFactory.getLogger(MockPersistenceService.class);

    static private final Map<Specification.MessageId, NewsgroupArticle> articles = new ConcurrentHashMap<>();
    static private final Map<Specification.MessageId, Date> rejectedArticles = new ConcurrentHashMap<>();
    static private final Map<Specification.NewsgroupName, NewsgroupImpl> newsgroups = new ConcurrentHashMap<>();
    static private final Map<String, PeerImpl> peers = new ConcurrentHashMap<>();
    static private final AtomicInteger peerIdCounter = new AtomicInteger(1);

    private static final String PERSISTENCE_DIR_ROOT = "dataSources/mock-persistence";
    private static String PERSISTENCE_DIR = PERSISTENCE_DIR_ROOT + "/nntp";
    private static final String ARTICLES_FILE = "articles.ser";
    private static final String REJECTED_ARTICLES_FILE = "rejectedArticles.ser";
    private static final String NEWSGROUPS_FILE = "newsgroups.ser";
    private static final String PEERS_FILE = "peers.ser";
    private static final String PEER_ID_COUNTER_FILE = "peerIdCounter.ser";

    private static Boolean isDataLoaded = false;

    static {
        if (System.getenv("nntp.port") != null) {
            PERSISTENCE_DIR =  PERSISTENCE_DIR_ROOT + "/" + System.getenv("nntp.port");
        }
    }

    @Override
    public void init() {
        loadFromDisk();
    }

    @Override
    public void checkpoint() {
        saveToDisk();
    }

    @Override
    public void rollback() {
        // For mock implementation, rollback clears everything and reloads from disk
        articles.clear();
        rejectedArticles.clear();
        newsgroups.clear();
        peers.clear();
        loadFromDisk();
    }

    @Override
    public void commit() {
        saveToDisk();
    }

    @Override
    public void close() {
        saveToDisk();
    }

    private void saveToDisk() {
        try {
            Path persistenceDir = Paths.get(PERSISTENCE_DIR);
            Files.createDirectories(persistenceDir);

            // Save articles
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(persistenceDir.resolve(ARTICLES_FILE).toFile()))) {
                oos.writeObject(new ConcurrentHashMap<>(articles));
            }

            // Save rejected articles
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(persistenceDir.resolve(REJECTED_ARTICLES_FILE).toFile()))) {
                oos.writeObject(new ConcurrentHashMap<>(rejectedArticles));
            }

            // Save newsgroups
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(persistenceDir.resolve(NEWSGROUPS_FILE).toFile()))) {
                oos.writeObject(new ConcurrentHashMap<>(newsgroups));
            }

            // Save peers
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(persistenceDir.resolve(PEERS_FILE).toFile()))) {
                oos.writeObject(new ConcurrentHashMap<>(peers));
            }

            // Save peer ID counter
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(persistenceDir.resolve(PEER_ID_COUNTER_FILE).toFile()))) {
                oos.writeInt(peerIdCounter.get());
            }
        } catch (IOException e) {
            logger.error("Warning: Failed to persist MockPersistenceService data: {}", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void loadFromDisk() {
        if (isDataLoaded) {
            return;
        }

        Path persistenceDir = Paths.get(PERSISTENCE_DIR);
        if (!Files.exists(persistenceDir)) {
            return; // No persisted data to load
        }

        try {
            // Load articles
            Path articlesPath = persistenceDir.resolve(ARTICLES_FILE);
            if (Files.exists(articlesPath)) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(articlesPath.toFile()))) {
                    Map<Specification.MessageId, NewsgroupArticle> loadedArticles =
                        (Map<Specification.MessageId, NewsgroupArticle>) ois.readObject();
                    articles.putAll(loadedArticles);
                }
            }

            // Load rejected articles
            Path rejectedPath = persistenceDir.resolve(REJECTED_ARTICLES_FILE);
            if (Files.exists(rejectedPath)) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(rejectedPath.toFile()))) {
                    Map<Specification.MessageId, Date> loadedRejected =
                        (Map<Specification.MessageId, Date>) ois.readObject();
                    rejectedArticles.putAll(loadedRejected);
                }
            }

            // Load newsgroups
            Path newsgroupsPath = persistenceDir.resolve(NEWSGROUPS_FILE);
            if (Files.exists(newsgroupsPath)) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(newsgroupsPath.toFile()))) {
                    Map<Specification.NewsgroupName, NewsgroupImpl> loadedNewsgroups =
                        (Map<Specification.NewsgroupName, NewsgroupImpl>) ois.readObject();
                    newsgroups.putAll(loadedNewsgroups);
                }
            }

            // Load peers
            Path peersPath = persistenceDir.resolve(PEERS_FILE);
            if (Files.exists(peersPath)) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(peersPath.toFile()))) {
                    Map<String, PeerImpl> loadedPeers =
                        (Map<String, PeerImpl>) ois.readObject();
                    peers.putAll(loadedPeers);
                }
            }

            // Load peer ID counter
            Path counterPath = persistenceDir.resolve(PEER_ID_COUNTER_FILE);
            if (Files.exists(counterPath)) {
                try (ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(counterPath.toFile()))) {
                    peerIdCounter.set(ois.readInt());
                }
            }
            isDataLoaded = true;
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Warning: Failed to load MockPersistenceService data: {}", e.getMessage());
        }
    }

    @Override
    public boolean hasArticle(Specification.MessageId messageId) {
        return articles.containsKey(messageId) || rejectedArticles.containsKey(messageId);
    }

    @Override
    public NewsgroupArticle getArticle(Specification.MessageId messageId) {
        return articles.get(messageId);
    }

    @Override
    public Iterator<Specification.MessageId> getArticleIdsAfter(LocalDateTime after) {
        return articles.values().stream()
                .filter(article -> article.getInsertionTime().isAfter(after))
                .map(NewsgroupArticle::getMessageId)
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
                              Specification.PostingMode postingMode, LocalDateTime createdAt,
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
    public Iterator<Newsgroup> listAllGroupsAddedSince(LocalDateTime insertedTime) {
        return newsgroups.values().stream()
                .filter(ng -> ng.getCreatedAt().isAfter(insertedTime))
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
            if (peer.getAddress().equals(address)) {
                throw new ExistingPeerException("Peer already exists with that address");
            }
        }
        PeerImpl peer = new PeerImpl(peerIdCounter.getAndIncrement(), label, address);
        peers.put(peer.address, peer);
        return peer;
    }

    @Override
    public void removePeer(Peer peer) {
        peers.remove(peer.getAddress());
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

    private static class NewsgroupImpl implements Newsgroup, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private final Specification.NewsgroupName name;
        private final String description;
        private Specification.PostingMode postingMode;
        private final LocalDateTime createdAt;
        private final String createdBy;
        private boolean ignored;

        // storage
        private final Map<Specification.MessageId, NewsgroupArticle> articles = new ConcurrentHashMap<>();
        private final Map<Specification.ArticleNumber, NewsgroupArticle> articlesByNumber = new ConcurrentHashMap<>();
        private final Map<String, FeedImpl> feeds = new ConcurrentHashMap<>();
        private final AtomicInteger articleNumberCounter = new AtomicInteger(1);

        NewsgroupImpl(Specification.NewsgroupName name, String description,
                      Specification.PostingMode postingMode, LocalDateTime createdAt,
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
        public LocalDateTime getCreatedAt() {
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
            if (feeds.containsKey(peer.getAddress())) {
                throw new ExistingFeedException("Feed already exists for peer: " + peer.getLabel());
            }
            FeedImpl feed = new FeedImpl(this, peer);
            feeds.put(peer.getAddress(), feed);
            return feed;
        }

        @Override
        public Feed[] getFeeds() {
            return feeds.values().toArray(new Feed[0]);
        }

        void removeFeedForPeer(Peer peer) {
            feeds.remove(peer.getAddress());
        }

        @Override
        public NewsgroupArticle addArticle(Specification.MessageId messageId,
                                           Specification.Article.ArticleHeaders headers,
                                           String body, boolean isRejected) throws ExistingArticleException {
            if (articles.containsKey(messageId)) {
                throw new ExistingArticleException("Article already exists in newsgroup: " + messageId);
            }

            // In this mock, we create a new article entry.
            // We wrap it in our NewsgroupArticle implementation.
            Specification.ArticleNumber articleNumber;
            try {
                articleNumber = new Specification.ArticleNumber(articleNumberCounter.getAndIncrement());
            } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                throw new RuntimeException("Failed to generate article number", e);
            }

            // Create the underlying article representation for the mock
            NewsgroupArticle ngArticle = new NewsgroupArticle(articleNumber, messageId, headers, body, this);

            // Add to newsgroup-specific collections
            articles.put(messageId, ngArticle);
            articlesByNumber.put(articleNumber, ngArticle);

            // Register globally in the PersistenceService
            MockPersistenceService.articles.put(messageId, ngArticle);

            if (isRejected) {
                MockPersistenceService.rejectedArticles.put(messageId, new Date());
            }

            return ngArticle;
        }


        @Override
        public Specification.ArticleNumber includeArticle(PersistenceService.NewsgroupArticle article) throws ExistingArticleException {
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
            NewsgroupArticle ngArticle = new NewsgroupArticle(articleNumber, (NewsgroupArticle) article, this);
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
            NewsgroupArticle article = articles.get(messageId);
            return article != null ? article.getArticleNumber() : null;
        }

        @Override
        public NewsgroupArticle getArticleNumbered(Specification.ArticleNumber articleNumber) {
            return articlesByNumber.get(articleNumber);
        }

        @Override
        public Iterator<PersistenceService.NewsgroupArticle> getArticlesNumbered(Specification.ArticleNumber lowerBound, Specification.ArticleNumber upperBound) {
            long low = lowerBound.getValue();
            long high = upperBound.getValue();

            return articlesByNumber.values().stream()
                    .filter(article -> {
                        long num = article.getArticleNumber().getValue();
                        return num >= low && num <= high;
                    })
                    .sorted(Comparator.comparingLong(a -> a.getArticleNumber().getValue()))
                    .map(a -> (PersistenceService.NewsgroupArticle) a)
                    .toList()
                    .iterator();
        }

        @Override
        public Iterator<PersistenceService.NewsgroupArticle> getArticlesSince(LocalDateTime insertionTime) {
            return articles.values().stream()
                    .filter(article -> article.getInsertionTime().isAfter(insertionTime))
                    .sorted(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .map(a -> (PersistenceService.NewsgroupArticle) a)
                    .collect(Collectors.toList())
                    .iterator();
        }

        @Override
        public NewsgroupArticle getNextArticle(Specification.ArticleNumber currentArticleNumber) {
            if (currentArticleNumber == null) {
                return null;
            }

            return articlesByNumber.values().stream()
                    .filter(article -> article.getArticleNumber().getValue() > currentArticleNumber.getValue())
                    .min(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .orElse(null);
        }

        @Override
        public NewsgroupArticle getPreviousArticle(Specification.ArticleNumber currentArticleNumber) {
            if (currentArticleNumber == null) {
                return null;
            }

            return articlesByNumber.values().stream()
                    .filter(article -> article.getArticleNumber().getValue() < currentArticleNumber.getValue())
                    .max(Comparator.comparing(a -> a.getArticleNumber().getValue()))
                    .orElse(null);

        }
    }

    public static class NewsgroupArticle extends PersistenceService.NewsgroupArticle implements Serializable {
        private final Specification.ArticleNumber articleNumber;
        private final NewsgroupImpl newsgroup;
        private final LocalDateTime insertionTime;

        NewsgroupArticle(Specification.ArticleNumber articleNumber, Specification.MessageId messageId,
                         Specification.Article.ArticleHeaders headers, String body, NewsgroupImpl newsgroup) {
            super(messageId, headers, body);
            this.articleNumber = articleNumber;
            this.newsgroup = newsgroup;
            this.insertionTime = LocalDateTime.now();
        }

        // Constructor for including an existing article
        NewsgroupArticle(Specification.ArticleNumber articleNumber, NewsgroupArticle article, NewsgroupImpl newsgroup) {
            super(article.getMessageId(), article.getAllHeaders(), article.getBody());
            this.articleNumber = articleNumber;
            this.newsgroup = newsgroup;
            this.insertionTime = article.getInsertionTime();
        }

        @Override
        public Specification.ArticleNumber getArticleNumber() {
            return articleNumber;
        }

        @Override
        public Newsgroup getNewsgroup() {
            return newsgroup;
        }

        @Override
        public LocalDateTime getInsertionTime() {
            return insertionTime;
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

    private static class FeedImpl implements Feed, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private final Peer peer;
        private final Newsgroup newsgroup;
        private LocalDateTime lastSyncTime;
        private Specification.ArticleNumber lastSyncArticleNum;

        FeedImpl(Newsgroup newsgroup, Peer peer) {
            this.newsgroup = newsgroup;
            this.peer = peer;
        }

        @Override
        public Specification.ArticleNumber getLastSyncArticleNum() {
            return lastSyncArticleNum;
        }

        @Override
        public LocalDateTime getLastSyncTime() {
            return lastSyncTime;
        }

        @Override
        public void setLastSyncTime(LocalDateTime time) {
            this.lastSyncTime = time;
        }

        @Override
        public void setLastSyncArticleNum(Specification.ArticleNumber num) {
            this.lastSyncArticleNum = num;
        }

        @Override
        public Newsgroup getNewsgroup() {
            return newsgroup;
        }

        @Override
        public Peer getPeer() {
            return peer;
        }
    }

    public static class PeerImpl implements Peer, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private final int id;
        private String label;
        private final String address;
        private boolean disabled;
        private LocalDateTime listLastFetched;

        PeerImpl(int id, String label, String address) {
            this.id = id;
            this.label = label;
            this.address = address;
            this.disabled = false;
        }

        @Override
        public int getID() {
            return id;
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
        public void setLabel(String label) { this.label = label; }

        @Override
        public boolean getDisabledStatus() {
            return disabled;
        }

        @Override
        public void setDisabledStatus(boolean disabled) {
            this.disabled = disabled;
        }

        @Override
        public LocalDateTime getListLastFetched() {
            return listLastFetched;
        }

        @Override
        public void setListLastFetched(LocalDateTime lastFetched) {
            this.listLastFetched = lastFetched;
        }

        @Override
        public String getPrincipal() {
            return getAddress();
        }
    }
}

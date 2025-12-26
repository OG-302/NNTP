package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;

import java.io.Reader;
import java.time.LocalDateTime;
import java.util.Iterator;

/**
 * The EntityModel interface provides a simple API for storing and retrieving articles and newsgroups.  It is
 * designed to be abstract enough to support multiple storage backends and not just relational databases.
 * Each EntityModel **instance** will be used by one NNTP client and will be invoked by the same thread during
 * its lifetime.
 * Init() will be called at the start of the lifecycle and close() at the end.
 */
public interface PersistenceService extends AutoCloseable {

    // explicitly readies the PersistentService for use
    void init();

    // commits any outstanding updates, and starts a new transaction which remains open until either rollback() or commit() is called
    void checkpoint();

    // aborts all updates since to the checkpoint() invocation
    void rollback();

    // makes all updates since checkpoint() permanent
    void commit();

    // indicates no further use of this service's instance.  Invoking close() with uncommitted updates will abort those updates
    void close();

    /**
     * hasArticle determines whether the specified message id exists in the database.  Rejected articles are not
     * included.  To determine if an article exists in the store but has been marked as rejected, use isRejectedArticle().
     *
     * @return true if the message id exists, false otherwise
     */
    boolean hasArticle(Specification.MessageId messageId);

    /**
     * getArticle returns the article with the specified message id or null if it does not exist.
     * Rejected articles are never returned.
     * As every article always belongs to a Newsgroup, this also returns a Newsgroup the Article appears in.
     *
     * @return the article with the specified message id or null if it does not exist
     */
    NewsgroupArticle getArticle(Specification.MessageId messageId);

    /**
     * getArticleIdsAfter returns an iterator over all message ids that were added after the specified time.
     *
     * @return an iterator over all message ids that were added after the specified time
     */
    Iterator<Specification.MessageId> getArticleIdsAfter(LocalDateTime after);

    /**
     * rejectArticle marks the specified message id as rejected.  A rejected article will not be returned
     * to readers.
     */
    void rejectArticle(Specification.MessageId messageId);

    /**
     * isRejectedArticle returns true if the specified message id has been marked as rejected.
     *
     * @return true if the specified message id has been marked as rejected, false otherwise
     */
    Boolean isRejectedArticle(Specification.MessageId messageId);

    /**
     * addGroup adds a new newsgroup to the database.
     *
     * @return the newly created Newsgroup or null if the newsgroup already exists
     */
    Newsgroup addGroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, LocalDateTime createdAt, String createdBy, boolean toBeIgnored)
        throws ExistingNewsgroupException;

    Iterator<Newsgroup> listAllGroups(boolean subscribedOnly, boolean includeIgnored);

    Iterator<Newsgroup> listAllGroupsAddedSince(LocalDateTime insertedTime);

    /**
     * getGroupByName returns the Newsgroup with the specified name or nil if no such Newsgroup exists.
     * Ignored newsgroups will be included in the results.
     */
    Newsgroup getGroupByName(Specification.NewsgroupName name);

    /**
     * addPeer adds a new Peer to the database.
     * Throws ExistingPeerException if a Peer with the same address already exists.
     * @return the newly created Peer
     */
    Peer addPeer(String label, String address)
        throws ExistingPeerException;

    /**
     * RemovePeer removes the specified Peer from the database.  As a consequence, it also REMOVES all Feeds
     * associated with this Peer and the associated state (such as the last sync time for each newsgroup feed, etc.).
     * Consider using Peer.SetDisabled(true) if you simply do not want the Peer to be interrogated during syncs.
     */
    void removePeer(Peer peer);

    /**
     * getPeers returns an iterator over all Peers in the database.
     *
     * @return an iterator over all Peers in the database.
     */
    Iterator<Peer> getPeers();

    class ExistingNewsgroupException extends Exception {
        public ExistingNewsgroupException(String message) {
            super(message);
        }
    }

    class ExistingPeerException extends Exception {
        public ExistingPeerException(String message) {
            super(message);
        }
    }


//    interface Article {
//        /**
//         * GetMessageId returns the MesageId that the Article was POSTed or submitted with via IHAVE.  There is also
//         * a standard header (messageId) which may be different (See RFC-3977)
//         */
//        Specification.MessageId getMessageId();
//        Specification.Article.ArticleHeaders getAllHeaders();
//        StringReader getBody();
//    }

    abstract class Article extends Specification.Article {

        protected Article(Specification.MessageId messageId, ArticleHeaders headers, String body) {
            super(messageId, headers, body);
        }

        public abstract LocalDateTime getInsertionTime();
    }


    /**
     * Newsgroup is a structure that contains a list of Articles and their ArticleNumbers in a particular Newsgroup.
     * This object maintains the state of the Newsgroup cursor (Current Article Number).  The cursor is initially set
     * to Invalid.
     */
    interface Newsgroup {
        /**
         * GetName returns the immutable and normalized Newsgroup's name
         */
        Specification.NewsgroupName getName();
        String getDescription();
        Specification.PostingMode getPostingMode();
        void setPostingMode(Specification.PostingMode postingMode);
        LocalDateTime  getCreatedAt();
        String getCreatedBy();

        /**
         * GetMetrics returns the actual number of articles available in this newsgroup (count), as well as the
         * lowest and highest article numbers.  These values are calculated on demand, on each call.
         */
        NewsgroupMetrics getMetrics();

        boolean isIgnored();
        void setIgnored(boolean isIgnored);

        Feed addFeed(Peer peer) throws ExistingFeedException;
        Feed[] getFeeds();

        class ExistingFeedException extends Exception {
            public ExistingFeedException(String message) {
                super(message);
            }
        }

        /**
         * AddArticle creates an article with the supplied parameters AND includes it in this newsgroup.
         * @throws ExistingArticleException if an article with the same message-id already exists in this Newsgroup.
         */
        NewsgroupArticle addArticle(Specification.MessageId messageId, Specification.Article.ArticleHeaders headers, String body, boolean isRejected)
            throws ExistingArticleException;

        /**
         * IncludeArticle adds the existing Article (already present in at least one other newsgroup) to this Newsgroup.
         */
        Specification.ArticleNumber includeArticle(NewsgroupArticle article)
                throws ExistingArticleException;

        class ExistingArticleException extends Exception {
            public ExistingArticleException(String message) {
                super(message);
            }
        }

        /**
         * GetArticle returns the Article with the specified message id or nil if no such Article with this
         * message-id exists in this Newsgroup.
         */
        Specification.ArticleNumber getArticle(Specification.MessageId messageId);

        /**
         * GetArticleNumbered returns the Article with the specified articleNumber or nil if no such ArticleNumber exists.
         * This function does NOT change the CURRENT article number.
         * This method DOES NOT change the current article number (cursor).
         * @return the NewsgroupArticle with the specified ArticleNumber or null if no such ArticleNumber exists
         */
        NewsgroupArticle getArticleNumbered(Specification.ArticleNumber articleNumber);

        /**
         * GetArticlesNumbered returns a list of Articles (in order) of their ArticleNumbers within the specified bounds
         * (inclusive).
         */
        Iterator<NewsgroupArticle> getArticlesNumbered(Specification.ArticleNumber lowerBound, Specification.ArticleNumber upperBound);

        /**
         * GetArticlesSince returns a list of MessageIDs of Articles that were added or included to this Newsgroup since
         * the specified Time.
         */
        Iterator<NewsgroupArticle> getArticlesSince(LocalDateTime insertionTime);

        /**
         * Returns the first Article in this Newsgroup or null if there are no Articles.
         */
        NewsgroupArticle getFirstArticle();

        /**
         * Returns the article in the newsgroup adjacent to the supplied current article with the next higher article number, or null if no such article exists.
         */
        NewsgroupArticle getNextArticle(Specification.ArticleNumber currentArticle);

        /**
         * Returns the article in the newsgroup adjacent to the supplied current article with the next lower article number, or null if no such article exists.
         */
        NewsgroupArticle getPreviousArticle(Specification.ArticleNumber currentArticle);
    }

    /**
     * NewsgroupArticle is an Article located in a particular Newsgroup.  Hence, the article always has a corresponding
     * article number within that newsgroup.  The same article can appear in multiple newsgroups, and if so, it will
     * likely have different article numbers.
     */
    abstract class NewsgroupArticle extends Article {
        public NewsgroupArticle(Specification.MessageId messageId, ArticleHeaders headers, String body) {
            super(messageId, headers, body);
        }

        public abstract Specification.ArticleNumber getArticleNumber();
        public abstract Newsgroup getNewsgroup();
    }

    /**
     * A snapshot of the metrics of a Newsgroup.
     * If there are no articles in the newsgroup, then the number of articles = 0, and the lowest article number =
     * Spec.NoArticlesLowestNumber, and the highest article number = Spec.NoArticlesHighestNumber.
     */
    interface NewsgroupMetrics {
        int getNumberOfArticles();
        Specification.ArticleNumber getLowestArticleNumber();
        Specification.ArticleNumber getHighestArticleNumber();
    }

    /**
     * A Feed is a source of new articles for a Newsgroup, which comes from a Peer.  Note, a newsgroup may have multiple
     * feeds.  These will be provided by different Peers.
     * A feed maintains the state of the last sync, which includes the sync time and greatest article number synced.
     */
    interface Feed {
        // GetLastSyncArticleNum returns the value set by UpdateLastSyncArticleNum() or nil if none, err if error.
        Specification.ArticleNumber getLastSyncArticleNum();
        LocalDateTime getLastSyncTime();

        void setLastSyncTime(LocalDateTime time);
        void setLastSyncArticleNum(Specification.ArticleNumber num);

        Peer getPeer();
        Newsgroup getNewsgroup();
    }

    /**
     * A Peer is another NNTP Service that responds to NNTP Commands.   It is uniquely identified by its address,
     * which is also immutable.
     */
    interface Peer extends IdentityService.Subject {
        /**
         * GetAddress returns the immutable address of this Peer.  The address is also the unique identifier for this Peer.
         *
         * @return the immutable address of this Peer
         */
        String getAddress();

        /**
         * GetLabel returns a free-form field (the label) that has no particular meaning to the NNTP Protocol Engine.
         * Its use is purely for human consumption - e.g. a human-readable name for the Peer.
         * @return the immutable label of this Peer
         */
        String getLabel();
        void setLabel(String label);

        boolean getDisabledStatus();
        void setDisabledStatus(boolean disabled);

        LocalDateTime getListLastFetched();
        void setListLastFetched(LocalDateTime lastFetched);
    }
}

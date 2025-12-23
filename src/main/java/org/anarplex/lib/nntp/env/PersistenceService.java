package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;

import java.io.Reader;
import java.util.Date;
import java.util.Iterator;

/**
 * The PersistentService interface provides a simple API for storing and retrieving articles and newsgroups.  It is
 * designed to be abstract enough to support multiple storage backends and not just relational databases.
 * Each PersistentService **instance** will be used by one NNTP client and will be invoked by the same thread during
 * its lifetime.
 * Init() will be called at the start of the lifecycle and close() at the end.
 */
public interface PersistenceService extends AutoCloseable {

    // explicitly readies the PersistentService for use
    void init();

    // make outstanding updates permanent
    void commit();

    // indicates no further use of this service's instance.  Invoking close() with uncommitted updates will abort those updates
    void close();

    /**
     * hasArticle determines whether the specified message id exists in the database.  Rejected articles are not
     * included.  To determine if an article exists in the store but has been marked as rejected, use isRejectedArticle().
     *
     * @param messageId
     * @return true if the message id exists, false otherwise
     */
    boolean hasArticle(Specification.MessageId messageId);

    /**
     * getArticle returns the article with the specified message id or null if it does not exist.
     * Rejected articles are never returned.
     *
     * @param messageId
     * @return the article with the specified message id or null if it does not exist
     */
    Article getArticle(Specification.MessageId messageId);

    /**
     * getArticleIdsAfter returns an iterator over all message ids that were added after the specified time.
     *
     * @param after
     * @return an iterator over all message ids that were added after the specified time
     */
    Iterator<Specification.MessageId> getArticleIdsAfter(Date after);

    /**
     * rejectArticle marks the specified message id as rejected.  A rejected article will not be returned
     * to readers.
     *
     * @param messageId
     */
    void rejectArticle(Specification.MessageId messageId);

    /**
     * isRejectedArticle returns true if the specified message id has been marked as rejected.
     *
     * @param messageId
     * @return true if the specified message id has been marked as rejected, false otherwise
     */
    Boolean isRejectedArticle(Specification.MessageId messageId);

    /**
     * addGroup adds a new newsgroup to the database.
     *
     * @param name
     * @param description
     * @param postingMode
     * @param createdAt
     * @param createdBy
     * @param toBeIgnored
     * @return the newly created Newsgroup or null if the newsgroup already exists
     */
    Newsgroup addGroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, Date createdAt, String createdBy, boolean toBeIgnored)
        throws ExistingNewsgroupException;

    Iterator<Newsgroup> listAllGroups(boolean subscribedOnly, boolean includeIgnored);

    Iterator<Newsgroup> listAllGroupsAddedSince(Date insertedTime);

    /**
     * getGroupByName returns the Newsgroup with the specified name or nil if no such Newsgroup exists.
     * Ignored newsgroups will be included in the results.
     * @param name
     * @return
     */
    Newsgroup getGroupByName(Specification.NewsgroupName name);

    /**
     * addPeer adds a new Peer to the database.
     *
     * @param label
     * @param address
     * @return the newly created Peer
     */
    Peer addPeer(String label, String address)
        throws ExistingPeerException;

    /**
     * RemovePeer removes the specified Peer from the database.  As a consequence, its also REMOVES all Feeds
     * associated with this Peer and the associated state (such as the last sync time for each newsgroup feed, etc.).
     * Consider using Peer.SetDisabled(true) if you simply do not want the Peer to be interrogated during syncs.
     *
     * @param peer
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

    interface Article {
        /**
         * GetMessageId returns the MesageId that the Article was POSTed or submitted with via IHAVE.  There is also
         * a standard header (messageId) which may be different (See RFC-3977)
         * @return
         */
        Specification.MessageId getMessageId();
        Specification.Article.ArticleHeaders getAllHeaders();
        Reader getBody();
    }

    /**
     * Newsgroup is a structure that contains a list of Articles and their ArticleNumbers in a particular Newsgroup.
     * This object maintains the state of the Newsgroup cursor (Current Article Number).  The cursor is initially set
     * to Invalid.     */
    interface Newsgroup {
        /**
         * GetName returns the immutable and normalised Newsgroup's name
         *
         * @return
         */
        Specification.NewsgroupName getName();
        String getDescription();
        Specification.PostingMode getPostingMode();
        void setPostingMode(Specification.PostingMode postingMode);
        Date  getCreatedAt();
        String getCreatedBy();

        /**
         * GetMetrics returns the actual number of articles available in this newsgroup (count), as well as the
         * lowest and highest article numbers.  These values are calculated on demand, on each call.
         * @return
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
         *
         * @param messageId
         * @param headers
         * @param body
         * @param isRejected
         * @return
         * @throws ExistingArticleException if an article with the same message-id already exists in this Newsgroup
         */
        NewsgroupArticle addArticle(Specification.MessageId messageId, Specification.Article.ArticleHeaders headers, Reader body, boolean isRejected)
            throws ExistingArticleException;

        class ExistingArticleException extends Exception {
            public ExistingArticleException(String message) {
                super(message);
            }
        }

        /**
         * IncludeArticle adds the existing Article (already present in at least one newsgroup) to this Newsgroup.
         * @param article
         * @return
         */
        Specification.ArticleNumber includeArticle(NewsgroupArticle article)
            throws ExistingArticleException;

        /**
         * GetArticle returns the Article with the specified message id or nil if no such Article with this
         * message-id exists in this Newsgroup.
         * @param messageId
         * @return
         */
        Specification.ArticleNumber getArticle(Specification.MessageId messageId);

        /**
         * GetArticleNumbered returns the Article with the specified articleNumber or nil if no such ArticleNumber exists.
         * This function does NOT change the CURRENT article number.
         * This method DOES NOT change the current article number (cursor).
         * @param articleNumber
         * @return the NewsgroupArticle with the specified ArticleNumber or null if no such ArticleNumber exists
         */
        NewsgroupArticle getArticleNumbered(Specification.ArticleNumber articleNumber);

        /**
         * GetArticlesNumbered returns a list of Articles (in order) of their ArticleNumbers within the specified bounds
         * (inclusive).
         *
         * @param lowerBound
         * @param upperBound
         * @return
         */
        Iterator<NewsgroupArticle> getArticlesNumbered(Specification.ArticleNumber lowerBound, Specification.ArticleNumber upperBound);

        /**
         * GetArticlesSince returns a list of MessageIDs of Articles added to this Newsgroup since the specified
         * Time (UTC).
         *
         * @param insertionTime
         * @return
         */
        Iterator<NewsgroupArticle> getArticlesSince(Date insertionTime);

        /**
         * Identifies the first Article in this Newsgroup or nil if there are no Articles.
         * @return the first Article in this Newsgroup or nil if there are no Articles.
         */
        NewsgroupArticle getFirstArticle();

        /*
         * The following three methods are NON-Idempotent functions!
         * These methods change the state of this Newgroup cursor - specifically, its Current Article Number.
         */

        /**
         * GotoNextArticle changes the CURRENT Article to be the Article with the next HIGHER valid article number in
         * the Newsgroup sequence than Current.
         * If the CURRENT Article number is Invalid, then this function does not change the CURRENT Article position and
         * returns nil.
         * If there are no more Articles with greater Article Numbers in this Newsgroup, then the CURRENT Article Number
         * maybe null after this operation if there is no Higher.
         *
         * @return the next article to the Current Article, if exists or nil if none
         */
        NewsgroupArticle gotoNextArticle(Specification.ArticleNumber currentArticle);

        /**
         * GotoPreviousArticle changes the CURRENT Article to be the Article with the next LOWER valid article number in
         * the Newsgroup sequence than Current.  Current Article Number maybe null after this operation if there is no Lower.
         *
         * @return the previous article to the Current Article, if exists or nil if none
         */
        NewsgroupArticle gotoPreviousArticle(Specification.ArticleNumber currentArticle);

    }

    /**
     * NewsgroupArticle is an Article located in a particular Newsgroup
     */
    interface NewsgroupArticle extends Article {
        Specification.ArticleNumber getArticleNumber();
        Newsgroup getNewsgroup();
    }

    /**
     * A snapshot of the metrics of a Newsgroup.
     * If there are no articles in the newsgroup then
     * - number of articles = 0,
     * - lowest article number = Spec.NoArticlesLowestNumber
     * - highest article number = Spec.NoArticlesHighestNumber
     */
    interface NewsgroupMetrics {
        int getNumberOfArticles();
        Specification.ArticleNumber getLowestArticleNumber();
        Specification.ArticleNumber getHighestArticleNumber();
    }


    interface Feed {
        // GetLastSyncArticleNum returns the value set by UpdateLastSyncArticleNum() or nil if none, err if error.
        Specification.ArticleNumber getLastSyncArticleNum();
        Date getLastSyncTime();

        void setLastSyncTime(Date time);
        void setLastSyncArticleNum(Specification.ArticleNumber num);

        Peer GetPeer();
    }

    interface Peer {
        int getPk();

        /**
         * GetAddress returns the immutable address of this Peer.  To change the address requires creating a new Peer
         *
         * @return the immutable address of this Peer
         */
        String getAddress();

        /**
         * GetLabel returns the immutable label of this Peer.  To change the label requires creating a new Peer
         *
         * @return the immutable label of this Peer
         */
        String getLabel();

        // int getConnectionPriority();
        // setConnectionPriority(int priority int);

        boolean getDisabledStatus();
        void setDisabledStatus(boolean disabled);

        Date getListLastFetched();
        void setListLastFetched(Date lastFetched);
    }

}

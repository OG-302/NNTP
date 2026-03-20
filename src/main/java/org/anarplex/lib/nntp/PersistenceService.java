package org.anarplex.lib.nntp;

import java.time.Instant;
import java.util.Iterator;
import java.util.Set;

/**
 * The PersistenceService interface is a domain-specific Entity Model designed and optimized for use by the ProtocolEngine
 * and NewsgroupSynchronizer classes of this library.  The interface provides a compact yet sufficient, implementation agnostic
 * API for storing and retrieving articles and newsgroups.  It is designed to be abstract enough to allow for various
 * storage backends (implementations) - not just relational databases, but non-relational ones as well.
 * Each EntityModel **instance** does not need to be thread-safe as it will be invoked by the same thread during its lifetime.
 */

// Creator with factory method
public abstract class PersistenceService {

    // Protected to allow test and implementation subclasses to extend this service.
    protected PersistenceService() {
    }

    /**
     * Indicates that a PersistentService instance should acquire any resources, etc. it needs for regular operation.
     * And now is a good time for garbage collection on the database.
     */
    protected abstract void init();

    /**
     * The save point where subsequent rollbacks will revert to.  This is useful for ensuring that all updates made
     * since this checkpoint can be rolled back safely.
     */
    protected abstract void checkpoint();

    /**
     * Indicates that all updates made by this thread since the last commit() or checkpoint() should be discarded.
     */
    protected abstract void rollback();

    /**
     * Indicates that all updates made by this thread since the last commit(), checkpoint() or rollback() should be made permanent.
     */
    protected abstract void commit();

    /**
     * Indicates that no further use of this service's instance will be made.
     * If close() is invoked with uncommitted updates, then these updates should be rolled back.
     */
    protected abstract void close();

    /**
     * This method inserts the specified article, along with its submitter and articleSource in the database if and only
     * if it is not an already a known article (see isKnownArticle() above).  If the article is already known
     * (see isKnown() method), then returns null.
     */
    protected abstract PendingArticle addArticle(Specification.Article article,
                                                 Specification.ArticleSource articleSource,
                                                 IdentityService.Subject submitter);

    /**
     * Deletes this article from the database, deletes all its associations with referenced newsgroups, and deletes
     * any article numbers (in those newsgroups) assigned to it if any.
     */
    protected abstract void deleteArticle(Specification.MessageId messageId);

    protected abstract StoredArticle getArticle(Specification.MessageId messageId);

    protected abstract boolean hasArticle(Specification.MessageId messageId);

    /**
     * This method returns a list of all stored articles (suitable for the review process) ordered by insertion time
     * (ascending).
     */
    public abstract Iterator<StoredArticle> listArticles();

    /**
     * This method adds a new newsgroup to the database and returns that newsgroup, or null if the
     * newsgroup already exists (in any form, including a banned newsgroup).
     * The name parameter is immutable and must not be null.  All other
     * parameters are optional and may be null.
     */
    protected abstract StoredNewsgroup addNewsgroup(Specification.NewsgroupName name,
                           String description,
                           Specification.PostingMode postingMode,
                           String createdBy);
    /**
     * Deletes this newsgroup from the database, deletes all associations it has with any articles including any
     * ArticleNumbers assigned to them, and deletes all Feeds associated with this Newsgroup.  Does NOT delete any Peers
     * mentioned in the Feeds, and does NOT delete any articles associated with the newsgroup as they may appear in
     * other newsgroups.
     */
    protected abstract void deleteNewsgroup(StoredNewsgroup newsgroup);

    /**
     * Returns the newsgroup with the supplied name or null if no such newsgroup exists which includes banned newsgroups.
     */
    public abstract StoredNewsgroup getNewsgroup(Specification.NewsgroupName name);

    /**
     * Returns the list of Published Newsgroups ordered by their insertion time field in decreasing values (i.e., newest
     * appears first in the list).  A published newsgroup is one that has at least one ArticleNumber assigned to a
     * (Published) Article in it.
     */
    public abstract Iterator<PublishedNewsgroup> listPublishedGroups();

    /**
     * Returns a list of all Newsgroups ordered by their insertion time field in decreasing values (i.e.,
     * newest appears first in the list).
     */
    public abstract Iterator<StoredNewsgroup> listAllGroups();

    /**
     * The same result as listGroups() except only those newsgroups that were added since the supplied time are returned.
     * Result is never null although it may be empty.
     */
    public abstract Iterator<PublishedNewsgroup> listGroupsAddedSince(Instant since);

    /**
     * Returns the newsgroup with the supplied name or null if no such newsgroup exists.
     */
    public abstract StoredNewsgroup getGroupByName(Specification.NewsgroupName name);

    public abstract Peer addPeer(String label, String address)
        throws ExistingPeerException;

    protected abstract Iterator<Peer> getPeers();

    public abstract static class StoredArticle extends Specification.Article {
        private final Specification.ArticleSource source;

        protected StoredArticle(Specification.MessageId messageId, Specification.ArticleSource source) {
            super(messageId);
            this.source = source;
        }

        public abstract Instant getInsertionTime();

        public Specification.ArticleSource getSource() {
            return source;
        }

        public abstract IdentityService.Subject getSubmitter();

        /**
         * Names of all the newsgroups this article is Published in. (i.e. has an assigned ArticleNumber).  Possibly an
         * empty set but never null.
         */
        public abstract Set<Specification.Newsgroup> getPublished();

        /**
         * This method returns the NewsgroupArticle, which associates this StoredArticle with the supplied newsgroup, or
         * null if no such association exists.
         */
        public abstract NewsgroupArticle getNewsgroup(Specification.Newsgroup newsgroup);

        public abstract void ban();

        /**
         * Returns true if this StoredArticle is Published in any newsgroup.
         */
        public abstract boolean isPublished();
    }

    /**
     * A StoredNewsgroup is a Newsgroup that has been added to the datastore but does not have any PublishedArticles
     * associated with it.
     * As soon as at least one PublishedArticle appears in this newsgroup, it is promoted to a PublishedNewsgroup.
     */
    public abstract static class StoredNewsgroup extends Specification.Newsgroup {
        protected StoredNewsgroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, String createdBy) {
            super(name, description, postingMode, createdBy);
        }

        /**
         * Adds this peer as a new feed for this newsgroup if not already present, otherwise returns the existing feed.
         */
        protected abstract Feed addFeed(Peer peer);

        public abstract Feed[] getFeeds();

        /**
         * This method returns the number of Pending (i.e. not Published) NewsgroupArticles (i.e., associations
         * between a StoredArticle and a Newsgroup) in this Newsgroup, or 0 if there are none.
         */
        public abstract int numPendingArticles();

        /**
         * Returns all the Pending Articles currently within this newsgroup
         */
        public abstract Iterable<? extends PendingArticle> getPendingArticles();

        /**
         * Removes this Feed from the Newsgroup but does not delete the Peer associated with the Feed.
         */
        protected abstract void deleteFeed(Feed feed);
    }

    /**
     * A PublishedNewsgroup is a Newsgroup that contains at least one PublishedArticle.
     */
    public abstract static class PublishedNewsgroup extends StoredNewsgroup {

        protected PublishedNewsgroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, String createdBy) {
            super(name, description, postingMode, createdBy);
        }
        /**
         * This method returns a list of PublishedArticles (i.e., associations between a StoredArticle and a
         * Newsgroup) that were associated with this Newsgroup since the supplied insertionTime.  This method uses the
         * time that the association was transitioned to Published: i.e. when the article was assigned its ArticleNumber
         * in this newsgroup. The returned list is ordered by that parameter in ascending order (i.e., oldest first).
         */
        protected abstract Iterator<PublishedArticle> getArticlesSince(Instant insertionTime);

        /**
         * Returns the PublishedArticle with the specified articleNumber or null if no such published article with that
         * ArticleNumber exists.
         */
        protected abstract PublishedArticle getArticleNumbered(Specification.ArticleNumber articleNumber);

        /**
         * Returns a list of PublishedArticles (in order) of their ArticleNumbers within the specified bounds
         * (inclusive).  One or both of the supplied ArticleNumbers may be null, in which case there is no restriction
         * on the corresponding bound.
         */
        protected abstract Iterator<PublishedArticle> getArticlesNumbered(Specification.ArticleNumber lowerBound,
                                                                      Specification.ArticleNumber upperBound);

        /**
         * Returns the first PublishedArticle in this Newsgroup (i.e., one with an ArticleNumber) or null if no such exists.
         */
        protected abstract PublishedArticle getFirstArticle();

        /**
         * Returns the PublishedArticle in the newsgroup that is adjacent to the supplied current article and whose
         * article number is the next higher one than the supplied current article's article number, or null if no such
         * article exists.
         */
        protected abstract PublishedArticle getNextArticle(Specification.ArticleNumber currentArticle);

        /**
         * Returns the PublishedArticle in the newsgroup that is adjacent to the supplied current article and whose
         * article number is the next lower one than the supplied current article's article number, or null if no such
         * article exists.
         */
        protected abstract PublishedArticle getPreviousArticle(Specification.ArticleNumber currentArticle);

        /**
         * Returns the PublishedArticle in the newsgroup whose messageId matches that supplied, or null if no such
         * PublishedArticle exists even though a PendingArticle with such a messageId may exist.
         */
        public abstract PublishedArticle getPublishedArticle(Specification.MessageId messageId);
    }

    /**
     * A NewsgroupArticle is an association between a StoredArticle and a StoredNewsgroup.
     * As all Articles are associated with at least one Newsgroup, therefore each Article has at least one such
     * association.  Whereas a StoredNewsgroup may not have any Articles associated with it and therefore may not have
     * any such associations.
     */
    public abstract static class NewsgroupArticle {
        protected abstract StoredNewsgroup getNewsgroup();
        protected abstract StoredArticle getArticle();
    }

    /**
     * Associates the supplied storedArticle with the supplied newsgroup yielding a PendingArticle.
     * If the storedArticle is already associated with this newsgroup, then returns that existing association.
     */
    protected abstract PendingArticle addAssociation(StoredArticle storedArticle, StoredNewsgroup newsgroup);

    /**
     * Deletes this article-newsgroup association from the persistent store, including any ArticleNumber, which might be
     * related to this association.
     */
    protected abstract void deleteAssociation(NewsgroupArticle newsgroupArticle);

    /**
     * A PendingArticle is an Article associated with a particular Newsgroup that has not been published or rejected.
     * It is a candidate for review by the PolicyService's reviewPosting() method.
     */
    public abstract static class PendingArticle extends NewsgroupArticle {
        /**
         * Publishes this PendingArticle, causing it to be:
         * a) promoted to the PublishedArticle subtype,
         * b) assigned the next ArticleNumber from this Newsgroup's ArticleNumber sequence,
         * c) setting its PublicationTime to the instant this method was invoked, and
         * d) promoting the associated Newsgroup to a PublishedNewsgroup if not already the case.
         */
        protected abstract void publish();

        /**
         * Rejects this PendingArticle, causing it to be removed from the associated Newsgroup.
         */
        protected abstract void reject();
    }

    /**
     * A PublishedArticle is an Article associated with a particular Newsgroup that has been published via the
     * PendingArticle's publish() method.
     */
    public abstract static class PublishedArticle extends NewsgroupArticle {
        /**
         * Returns the immutable ArticleNumber assigned to this StoredArticle in this Newsgroup. Never null.
         */
        public abstract Specification.ArticleNumber getArticleNumber();

        /**
         * Returns the time when this association transitioned from Pending to Published: i.e. the time when this
         * article was assigned its ArticleNumber in this newsgroup.
         */
        public abstract Instant getPublicationTime();

        /**
         * Rejects this PublishedArticle, causing it to be:
         * a) removed from the associated Newsgroup,
         * b) releasing its associated ArticleNumber (never to be re-used), and
         * c) demoting the associated PublishedNewsgroup to a StoredNewsgroup if this were the only PublishedArticle.
         */
        protected abstract void reject();
    }

    /**
     * A Feed is a source of articles and metadata from a particular Peer for a particular Newsgroup.
     * Note, a newsgroup may have multiple feeds if there are different Peers hosting the same newsgroup.
     * A feed maintains the state (highest article number and time) of the last sync with the Peer and includes
     * the highest article number reported by the Peer.
     */
    public abstract static class Feed {
        // the time (using the Peer's clock) of our last successful (pull) sync of articles from the Peer
        protected abstract Instant getLastPullSync();
        protected abstract void setLastPullSync(Instant time);

        // the time (using our local clock) of our last successful (push) sync of articles to the Peer
        protected abstract Instant getLastPushSync();
        protected abstract void setLastPushSync(Instant time);

        // local state. highest article number synced with this Peer, or null if none
        public abstract Specification.ArticleNumber getHighestArticleNumber();
        protected abstract void setHighestArticleNumber(int num);

        public abstract Peer getPeer();
        abstract StoredNewsgroup getNewsgroup();
    }

    /**
     * A Peer is another NNTP Service that responds to NNTP Commands.   It is uniquely identified by its address,
     * which is also immutable.
     */
    public abstract static class Peer implements IdentityService.Subject {
        private int identifier;

        /**
         * ID is the immutable identifier for this Peer.
         */
        protected int getID() {
            return identifier;
        }

        /**
         * GetAddress returns the immutable address of this Peer.
         *
         * @return the immutable address of this Peer
         */
        public abstract String getAddress();

        public abstract boolean isDisabled();
        public abstract void setDisabledStatus(boolean disabled);

        /**
         * GetLabel returns a free-form field (the label) that has no particular meaning to the NNTP Protocol Engine.
         * Its use is purely for human consumption - e.g. a human-readable name for the Peer.
         * @return the immutable label of this Peer
         */
        public abstract String getLabel();
        public abstract void setLabel(String label);

        // the time (on the peer) when the newsgroups list was last fetched
        public abstract Instant getListLastFetched();
        protected abstract void setListLastFetched(Instant lastFetched);
    }

    /**
     * Deletes the supplied peer from the datastore and also deletes all associated Feeds.
     */
    protected abstract void deletePeer(Peer peer);

    public static class ExistingPeerException extends Exception {
        public ExistingPeerException(String message) {
            super(message);
        }
    }

    /**
     * Persists the supplied identifier in the database.
     */
    protected abstract void saveId(String id);

    /**
     * Determines if the supplied identifier was previously persisted via the saveId() method.
     */
    protected abstract boolean existsId(String id);

    /**
     * Persists the supplied value in the database.  The value is associated with the supplied key.
     * If the key already exists, then the value is updated.  If the key does not exist, then it is created.
     * If the value is null, then the key and its value are deleted.
     */
    protected abstract void saveValue(String key, String value);

    /**
     * Retrieves the value associated with the supplied key.  Returns null if no such key exists.
     */
    protected abstract String fetchValue(String key);

    /**
     * This method identifies whether the supplied messageId is known to the datastore.  A known messageId maybe a
     * stored article or a banned article.
     */
    boolean isKnown(Specification.MessageId messageId) {
        return hasArticle(messageId) || isBanned(messageId);
    }

    /**
     * This method identifies whether the supplied newsgroup name is known to the datastore.  A known newsgroup maybe a
     * stored newsgroup or a banned newsgroup.
     */
    public boolean isKnown(Specification.NewsgroupName name) {
        return getGroupByName(name) != null || isBanned(name);
    }

    public boolean isBanned(Specification.MessageId messageId) {
        return existsId(Utilities.Cryptography.sha256(messageId.getValue()));
    }

    public boolean isBanned(Specification.NewsgroupName name) {
        return existsId(Utilities.Cryptography.sha256(name.getValue()));
    }

    public void ban(Specification.MessageId messageId) {
        saveId(Utilities.Cryptography.sha256(messageId.getValue()));
        deleteArticle(messageId);
    }

    public void ban(Specification.NewsgroupName newsgroupName) {
        saveId(Utilities.Cryptography.sha256(newsgroupName.getValue()));
        deleteNewsgroup(getGroupByName(newsgroupName));
    }
}

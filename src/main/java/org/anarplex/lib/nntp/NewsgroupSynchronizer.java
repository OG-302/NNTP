package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.PersistenceService.Feed;
import org.anarplex.lib.nntp.Specification.ArticleNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;

public class NewsgroupSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(NewsgroupSynchronizer.class);

    private final PersistenceService persistenceService;
    private final PolicyService policyService;
    private final NetworkService.PeerConnectionCache peerConnectionCache;

    /**
     * A class that represents an externally advertised newsgroup.
     * Empty Newsgroups (i.e. ones that reported no Published articles) have their highestArticleNumber == lowestArticleNumber == null.
     */
    static class ExternalNewsgroup {
        Specification.NewsgroupName name;
        Specification.PostingMode postingMode;
        ArticleNumber lowestArticleNumber;
        ArticleNumber highestArticleNumber;

        ExternalNewsgroup(Specification.NewsgroupName name,
                          Specification.PostingMode postingMode,
                          ArticleNumber lowestArticleNumber,
                          ArticleNumber highestArticleNumber) {
            this.name = name;
            this.postingMode = postingMode;
            if (highestArticleNumber != null && lowestArticleNumber != null && (highestArticleNumber.getValue() < lowestArticleNumber.getValue())) {
                this.highestArticleNumber = this.lowestArticleNumber = null;
            }
        }

        private int getEstimatedNumArticles() {
            // some NNTP Servers will report an empty newsgroup as Highest = Lowest-1.  Avoid this when calculating size.
            return (highestArticleNumber.getValue() < lowestArticleNumber.getValue()) ? 0 : highestArticleNumber.getValue() - lowestArticleNumber.getValue();
        }
    }

    NewsgroupSynchronizer(PersistenceService persistenceService, PolicyService policyService, NetworkService networkService) {
        this.persistenceService = persistenceService;
        this.policyService = policyService;
        this.peerConnectionCache = new NetworkService.PeerConnectionCache(networkService);
    }

    /**
     * This method requests various newsgroup metadata on all newsgroups published by the supplied Peer.
     * That metadata consist of the newsgroup's description, its creator and the highest article number in the group.
     * The collection of this information is optimized by asking for updates from last sync time when possible.
     * If the Peer returns the name of an unknown newsgroup then a new group with that name is added to the datastore,
     * and this peer is added as one of the newsgroup's feeds.
     * This method DOES NOT fetch/share any articles from/with the Peer.  See the syncNewsgroup() method for that behavior.
     */
    public void fetchNewsgroupsList(NetworkService.ConnectedPeer connectedPeer) {

        if (connectedPeer == null) {
            logger.error("No Peer supplied to fetchNewsgroupsList() call.  Nothing to do.");
            return;
        }
        if (connectedPeer.isDisabled()) {
            logger.info("Peer supplied to fetchNewsgroupsList() call is disabled.");
            return;
        }
        if (!connectedPeer.isConnected()) {
            logger.error("Peer supplied to fetchNewsgroupsList() call is not connected.");
            return;
        }

        // fetch name and description field of all newsgroups hosted by Peer and update datastore with this
        for (Map.Entry<Specification.NewsgroupName, String> entry : getNewsgroupDescriptions(connectedPeer).entrySet()) {
            Specification.NewsgroupName n = entry.getKey();
            String d = entry.getValue();    // description of newsgroup n provided by peer
            if (!persistenceService.isKnown(n)) {
                // add the newsgroup
                PersistenceService.StoredNewsgroup g = persistenceService.addNewsgroup(n, d, null, null);
                // and add this peer as one of its feeds
                g.addFeed(connectedPeer.getPeer());
            } else {
                // newsgroup is known (which includes being banned)
                PersistenceService.StoredNewsgroup g = persistenceService.getNewsgroup(n);
                // update the description if an unbanned group
                if (d != null && g != null && g.getDescription() == null) {
                    // update if missing a description and the peer provided a description
                    logger.info("Newsgroup {} empty description being replaced by {} found on peer {}", n, d, connectedPeer);
                    g.setDescription(d);
                    g.addFeed(connectedPeer.getPeer());
                }
            }
        }

        // fetch name and createdBy field of all newsgroups hosted by Peer and update datastore
        for (Map.Entry<Specification.NewsgroupName, String> entry : getNewsgroupCreatedBy(connectedPeer).entrySet()) {
            Specification.NewsgroupName n = entry.getKey();
            String c = entry.getValue();    // creator of newsgroup n provided by peer
            if (!persistenceService.isKnown(n)) {
                // add the newsgroup
                PersistenceService.StoredNewsgroup g = persistenceService.addNewsgroup(n, null, null, c);
                // and add this peer as one of its feeds
                g.addFeed(connectedPeer.getPeer());
            } else {
                // newsgroup is known (which includes being banned)
                PersistenceService.StoredNewsgroup g = persistenceService.getNewsgroup(n);
                // update the creator if an unbanned group
                if (c != null && g != null && g.getCreatedBy() == null) {
                    logger.info("Newsgroup {} empty creator being replaced by {} found on peer {}", n, c, connectedPeer);
                    g.setCreatedBy(c);
                    g.addFeed(connectedPeer.getPeer());
                }
            }
        }

        // fetch the highest article number on all newsgroups hosted by the Peer since lastFetched
        for (Map.Entry<Specification.NewsgroupName, Integer> entry : getHighestArticleNum(connectedPeer).entrySet()) {
            Specification.NewsgroupName n = entry.getKey();
            int h = entry.getValue();    // highest article number provided by peer for newsgroup n
            if (!persistenceService.isKnown(n)) {
                // add the newsgroup
                PersistenceService.StoredNewsgroup g = persistenceService.addNewsgroup(n, null, null, null);
                // and add this peer as one of its feeds
                Feed f = g.addFeed(connectedPeer.getPeer());
                f.setHighestArticleNumber(h);
            } else {
                // newsgroup is known (which includes being banned)
                PersistenceService.StoredNewsgroup g = persistenceService.getNewsgroup(n);
                // update the highest article number if an unbanned group
                if (h > 0  && g != null) {
                    g.addFeed(connectedPeer.getPeer()).setHighestArticleNumber(h);
                }
            }
        }
    }

    /**
     *  This method takes the supplied Newsgroup (already present in the PersistenceService, which may be a
     *  PublishedNewsgroup but not necessarily so) and synchronizes it with the Peers identified in the group's Feeds.
     *  (Feeds with disabled Peers are skipped.)
     *  Synchronization with a Peer involves first fetching new articles from that Peer and then sharing new (Published)
     *  articles with that Peer.
     *  This is achieved by the following algorithm:
     *  For each Peer recorded as a Feed for this newsgroup, connect to the Peer and use the NEWNEWS command to get
     *  a list of MessageIds of new Articles.  If the Peer does not support NEWNEWS, then LISTGROUP is used.
     *  Each article that is found in that list but not found in the datastore (!isKnown) is fetched from the Peer and
     *  added to the datastore.
     *  Once all Feeds for the newsgroup have been visited in this way, each Feed is again contacted (via IHAVE command)
     *  to see if it is interested in Articles which are present in the datastore (for this newsgroup) but which are
     *  suspected not to exist on the Peer.
     *  This group-wise approach to feed synchronization (one newsgroup at a time, with a fetch round followed by a
     *  sending round) helps to distribute the nntp request load across peers and for each peer across time.
     *  Connections opened with Peers during this process are kept open even after this method returns (useful for
     *  subsequent calls to this method given other newsgroups to sync) and are saved to an internal cache which can be
     *  emptied via CloseAllConnections().
     */
    public void syncNewsgroup(PersistenceService.StoredNewsgroup newsgroup) {

        if (newsgroup == null) {
            logger.error("Newsgroup can't be null.  Nothing to do.");
            return;
        }
        if (persistenceService.isBanned(newsgroup.getName())) {
            logger.info("Skipping Newsgroup({}) because it is banned.", newsgroup);
            return;
        }
        if (Specification.NewsgroupName.isLocal(newsgroup.getName())) {
            logger.info("Skipping Newsgroup({}) because it is a Local (only) newgroup.", newsgroup);
            return;
        }
        PersistenceService.StoredNewsgroup g = persistenceService.getGroupByName(newsgroup.getName());
        if (g == null) {
            logger.error("No such Newsgroup{}", newsgroup);
            return;
        }
        // note this instant in time to avoid race-conditions later
        Instant startOfSync = Instant.now();

        // keep a map of messageIds found on each feed for this newsgroup.  This will be needed to determine whether
        // to offer (via IHAVE) articles to that Peer or not.
        Map<Feed, List<Specification.MessageId>> msgIds = new HashMap<>();

        // Phase 1: Download new articles of this newsgroup from its various feeds (peers).
        for (Feed f : g.getFeeds()) {
            if (f.getPeer().isDisabled()) {
                // skip over peers that have been marked as disabled
                continue;
            }

            // connect with Peer
            NetworkService.ConnectedPeer connectedPeer = peerConnectionCache.getConnectedPeer(f.getPeer());

            if (connectedPeer == null || !connectedPeer.isConnected()) {
                // peer is not connected properly.  skip it.
                logger.warn("Peer {} is not connected properly.  Skipping sync.", f.getPeer().getLabel());
                if (connectedPeer != null) {
                    connectedPeer.close();
                }
                continue;
            }

            // get a RemoteArticleStream via the create() method
            Iterator<Specification.Article> articleStream = RemoteArticleStream.create(
                    f,
                    peerConnectionCache::getConnectedPeer,
                    persistenceService::isKnown
            );

            if (articleStream == null) {
                logger.warn("Could not create RemoteArticleStream for peer {}. Skipping sync.", f.getPeer().getLabel());
                continue;
            }

            // iterate through articles from the peer
            List<Specification.MessageId> fetchedMessageIds = new ArrayList<>();
            while (articleStream.hasNext()) {
                Specification.Article streamedArticle = articleStream.next();
                Specification.MessageId msgId = streamedArticle.getMessageId();

                // check that the retrieved article mentions the current newsgroup in its Newsgroups header
                if (!streamedArticle.getAllHeaders().getNewsgroups().contains(newsgroup.getName())) {
                    continue;
                }

                // have the PolicyService do a review of whether to allow this article into the datastore
                switch (policyService.reviewArticle(streamedArticle, Specification.ArticleSource.NewsgroupSynchronization, connectedPeer.getPeer())) {
                    case Allow -> {
                        // save the article for a later review by PolicyService.reviewPosting()
                        persistenceService.addArticle(streamedArticle, Specification.ArticleSource.NewsgroupSynchronization, connectedPeer.getPeer());
                        fetchedMessageIds.add(msgId);
                    }
                    case Ignore -> {
                        // do nothing / not added to datastore
                    }
                    case Ban -> {
                        // ban this msgId
                        persistenceService.ban(msgId);
                    }
                }
            }

            // record the messageIds that were fetched for later use in phase 2
            msgIds.put(f, fetchedMessageIds);
        }   // per feed loop

        // allow the PolicyService to review all pending articles in this newsgroup because only published articles
        // will be shared with the peers in phase-2 below.
        for (PersistenceService.PendingArticle p : newsgroup.getPendingArticles()) {
            // review the association of this newly added article to this newsgroup.
            policyService.reviewPosting(p);
        }

        // reload the newsgroup as its type may have changed due to reviewPosting() above (e.g. Pending to Published or
        // Banned).
        newsgroup = persistenceService.getNewsgroup(newsgroup.getName());

        if (!(newsgroup instanceof PersistenceService.PublishedNewsgroup publishedNewsgroup)) {
            // if there are no published articles in this newsgroup, then there is nothing to share with other Peers
            return;
        }

        // Phase 2: Share articles from our store with peers.  These interactions (based on IHAVE command) are NOT pipelined.
        for (Feed f : newsgroup.getFeeds()) {
            if (f.getPeer().isDisabled()) {
                // Peer is disabled.  skip it.
                continue;
            }

            // get a connection to the peer
            NetworkService.ConnectedPeer connectedPeer = peerConnectionCache.getConnectedPeer(f.getPeer());

            if (!connectedPeer.isConnected()) {
                // can't reach the Peer.  skip it.
                continue;
            }

            // Do not pre-check for IHAVE capability; rely on server responses to IHAVE offers instead.

            // calculate the articles that we have and that the peer does not, so that they can be shared with the peer.
            // get the list of articles in the datastore that were added to the newsgroup since last sync time with
            // this peer on this newsgroup.
            Instant lastSyncTime = f.getLastPushSync();
            if (lastSyncTime == null) {
                lastSyncTime = Instant.EPOCH;
            }
            Iterator<PersistenceService.PublishedArticle> newArticles = publishedNewsgroup.getArticlesSince(lastSyncTime);

            if (newArticles != null) {
                // Offer all newly published articles since last sync; the peer will indicate interest (335) or not (435/436)
                List<PersistenceService.PublishedArticle> articlesToOffer = new ArrayList<>();
                while (newArticles.hasNext()) {
                    articlesToOffer.add(newArticles.next());
                }

                boolean anyShared = false;
                // for each article provided
                exitLoop:
                for (Iterator<PersistenceService.PublishedArticle> it = articlesToOffer.iterator(); it.hasNext(); ) {
                    PersistenceService.PublishedArticle article = it.next();
                    Specification.MessageId msgId = article.getArticle().getMessageId();
                    // send IHAVE request to peer
                    connectedPeer.sendCommand(Specification.NNTP_Request_Commands.IHAVE, msgId.getValue());
                    // check the response to see if it's wanted
                    Specification.NNTP_Response_Code responseCode = connectedPeer.getResponseCode();
                    switch (responseCode) {
                        case Specification.NNTP_Response_Code.Code_335: // peer is interested, send article
                            logger.info("Peer {} indicated interest in sending article {}. Sending article.", connectedPeer.getLabel(), msgId);
                            // send article to peer
                            connectedPeer.printf("%s", article);
                            // check response from peer about the article transfer
                            Specification.NNTP_Response_Code responseCode2 = connectedPeer.getResponseCode();
                            switch (responseCode2) {
                                case Specification.NNTP_Response_Code.Code_235: // transfer successful
                                    logger.info("Peer {} indicated transfer of article {} was successful.", connectedPeer.getLabel(), msgId);
                                    it.remove();
                                    anyShared = true;
                                    continue;
                                case Specification.NNTP_Response_Code.Code_436: // transfer failed; try again later
                                    logger.warn("Peer {} indicated transfer of article {} not possible at this time.  Try again later.", connectedPeer.getAddress(), msgId);
                                    connectedPeer.close();
                                    break;
                                case Specification.NNTP_Response_Code.Code_437: // transfer rejected; do not retry
                                    logger.info("Peer {} indicated transfer of article {} failed and not to try again.", connectedPeer.getAddress(), msgId);
                                    // presume this will be true for all articles attempted
                                    it.remove();
                                    continue;
                                default:
                                    logger.warn("Peer {} indicated unexpected response code {} to IHAVE command.", connectedPeer.getLabel(), responseCode2);
                                    break;
                            }
                            continue;
                        case Specification.NNTP_Response_Code.Code_435: // peer is not interested, skip article
                            logger.info("Peer {} indicated it was not interested in article {}.  StoredArticle not shared.", connectedPeer.getLabel(), msgId);
                            it.remove();
                            continue;
                        case Specification.NNTP_Response_Code.Code_436: // transfer not possible; try again later
                            logger.warn("Peer {} indicated transfer of article {} not possible at this time.  Try again later.", connectedPeer.getLabel(), msgId);
                            // presume this will be true for all articles attempted
                            connectedPeer.close();
                            break exitLoop;
                        default:
                            logger.warn("Peer {} indicated unexpected response code {} to IHAVE command.", connectedPeer.getLabel(), responseCode);
                            break exitLoop;
                    }
                }
                if (articlesToOffer.isEmpty() || anyShared) {
                    // no unshared articles remain.  update the lastSyncTime in the feed
                    f.setLastPushSync(Instant.now());
                }
            } else {
                logger.info("No new articles in this newsgroup {} since last synced {} with peer {}", f.getNewsgroup().getName(), lastSyncTime, f.getPeer().getLabel());
            }
        }
    }

    /**
     * Sends LIST NEWSGROUPS command to the supplied Peer to get descriptions of all its newsgroups which is returned as
     * a Map of newsgroup names to descriptions, possibly an empty map.
     */
    protected Map<Specification.NewsgroupName, String> getNewsgroupDescriptions(NetworkService.ConnectedPeer peer) {
        Map<Specification.NewsgroupName, String> result = new HashMap<>();
        if (peer.hasCapability(Specification.NNTP_Server_Capabilities.LIST)) {  // for LIST NEWSGROUPS command
            peer.sendCommand(Specification.NNTP_Request_Commands.LIST_NEWSGROUPS);
            try {
                Specification.NNTP_Response_Code responseCode = peer.getResponseCode();
                if (responseCode != null) {
                    if (Specification.NNTP_Response_Code.Code_215.equals(responseCode)) {
                        for (String line : peer.readList()) {
                            // expecting the format to be: <groupname> <description possibly with spaces>
                            String[] parts = line.split(Specification.WHITE_SPACE, 2);
                            try {
                                if (parts.length == 2) {
                                    result.put(new Specification.NewsgroupName(parts[0]), parts[1]);
                                }
                            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                                logger.error("Invalid newsgroup name in LIST NEWSGROUPS response from peer: {}", e.getMessage());
                            }
                        }
                    } else {
                        logger.error("Unexpected response code {} from LIST NEWSGROUPS command to peer {}", peer.getResponseCode(), peer.getAddress());
                    }
                } else {
                    logger.error("No response code from LIST NEWSGROUPS command to peer {}", peer.getAddress());
                }
            } catch (IllegalArgumentException e) {
                logger.error("Error reading LIST NEWSGROUPS response from peer: {}", e.getMessage());
            }
        }
        return result;
    }

    /**
     * Sends LIST ACTIVE.TIMES command to the supplied Peer to get createdBy field of all its newsgroups which is
     * returned as a Map of newsgroup names to descriptions, possibly an empty map, or null on error.
     */
    protected Map<Specification.NewsgroupName, String> getNewsgroupCreatedBy(NetworkService.ConnectedPeer peer) {
        Map<Specification.NewsgroupName, String> result = new HashMap<>();
        if (peer.hasCapability(Specification.NNTP_Server_Capabilities.LIST)) {  // for LIST NEWSGROUPS command
            peer.sendCommand(Specification.NNTP_Request_Commands.LIST_ACTIVE_TIMES);
            try {
                Specification.NNTP_Response_Code responseCode = peer.getResponseCode();
                if (responseCode != null) {
                    if (Specification.NNTP_Response_Code.Code_215.equals(responseCode)) {
                        for (String line : peer.readList()) {
                            // expecting the format to be: <groupname> <activetime> <creator>
                            String[] parts = line.split(Specification.WHITE_SPACE, 3);
                            try {
                                if (parts.length == 3) {
                                    result.put(new Specification.NewsgroupName(parts[0]), parts[2]);
                                }
                            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                                logger.error("Invalid newsgroup name in LIST ACTIVE.TIMES response from peer: {}", e.getMessage());
                            }
                        }
                    } else {
                        logger.error("Unexpected response code {} from LIST ACTIVE.TIMES command to peer {}", peer.getResponseCode(), peer.getAddress());
                    }
                } else {
                    logger.error("No response code from LIST ACTIVE.TIMES command to peer {}", peer.getAddress());
                }
            } catch (IllegalArgumentException e) {
                logger.error("Error reading LIST ACTIVE.TIMES response from peer: {}", e.getMessage());
            }
        }
        return result;
    }

    /**
     * Sends NEWGROUPS command to the supplied Peer to get highest article number field on all its newsgroups which is
     * returned as a Map of newsgroup names to ints possibly an empty map but never null.  If the Peer has been contacted
     * previously, then this NEWGROUPS command is qualified by that datetime so that only NEW groups since then are
     * fetched.
     */
    protected Map<Specification.NewsgroupName, Integer> getHighestArticleNum(NetworkService.ConnectedPeer peer) {
        Map<Specification.NewsgroupName, Integer> result = new HashMap<>();

        if (peer.hasCapability(Specification.NNTP_Server_Capabilities.READER)) {
            // get current time on peer
            Instant timeAndDateOnPeer = getTimeAndDateOnPeer(peer);

            // get last time the list was fetched
            Instant lastFetched = peer.getListLastFetched();
            if  (lastFetched == null) {
                lastFetched = Instant.EPOCH;
            }

            // fetch the list with only those newsgroups new or updated since last list fetch
            peer.sendCommand(
                    Specification.NNTP_Request_Commands.NEW_GROUPS,
                    Utilities.DateAndTime.formatTo_yyyyMMdd_hhmmss(lastFetched));
            try {
                Specification.NNTP_Response_Code responseCode = peer.getResponseCode();
                if (responseCode != null) {
                    if (Specification.NNTP_Response_Code.Code_231.equals(responseCode)) {
                        for (String line : peer.readList()) {
                            // expecting the format to be: <groupname> <high> <low> <mode>
                            String[] parts = line.split(Specification.WHITE_SPACE);
                            try {
                                if (parts.length == 4) {
                                    result.put(new Specification.NewsgroupName(parts[0]), Integer.parseInt(parts[1]));
                                }
                            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                                logger.error("Invalid newsgroup name in NEW_GROUPS response from peer: {}", e.getMessage());
                            } catch (NumberFormatException e) {
                                logger.error("Invalid number format in NEW_GROUPS response from peer: {}", e.getMessage());
                            }
                        }
                    } else {
                        logger.error("Unexpected response code {} from NEW_GROUPS command to peer {}", peer.getResponseCode(), peer.getAddress());
                    }
                } else {
                    logger.error("No response code from NEW_GROUPS command to peer {}", peer.getAddress());
                }
            } catch (IllegalArgumentException e) {
                logger.error("Error reading NEW_GROUPS response from peer: {}", e.getMessage());
            }
            // update last list fetch time
            peer.setListLastFetched(timeAndDateOnPeer);
        }
        return result;
    }



    abstract static class RemoteArticleStream implements Iterator<Specification.Article> {
        protected final PersistenceService.Feed feed;
        protected final NetworkService.ConnectedPeer connectedPeer;
        protected final Function<Specification.MessageId, Boolean> isKnownMsgIdFnc;

        private List<Specification.MessageId> candidateMsgIds = null;       // messageIds that are candidates for fetching
        private Iterator<Specification.MessageId> i = null;                 // iterator over candidateMsgIds
        private final List<Specification.Article> articles = new ArrayList<>();   // articles received from the peer

        final int MaxBatchSize = 10; // number of requests pipelined to peer at one time

        /**
         * Creates a new Iterator that fetches Articles from the supplied Feed's remote Peer.
         * This factory method interrogates the Peer first to determine which Capabilities it supports and then creates
         * a new Iterator which uses a supported Capability based on that.
         * The articles fetched are those that are unknown, according to the supplied isKnownMsgId() function, or null if none.
         */
        static Iterator<Specification.Article> create(PersistenceService.Feed feed,
                                                      Function<PersistenceService.Peer, NetworkService.ConnectedPeer> connectedPeerLookup,
                                                      Function<Specification.MessageId, Boolean> isKnownMsgIdFnc) {

            NetworkService.ConnectedPeer connectedPeer = connectedPeerLookup.apply(feed.getPeer());
            // must have a connected Peer to proceed
            if (connectedPeer != null && connectedPeer.isConnected()) {
                // Compile a list of possible implementations of RemoteArticleStream each based on a different NNTP command
                // set, in order of preference.
                RemoteArticleStream[] ras= new RemoteArticleStream[]{
                        new RemoteArticleStreamNN(feed, connectedPeerLookup, isKnownMsgIdFnc),
                        new RemoteArticleStreamLG(feed, connectedPeerLookup, isKnownMsgIdFnc),
                };
                RemoteArticleStream firstSupported = null;
                for (RemoteArticleStream candidate : ras) {
                    if (candidate.isSupported() && candidate.hasNext()) {
                        return candidate;
                    }
                }
            } else {
                logger.error("Peer {} is not connected properly. No (further) sharing of articles with peer.", feed.getPeer().getLabel());
            }
            return null;
        }

        RemoteArticleStream(PersistenceService.Feed feed,
                            Function<PersistenceService.Peer, NetworkService.ConnectedPeer> connectedPeerLookup,
                            Function<Specification.MessageId, Boolean> isKnownMsgIdFnc) {
            this.feed = feed;
            this.connectedPeer = connectedPeerLookup.apply(feed.getPeer());
            this.isKnownMsgIdFnc = isKnownMsgIdFnc;
        }

        final boolean isSupported() {
            // Be tolerant: prefer capability checks inside concrete commands; allow trying and falling back.
            return connectedPeer.isConnected() && connectedPeer.hasCapability(requiresCapability());
        }

        abstract protected Specification.NNTP_Server_Capabilities requiresCapability();

        @Override
        public boolean hasNext() {
            if (candidateMsgIds == null) {
                // initialization
                // get all the candidate messageIds from the Peer (keep the order)
                candidateMsgIds = getMessageIds(feed);
                // and create an iterator over them
                i = candidateMsgIds.iterator();
            }
            // fetch a batch of articles.  Advances i through the candidateMsgIds and fills the articles List.
            requestBatch();
            return !articles.isEmpty();
        }

        @Override
        public Specification.Article next() {
            // consume another article from the batch
            if (!articles.isEmpty()) {
                return articles.removeFirst();
            } else {
                throw new NoSuchElementException();
            }
        }

        /**
         * Gets a list of all the messageIds of articles that should be fetched from the peer.
         */
        abstract protected List<Specification.MessageId> getMessageIds(PersistenceService.Feed feed);

        private void requestBatch() {
            // articles remain unrequested from the Peer
            if (connectedPeer.isConnected()) {
                // all retrieved articles have been processed, so request another batch
                int pendingRequests = 0;
                // Track which messageIds we requested in this batch to validate responses
                final Set<Specification.MessageId> batchRequested = new HashSet<>();
                while (i.hasNext() && pendingRequests < MaxBatchSize) {
                    Specification.MessageId nextId = i.next();
                    connectedPeer.sendCommand(Specification.NNTP_Request_Commands.ARTICLE, nextId.getValue());
                    batchRequested.add(nextId);
                    pendingRequests++;
                }
                // check the response from the peer
                try {
                    // expecting the same number of (pipelined) responses as requests
                    while (0 < pendingRequests--) {
                        Specification.NNTP_Response_Code responseCode = connectedPeer.getResponseCode();
                        if (Specification.NNTP_Response_Code.Code_220.equals(responseCode)) {
                            // the expected format is "220 0|n <message-id>" with the Article following on multiple lines
                            String line = connectedPeer.getCurrentLine();  // interpret remainder of reply line
                            String[] parts = line.split(Specification.WHITE_SPACE);

                            if (parts.length == 3) {
                                // read stream into an article object
                                Specification.ProtoArticle protoArticle = Specification.ProtoArticle.fromString(connectedPeer.readStream());

                                // proto article should pass validation
                                Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(protoArticle.getHeadersLowerCase());

                                // extract msgId from response line
                                Specification.MessageId msgId = new Specification.MessageId(parts[2]);

                                // check that messageIds match
                                if (!articleHeaders.getMessageId().equals(msgId)) {
                                    logger.warn("Article {} from peer {} had MessageId {} but expected {}",
                                            msgId, connectedPeer.getAddress(), articleHeaders.getMessageId(), msgId);
                                    // a mismatch is discouraged but not prohibited by the Spec.  Continue.
                                }

                                // check that this article is one that we requested
                                if (batchRequested.contains(msgId)) {
                                    // build the article from its received components
                                    Specification.Article streamedArticle = new Specification.Article(msgId, articleHeaders, protoArticle.getBodyText());
                                    // add to articles set
                                    articles.addLast(streamedArticle);
                                    // no longer needed in the batch set
                                    batchRequested.remove(msgId);
                                } else {
                                    logger.error("Unrequested article{} from peer{}", msgId, connectedPeer.getLabel());
                                    // ignore
                                }
                            } else {
                                // got response 220 but response line is syntactically invalid.
                                logger.error("Invalid response line {} from Peer {} in response to ARTICLE command", connectedPeer.getCurrentLine(), connectedPeer.getLabel());
                                // close the connection to the peer.
                                connectedPeer.close();
                                // abort this loop which tries to read all pipelined responses.
                                break;
                            }
                        } else if (Specification.NNTP_Response_Code.Code_430.equals(responseCode)) {
                            // Peer says they don't know that messageId.
                            logger.info("Peer {} doesn't recognize a messageId they were advertising.  They responded with {}.", connectedPeer.getLabel(), connectedPeer.getCurrentLine());
                            // do nothing, allows the loop to continue with the next pipelined response.
                        } else {
                            logger.error("Unknown response {} from Peer {} in response to ARTICLE command", connectedPeer.getCurrentLine(), connectedPeer.getLabel());
                            // didn't recognize the response code.  close the connection to the peer.
                            connectedPeer.close();
                            // abort this loop which tries to read all pipelined responses.
                            break;
                        }
                    }
                } catch (Specification.MessageId.InvalidMessageIdException e) {
                    logger.error("Error reading response from peer {}.  Invalid MessageID {}.", feed.getPeer().getLabel(), e.getMessage());
                    // possibly a corrupt reply stream.  close the connection to the peer.
                    connectedPeer.close();
                } catch (Specification.Article.InvalidArticleFormatException e) {
                    logger.error("Error reading response from peer {}.  Invalid Article format: {}", feed.getPeer().getLabel(), e.getMessage());
                    // possibly a corrupt reply stream.  close the connection to the peer.
                    connectedPeer.close();
                } catch (Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
                    logger.error("Error reading response from peer {}.  Invalid Article Header {}", feed.getPeer().getLabel(), e.getMessage());
                    // possibly a corrupt reply stream.  close the connection to the peer.
                    connectedPeer.close();
                }
            }
        }
    }

    /**
     * Represents a specialized stream that fetches new articles from a remote NNTP server using the NEWNEWS
     * command.  This class is designed to interact with a connected NNTP peer and request new articles of the Feed
     * in a batched manner.
     */
    private static class RemoteArticleStreamNN extends RemoteArticleStream {
        private Instant timeOfQuery = null;                                 // when the query was initiated

        RemoteArticleStreamNN(PersistenceService.Feed feed,
                              Function<PersistenceService.Peer, NetworkService.ConnectedPeer> connectedPeerLookup,
                              Function<Specification.MessageId, Boolean> isKnownFnc) {
            super(feed, connectedPeerLookup, isKnownFnc);
        }

        protected Specification.NNTP_Server_Capabilities requiresCapability() {
            return Specification.NNTP_Server_Capabilities.NEW_NEWS;
        }

        /**
         * Sends a NEWNEWS command to the connected Peer to fetch the articles that are unknown to the Peer.
         * The initial response is a list of messageIds, which this method then filters out ones already known,
         * then requests the articles themselves via ARTICLE commands in pipelined batches.
         */
        @Override
        public boolean hasNext() {
            if (timeOfQuery == null) {
                // initialization
                // get the time value of this instant on the Peer
                if ((timeOfQuery = getTimeAndDateOnPeer(connectedPeer)) == null) {
                    return false;
                }
            }
            if (super.hasNext()) {
                return true;
            } else {
                // no more articles to fetch.  update the feed to reflect the last time the query was performed.
                if (timeOfQuery != null) {
                    feed.setLastPullSync(timeOfQuery);
                }
                return false;
            }
        }

        /**
         * Collects a list of messageIds from the Peer that is new for this newsgroup since the last time the list was
         * synced (see Feed).  The messageIds returned are filtered to remove messageIds already known to the datastore.
         * They are also filtered to remove duplicates.  Although the order returned by the Peer, according to the RFC,
         * maybe arbitrary, that order is preserved in the returned list.
         */
        protected List<Specification.MessageId> getMessageIds(Feed feed) {
            List<Specification.MessageId> result = new ArrayList<>();

            if (connectedPeer.isConnected()) {
                // send command: NEWNEWS <newsgroupname> yyyyMMdd hhmmss GMT
                Instant lastSyncTime = feed.getLastPullSync() == null ? Instant.EPOCH : feed.getLastPullSync();
                connectedPeer.sendCommand(
                        Specification.NNTP_Request_Commands.NEW_NEWS,
                        feed.getNewsgroup().getName().toString(),
                        Utilities.DateAndTime.formatTo_yyyyMMdd_hhmmss(lastSyncTime));

                // collect result and filter out any messageIds already known to the datastore
                Specification.NNTP_Response_Code responseCode = connectedPeer.getResponseCode();
                if (responseCode != null) {
                    if (Specification.NNTP_Response_Code.Code_230.equals(responseCode)) {
                        // read the entire response from peer and interpret as a list of messageIds
                        for (String line : connectedPeer.readList()) {
                            try {
                                Specification.MessageId m = new Specification.MessageId(line);
                                if (!result.contains(m) && !isKnownMsgIdFnc.apply(m)) {
                                    // only follow messageIds that we don't already have (or know about), and no duplicates
                                    result.add(m);
                                }
                            } catch (Specification.MessageId.InvalidMessageIdException e) {
                                logger.error("Invalid MessageId in NEWNEWS response from peer {}: {}", connectedPeer.getAddress(), e.getMessage());
                                // ignore this messageId but continue with the rest of the list
                            }
                        }
                    } else {
                        logger.warn("Peer {} returned unexpected response code {} to NEWNEWS command.", connectedPeer.getAddress(), responseCode);
                    }
                }
            }
            return result;
        }
    }


    private static class RemoteArticleStreamLG extends RemoteArticleStream {
        private final Map<Specification.MessageId, ArticleNumber>  articleNumbersPending = new HashMap<>();   // list of outstanding Articles and their ArticleNumbers
        private ArticleNumber peerHighestArticleNumber = null;

        RemoteArticleStreamLG(Feed feed, Function<PersistenceService.Peer, NetworkService.ConnectedPeer> connectedPeerFunc, Function<Specification.MessageId, Boolean> isKnownFnc) {
            super(feed, connectedPeerFunc, isKnownFnc);
        }

        @Override
        protected Specification.NNTP_Server_Capabilities requiresCapability() {
            return Specification.NNTP_Server_Capabilities.READER;
        }
        
        @Override
        public boolean hasNext() {
            if (super.hasNext()) {
                return true;
            } else {
                // no more articles to fetch.
                ArticleNumber smallestPending = articleNumbersPending.values().stream()
                        .min(Comparator.comparingInt(ArticleNumber::getValue))
                        .orElse(null);

                if (smallestPending != null) {
                    // not all articles were fetched.  only update to smallest pending ArticleNumber
                    feed.setHighestArticleNumber(smallestPending.getValue());
                } else if (peerHighestArticleNumber != null) {
                    // all articles fetched.  used the newsgroup's highest article number
                    feed.setHighestArticleNumber(peerHighestArticleNumber.getValue());
                }
                return false;
            }
        }
        
        @Override
        public Specification.Article next() {
            Specification.Article article = super.next();
            // remove this MessageId from those pending, and its associated ArticleNumber
            articleNumbersPending.remove(article.messageId);
            return article;
        }
        
        /**
         * Collects a list of messageIds from the Peer that is new for this newsgroup based on the last articleNumber
         * synced (see Feed).  The messageIds returned are filtered to remove messageIds already known to the datastore.
         * They are also filtered to remove duplicates.  Although the order returned by the Peer, according to the RFC,
         * maybe arbitrary, that order is preserved in the returned list.
         */
        protected List<Specification.MessageId> getMessageIds(Feed feed) {
            List<Specification.MessageId> result = new ArrayList<>();

            if (connectedPeer.isConnected()) {
                // highest number previously synced to (if any)
                ArticleNumber lastSync = feed.getHighestArticleNumber();

                // start the query from our last sync point
                if (lastSync != null) {
                    // send command: LISTGROUP <newsgroupname> <n>-
                    connectedPeer.sendCommand(
                            Specification.NNTP_Request_Commands.LIST_GROUP,
                            feed.getNewsgroup().getName().toString(),
                            lastSync + "-");
                } else {
                    // send command: LISTGROUP <newsgroupname>
                    connectedPeer.sendCommand(
                            Specification.NNTP_Request_Commands.LIST_GROUP,
                            feed.getNewsgroup().getName().toString());
                }

                // a successful result is: 211 low high groupname, followed by an articleNumber per line
                List<ArticleNumber> articleNumbers = new ArrayList<>();
                Specification.NNTP_Response_Code responseCode = connectedPeer.getResponseCode();
                if (Specification.NNTP_Response_Code.Code_211.equals(responseCode)) {
                    String responseLine = connectedPeer.getCurrentLine();
                    String[] part = responseLine.split(Specification.WHITE_SPACE, 4);

                    try {
                        if (part.length == 4 && feed.getNewsgroup().getName().equals(new Specification.NewsgroupName(part[3]))) {
                            peerHighestArticleNumber = new ArticleNumber(part[2]);

                            // read the following lines from the peer and interpret as a list of ArticleNumbers
                            Set<Specification.ArticleNumber> requestsSent = new HashSet<>();
                            for (String line: connectedPeer.readList()) {
                                try {
                                    Specification.ArticleNumber an = new Specification.ArticleNumber(line.trim());

                                    // generate a pipeline of STAT requests to find out the MessageId corresponding to this ArticleNumber
                                    connectedPeer.sendCommand(Specification.NNTP_Request_Commands.STAT, an.toString());
                                    requestsSent.add(an);
                                } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                                    logger.error("Invalid ArticleNumber in LISTGROUP response from peer {}: {}", connectedPeer.getAddress(), e.getMessage());
                                }
                            }
                            for (int i = 0; i < requestsSent.size(); i++) {
                                Specification.NNTP_Response_Code responseCode2 = connectedPeer.getResponseCode();
                                if (Specification.NNTP_Response_Code.Code_223.equals(responseCode2)) {
                                    // got response: 223 n msgId
                                    part = connectedPeer.getCurrentLine().split(Specification.WHITE_SPACE, 3);
                                    try {
                                        if (part.length == 3) {
                                            Specification.ArticleNumber a = new ArticleNumber(part[1]);
                                            Specification.MessageId m = new Specification.MessageId(part[2]);

                                            if (requestsSent.remove(a) && !result.contains(m) && !isKnownMsgIdFnc.apply(m)) {
                                                // check to see if this messageId is already known to us
                                               result.add(m);
                                               articleNumbersPending.put(m, a);
                                            }
                                        }
                                    } catch (ArticleNumber.InvalidArticleNumberException e) {
                                        logger.error("Invalid ArticleNumber in STAT response from peer {}: {}", connectedPeer.getAddress(), e.getMessage());
                                        return result;
                                    } catch (Specification.MessageId.InvalidMessageIdException e) {
                                        logger.error("Error reading STAT response from peer {}.  Invalid MessageID {}.", feed.getPeer().getLabel(), e.getMessage());
                                    }
                                }
                            }
                        }
                    } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                        logger.error("Mismatched newsgroup name in LISTGROUP response from peer: {}", e.getMessage());
                        return result;
                    } catch (ArticleNumber.InvalidArticleNumberException e) {
                        logger.error("Invalid article number in LISTGROUP response from peer: {}", e.getMessage());
                        return result;
                    }
                } else {
                    logger.error("Unsuccessful response code from LISTGROUP command to peer {}", connectedPeer.getAddress());
                    return result;
                }
            }
            return result;
        }
    }




    /**
     * Contact the peer to get its local data and time.  If the response is not a 211 code or if the result can not
     * be parsed, then return null.
     */
    static Instant getTimeAndDateOnPeer(NetworkService.ConnectedPeer peer) {
        if (peer == null) {
            logger.error("Peer should not be null in call to getTimeAndDateOnPeer()");
            return null;
        }
        if (peer.isConnected()) {
            if (peer.hasCapability(Specification.NNTP_Server_Capabilities.READER)) {

                // contact peer for its local date time
                peer.sendCommand(Specification.NNTP_Request_Commands.DATE);

                Specification.NNTP_Response_Code responseCode = peer.getResponseCode();
                if (Specification.NNTP_Response_Code.Code_111.equals(responseCode)) {
                    return Utilities.DateAndTime.parse_yyyMMddHHmmss(peer.readNextLine());
                } else {
                    logger.warn("Peer {} returned unexpected response code {} to DATE command.", peer.getLabel(), responseCode);
                }

                // default value is unix epoch
                return Instant.EPOCH;
            } else {
                logger.warn("Peer {} does not support DATE.", peer.getLabel());
            }
        } else {
            logger.error("Peer {} is not connected properly.  Skipping Date request.", peer.getLabel());
        }

        return null;
    }

    /**
     * Asks the Peer for a list of Newsgroups it maintains.
     */
    ExternalNewsgroup[] getNewNewsgroupsList(NetworkService.ConnectedPeer peer) {
        ArrayList<ExternalNewsgroup> newsgroups = new ArrayList<>();

        if (peer == null) {
            logger.error("Peer should not be null in call to getNewNewsgroupsList()");
            return null;
        }

        if (!peer.hasCapability(Specification.NNTP_Server_Capabilities.READER)) {
            logger.warn("Peer {} does not support NEWNEWS.  Using LISTGROUPS instead.", peer.getAddress());
            // TODO is there another way to obtain a list of Newsgroups new to us?
            return null;
        }

        // determine the time since last fetched on this peer
        Instant lastFetched = peer.getListLastFetched();

        if (lastFetched == null) {
            lastFetched = Instant.EPOCH; // Default to unix epoch if never fetched

            // send NewGroups command to Peer to get its list of Newsgroups, as per RFC-3977 format: NEWGROUPS yyyyMMdd hhmmss
            peer.sendCommand(Specification.NNTP_Request_Commands.NEW_GROUPS, Utilities.DateAndTime.formatTo_yyyyMMdd_hhmmss(lastFetched));

            // read response from inputStream
            if (Specification.NNTP_Response_Code.Code_231.equals(peer.getResponseCode())) {
                // request accepted.  response follows

                // RFC-3977: Read a multi-line response until a line containing only "." is found
                // split response into lines
                for (String line : peer.readList()) {
                    try {
                        // for each line in the response
                        String[] parts = line.split(Specification.WHITE_SPACE);    // format: <groupname> <high> <low> <status>
                        Specification.NewsgroupName name = new Specification.NewsgroupName(parts[0]);

                        // fetch high and low article numbers from response
                        int high = Integer.parseInt(parts[1]);
                        int low = Integer.parseInt(parts[2]);

                        // adopt our convention for out-of-bounds high and low article numbers
                        if (high == low - 1 || (high == low && low == 0)) {
                            high = Specification.NoArticlesHighestNumber;
                            low = Specification.NoArticlesLowestNumber;
                        }
                        // and sanitize invalid values
                        if (!ArticleNumber.isValid(high)) {
                            logger.error("Invalid high article number: {}", high);
                            high = Specification.NoArticlesHighestNumber;
                        }
                        if (!ArticleNumber.isValid(low)) {
                            logger.error("Invalid low article number: {}", low);
                            low = Specification.NoArticlesLowestNumber;
                        }

                        // Map NNTP status character to domain PostingMode
                        Specification.PostingMode mode = switch (parts[3].toLowerCase()) {
                            case "y" -> Specification.PostingMode.Allowed;
                            case "m" -> Specification.PostingMode.Moderated;
                            default -> Specification.PostingMode.Prohibited;
                        };

                        newsgroups.add(new ExternalNewsgroup(name, mode, new ArticleNumber(low), new ArticleNumber(high)));
                    } catch (Specification.NewsgroupName.InvalidNewsgroupNameException
                             | Specification.ArticleNumber.InvalidArticleNumberException e) {
                        logger.warn("Malformed newsgroup entry received from peer {}: {}", peer.getAddress(), line);
                    }
                }
            }

            // Update the peer's fetch timestamp upon successful retrieval
            Instant peerServerTime = getTimeAndDateOnPeer(peer);
            if (peerServerTime == null) {
                peerServerTime = Instant.EPOCH;     // in the worst case, just keep using unix epoch
            }
            peer.setListLastFetched(peerServerTime);

        }
        return newsgroups.toArray(new ExternalNewsgroup[0]);
    }

}

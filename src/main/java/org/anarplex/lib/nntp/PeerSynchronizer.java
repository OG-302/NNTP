package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.Specification.ArticleNumber;
import org.anarplex.lib.nntp.env.NetworkUtilities;
import org.anarplex.lib.nntp.env.PersistenceService;
import org.anarplex.lib.nntp.env.PersistenceService.Feed;
import org.anarplex.lib.nntp.env.PersistenceService.Newsgroup;
import org.anarplex.lib.nntp.env.PolicyService;
import org.anarplex.lib.nntp.utils.DateAndTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class PeerSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(PeerSynchronizer.class);

    private final PersistenceService persistenceService;
    private final PolicyService policyService;
    private final NetworkUtilities.PeerConnectionCache peerConnectionCache;

    /**
     * Represents a newsgroup as advertised by an external peer during synchronization.
     * Empty Newsgroups (i.e. ones that reported no articles) will have the highestArticleNumber == lowestArticleNumber == null.
     */
    public record ExternalNewsgroup(Specification.NewsgroupName name, Specification.PostingMode postingMode,
                                    ArticleNumber lowestArticleNumber, ArticleNumber highestArticleNumber) {

        private int getEstimatedNumArticles() {
            // some NNTP Servers will report an empty newsgroup as Highest = Lowest-1.  Avoid this when calculating size.
            return (highestArticleNumber.getValue() < lowestArticleNumber.getValue()) ? 0 : highestArticleNumber.getValue() - lowestArticleNumber.getValue();
        }
    }

    public PeerSynchronizer(PersistenceService persistenceService, PolicyService policyService, NetworkUtilities networkUtilities) {
        this.persistenceService = persistenceService;
        this.policyService = policyService;
        this.peerConnectionCache = new NetworkUtilities.PeerConnectionCache(networkUtilities);
    }

    public void closeAllConnections() {
        peerConnectionCache.closeAllConnections();
    }

    /* fetchNewsgroupsList connects with the specified Peer and requests a list of all newsgroups created on that Peer
     * since last contacted by fetchNewsgroupsList(), or all its hosted newsgroups if this is the first time.
     * For each newsgroup provided by the peer -
     * - if the newsgroup does not exist in our persistent store, then consult the PolicyService to determine whether this
     * newsgroup should be followed or not, add it to the persistent store (with the ignored flag set as per Policy Service
     * response).
     * - whether the newsgroup is new (see above) or already exists in the store, check to see if this peer mentioned as
     * a feed for the newsgroup and if it is not, make it so.
     */
    public void fetchNewsgroupsList(NetworkUtilities.ConnectedPeer peer) throws IOException {

        if (peer == null) {
            logger.error("No Peer supplied to fetchNewsgroupsList() call.  Nothing to do.");
            return;
        }

        // get the list of newsgroups advertised by this Peer
        ExternalNewsgroup[] extNewsgroups = getNewNewsgroupsList(peer);

        Map<Specification.NewsgroupName, String> descriptions = null;

        // filter through the newgroups applying validity and policy checks, and update the Feeds of the Newsgroups with this Peer
        for (ExternalNewsgroup xn : extNewsgroups) {

            // fetch the named newsgroup
            Newsgroup g = persistenceService.getGroupByName(xn.name());

            if (g == null) {
                // the newsgroup mentioned by the peer does not exist in the datastore
                // get the Policy decision on whether this newsgroup is allowed
                // Note.  The group will be added, regardless of Policy, otherwise if not recorded, we'll forever be asking whether it should be admitted.
                boolean isAllowed = policyService.isNewsgroupAllowed(xn.name(), xn.postingMode(), xn.getEstimatedNumArticles(), peer);
                if (descriptions == null) {
                    // get all the newsgroup descriptions from the peer
                    descriptions = getNewsgroupDescriptions(peer);
                }
                try {
                    g = persistenceService.addGroup(xn.name(), descriptions.getOrDefault(xn.name(), ""), xn.postingMode(), LocalDateTime.now(), peer.getLabel(), isAllowed);
                } catch (PersistenceService.ExistingNewsgroupException e) {
                    // this should not have happened, we checked above
                    throw new RuntimeException(e);
                }
            }
            // newsgroup exists in the datastore

            // check to see if this Peer is recorded as one of its Feeds
            Feed[] feeds = g.getFeeds();
            boolean peerExists = Arrays.stream(feeds)
                    .map(Feed::getPeer)
                    // match Peers based on address
                    .anyMatch(p -> p.getAddress().equals(peer.getAddress()));

            if (!peerExists) {
                // no peer was not found in the existing feeds that had the same address as the supplied peer
                try {
                    // add the supplied Peer as a feed for this newsgroup
                    g.addFeed(peer);
                } catch (Newsgroup.ExistingFeedException e) {
                    logger.error("Error adding Feed for peer {}: {}", peer.getAddress(), e.getMessage());
                }
            }
        }
    }

    /**
     * Sends LIST NEWSGROUPS command to the supplied Peer to get descriptions of all its newsgroups which is returned as a Map of newsgroup names to descriptions, possibly an empty map, or null on error.
     */
    protected Map<Specification.NewsgroupName, String> getNewsgroupDescriptions(NetworkUtilities.ConnectedPeer peer) {
        if (peer.hasCapability(Specification.NNTP_Server_Capabilities.LIST)) {  // for LIST NEWSGROUPS command
            peer.sendCommand(Specification.NNTP_Request_Commands.LIST_NEWSGROUPS);
            try {
                Specification.NNTP_Response_Code responseCode = peer.getResponseCode();
                if (responseCode != null) {
                    if (Specification.NNTP_Response_Code.Code_215.equals(responseCode)) {// RFC-3977: Read multi-line response until a line containing only "." is found
                        String peerResponse = peer.readUntilDotLine();
                        // iterate over the response
                        String[] lines = peerResponse.split("\r\n");
                        Map<Specification.NewsgroupName, String> descriptions = new HashMap<>();
                        for (String line : lines) {
                            if (!".".equals(line)) {
                                String[] parts = line.split("\\s+", 2);    // format: <groupname> <description possibly with spaces>
                                try {
                                    descriptions.put(new Specification.NewsgroupName(parts[0]), (parts.length > 1 ? parts[1] : ""));
                                } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                                    logger.error("Invalid newsgroup name in LIST NEWSGROUPS response from peer: {}", e.getMessage());
                                }
                            } else {
                                // a dot-line is the end of the list
                                break;
                            }
                        }
                        return descriptions;
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
        return null;
    }


    /* syncNewsgroup takes the specified Newsgroup (already present in the PersistenceService) and updates it by
     * synchronizing with those Peers who are recorded in the PersistenceService as Feeds for this Newsgroup.
     * Synchronization with a Peer involves both fetching new articles from that Peer, and sharing new articles with that
     * Peer.  This is achieved by the following algorithm.
     * For each Peer recorded as a Feed for this newsgroup, connect to the Peer and use the NEWNEWS capability to get a
     * list of MessageIds of new Articles.  If the Peer does not support NEWNEWS, then LISTGROUP is used.  Each article
     * so listed but not already found in the local PersistenceService is retrieved from the Peer.  In this way, an
     * up-to-date list of Articles is built in the local PersistenceService.
     * Once every Feed for the newsgroup has been visited for the latest set of articles, each Feed is then again
     * contacted (via IHAVE command) to see if it is interested in Articles that are present locally present but which
     * are not found on the Peer.  In this way, by the end of the whole Sync process, all Peers should have identical
     * sets of Articles for this group.
     * This approach to feed synchronization (one newsgroup at a time) also helps to distribute the request load across
     * peers and for each peer across time.
     * The connections opened to Peers in the process are kept open and saved to an internal cache which can be emptied
     * via CloseAllConnections().
     * @param newsgroup
     */
    public void syncNewsgroup(Newsgroup newsgroup) {

        if (Specification.NewsgroupName.isLocal(newsgroup.getName())) {
            logger.info("Skipping Newsgroup({}) because it is local only", newsgroup.getName());
            return;
        }

        // checkpoint this instant in time to avoid race-conditions later
        LocalDateTime startOfSync = LocalDateTime.now();

        // get the list of feeds for this newsgroup
        Feed[] feeds = newsgroup.getFeeds();

        // keep a map of messageIds found on each feed for this newsgroup
        Map<Feed, List<Specification.MessageId>> msgIds = new HashMap<>();

        // Phase 1: Download new articles from feeds (peers).  These interactions begin with NEWNEWS but then proceed to
        // pipelined ARTICLE commands to fetch articles found on the peer but not in our store.
        for (Feed f : feeds) {
            if (f.getPeer().getDisabledStatus()) {
                // skip over peers that have been marked as disabled
                continue;
            }

            NetworkUtilities.ConnectedPeer connectedPeer = peerConnectionCache.getConnectedPeer(f.getPeer());

            if (connectedPeer == null || !connectedPeer.isConnected()) {
                // peer is not connected properly.  skip it.
                logger.warn("Peer {} is not connected properly.  Skipping sync.", f.getPeer().getLabel());
                if (connectedPeer != null) {
                    connectedPeer.close();
                }
                continue;
            }

            if (!connectedPeer.hasCapability(Specification.NNTP_Server_Capabilities.READER)) {  // for ARTICLE command
                logger.warn("Peer {} does not support READER.", connectedPeer.getAddress());
                continue;
            }

            // connect to the Peer and use the NEWNEWS (or, if not supported, LISTGROUP) capability to get a list of
            // MessageIds of new Articles.
            msgIds.put(f, getNewMessageIds(f));

            // the set of messageIds to be fetched (because we don't have those articles)
            Set<Specification.MessageId> fetchIds = new HashSet<>(msgIds.get(f));

            // determine which articles mentioned in msgIds are not already present in our persistent store
            for (Specification.MessageId msgId : msgIds.get(f)) {
                try {
                    if (newsgroup.getArticle(msgId) == null) {
                        // this article not found in our newsgroup
                        if (persistenceService.hasArticle(msgId)) {
                            // but it was found in our store, perhaps because it was added back when we didn't have this newsgroup
                            // add existing article to this newsgroup as well
                            newsgroup.includeArticle(persistenceService.getArticle(msgId));
                            // no need to fetch it from peer
                            fetchIds.remove(msgId);
                        }
                    } else {
                        // article found in our newsgroup.  No need to fetch it from the peer.
                        fetchIds.remove(msgId);
                    }
                } catch (Newsgroup.ExistingArticleException e) {
                    // this should never happen.  we just checked it wasn't in this newsgroup
                    throw new RuntimeException(e);
                }
            }

            // send a pipelined request to the peer for all the articles we don't have
            for (Specification.MessageId msgId : fetchIds) {
                connectedPeer.sendCommand(Specification.NNTP_Request_Commands.ARTICLE, msgId.getValue());
            }

            // read pipelined responses from peer.  These are all articles that we don't currently have.
            try {
                // expecting the same number of (pipelined) responses as requests
                for (int i = 0; i < fetchIds.size(); i++) {
                    // read the pipelined response from the peer - a response status line followed by an article
                    String line = connectedPeer.readLine();   // the response line which separates Articles.
                    String[] parts = line.split("\\s+");
                    if (3 <= parts.length && "220".equals(parts[0])) {  // Format: 220 0|n <message-id> Article follows (multi-line)
                        Specification.MessageId msgId;

                        // extract msgId from response line
                        msgId = new Specification.MessageId(parts[2]);

                        // read stream into an article object
                        Specification.ProtoArticle protoArticle = Specification.ProtoArticle.fromString(connectedPeer.readUntilDotLine());

                        // proto article should pass validation
                        Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(protoArticle.getHeadersLowerCase());

                        // check that messageIds match
                        if (!articleHeaders.getMessageId().equals(msgId)) {
                            logger.warn("Article {} from peer {} had MessageId {} but expected {}",
                                    msgId, connectedPeer.getAddress(), articleHeaders.getMessageId(), msgId);
                            // a mismatch is discouraged but not prohibited by the Spec.  Continue.
                        }

                        // check that this is one we asked for
                        if (!fetchIds.contains(msgId)) {
                            logger.error("Unrequested article from peer {}", msgId);
                            continue;   // ignore this article.  It's not one we asked for.
                        }

                        // check that the newsgroup doesn't have this article.  It shouldn't have, but could happen in a race-condition.
                        if (newsgroup.getArticle(msgId) == null) {
                            // the received article is not present in our newsgroup

                            // check that this newsgroup is mentioned in the article's header
                            if (!articleHeaders.getNewsgroups().contains(newsgroup.getName())) {
                                continue;
                            }

                            // check with PolicyService on whether to accept the article or just mark it as known but not allowed
                            boolean isAllowed = policyService.isArticleAllowed(msgId, articleHeaders, protoArticle.getBodyText(), newsgroup.getName(), newsgroup.getPostingMode(), f.getPeer());
                            if (!isAllowed) {
                                logger.info("Article {} being added to newsgroup {} was disallowed by PolicyService", msgId, newsgroup.getName());
                            }

                            // add the article to the newsgroup with the isAllowed disposition
                            try {
                                Specification.Article streamedArticle = new Specification.Article(msgId, articleHeaders, protoArticle.getBodyText());

                                PersistenceService.NewsgroupArticle addedArticle = newsgroup.addArticle(msgId, articleHeaders, protoArticle.getBodyText(), !isAllowed);

                                Set<Specification.NewsgroupName> newsgroupNames = streamedArticle.getAllHeaders().getNewsgroups();
                                newsgroupNames.remove(newsgroup.getName()); // delete from the set the newsgroup we just added this article to
                                for (Specification.NewsgroupName n : newsgroupNames) {
                                    if (!Specification.NewsgroupName.isLocal(n)) {
                                        // only non-local groups can have articles added to them
                                        Newsgroup g = persistenceService.getGroupByName(n);
                                        if (g != null && !g.isIgnored()) {
                                            // we do have this newsgroup in our store and it's not being ignored.  Add this article to it.
                                            g.includeArticle(addedArticle);
                                        }
                                    }
                                }
                            } catch (Newsgroup.ExistingArticleException e) {
                                // this should never happen.  we just checked it wasn't in this newsgroup'
                            }
                        }
                    }
                }
            } catch (Specification.MessageId.InvalidMessageIdException e) {
                logger.error("Error reading NEWNEWS response from peer.  Invalid MessageID in response line {}: {}", connectedPeer.getAddress(), e.getMessage());
                throw new RuntimeException(e);
            } catch (Specification.Article.InvalidArticleFormatException e) {
                logger.error("Error reading NEWNEWS response from peer.  Invalid Article format: {}", e.getMessage());
                throw new RuntimeException(e);
            } catch (Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
                logger.error("Error reading Article headers from peer {}: {}", f.getPeer().getLabel(), e.getMessage());
                throw new RuntimeException(e);
            }
        }

        // Phase 2: Share articles from our store with peers.  These interactions (based on IHAVE command) are NOT pipelined.
        for (Feed f : feeds) {
            if (f.getPeer().getDisabledStatus()) {
                // skip over peers that have been marked as disabled
                continue;
            }
            // calculate the articles that we have and that the peer does not, so that they can be shared with the peer.

            // get the list of messageIds of articles the peer has informed us they already have
            List<Specification.MessageId> msgIdsOnPeer = msgIds.get(f);

            // get the list of articles added to the newsgroup in our store since last sync time with this peer on this newsgroup.
            LocalDateTime lastSyncTime = f.getLastSyncTime();
            if (lastSyncTime == null) {
                lastSyncTime = DateAndTime.EPOCH;
            }
            Iterator<PersistenceService.NewsgroupArticle> newArticles = newsgroup.getArticlesSince(lastSyncTime);
            if (newArticles != null) {
                Set<Specification.Article> articlesToOffer = new HashSet<>();   // new articles to share with peer
                while (newArticles.hasNext()) {
                    PersistenceService.NewsgroupArticle article = newArticles.next();
                    // add this article to the set to be shared only if the peer does not already have it.
                    if (!msgIdsOnPeer.contains(article.getMessageId())) {
                        articlesToOffer.add(article);
                    }
                }

                if (!articlesToOffer.isEmpty()) {
                    // get a connection to the peer
                    NetworkUtilities.ConnectedPeer connectedPeer = peerConnectionCache.getConnectedPeer(f.getPeer());

                    if (connectedPeer != null && connectedPeer.isConnected()) {
                        // check that the Peer supports IHAVE capability
                        if (connectedPeer.hasCapability(Specification.NNTP_Server_Capabilities.I_HAVE)) {

                            // for each article provided
                            for (Specification.Article article : articlesToOffer) {
                                if (connectedPeer.isConnected()) {
                                    // send IHAVE request to peer
                                    connectedPeer.sendCommand(Specification.NNTP_Request_Commands.IHAVE, article.getMessageId().getValue());
                                    // check the response to see if it's wanted
                                    Specification.NNTP_Response_Code responseCode = connectedPeer.getResponseCode();
                                    switch (responseCode) {
                                        case Specification.NNTP_Response_Code.Code_335: // peer is interested, send article
                                            logger.info("Peer {} indicated interest in sending article {}. Sending article.", connectedPeer.getLabel(), article.getMessageId());
                                            // send article to peer
                                            connectedPeer.printf("%s", article);
                                            // check response from peer about the article transfer
                                            Specification.NNTP_Response_Code responseCode2 = connectedPeer.getResponseCode();
                                            switch (responseCode2) {
                                                case Specification.NNTP_Response_Code.Code_235: // transfer successful
                                                    logger.info("Peer {} indicated transfer of article {} was successful.", connectedPeer.getLabel(), article.getMessageId());
                                                    articlesToOffer.remove(article);
                                                    continue;
                                                case Specification.NNTP_Response_Code.Code_436: // transfer failed; try again later
                                                    logger.warn("Peer {} indicated transfer of article {} not possible at this time.  Try again later.", connectedPeer.getAddress(), article.getMessageId());
                                                    connectedPeer.close();
                                                    continue;
                                                case Specification.NNTP_Response_Code.Code_437: // transfer rejected; do not retry
                                                    logger.info("Peer {} indicated transfer of article {} failed and not to try again.", connectedPeer.getAddress(), article.getMessageId());
                                                    // presume this will be true for all articles attempted
                                                    articlesToOffer.remove(article);
                                                    continue;
                                                default:
                                                    logger.warn("Peer {} indicated unexpected response code {} to IHAVE command.", connectedPeer.getLabel(), responseCode2);
                                            }
                                            continue;
                                        case Specification.NNTP_Response_Code.Code_435: // peer is not interested, skip article
                                            logger.info("Peer {} indicated it was not interested in article {}.  Article not shared.", connectedPeer.getLabel(), article.getMessageId());
                                            articlesToOffer.remove(article);
                                            continue;
                                        case Specification.NNTP_Response_Code.Code_436: // transfer not possible; try again later
                                            logger.warn("Peer {} indicated transfer of article {} not possible at this time.  Try again later.", connectedPeer.getLabel(), article.getMessageId());
                                            // presume this will be true for all articles attempted
                                            connectedPeer.close();
                                            continue;
                                        default:
                                            logger.warn("Peer {} indicated unexpected response code {} to IHAVE command.", connectedPeer.getLabel(), responseCode);
                                    }
                                } else {
                                    logger.warn("Peer {} is not connected properly. No (further) sharing of articles with peer.", f.getPeer().getLabel());
                                    break;
                                }
                            }
                            if (articlesToOffer.isEmpty()) {
                                // no unshared articles.
                                // update the lastSyncTime in the feed
                                f.setLastSyncTime(startOfSync);
                            }
                        } else {
                            logger.warn("Peer {} does not support IHAVE capability.", connectedPeer.getLabel());
                        }
                    } else {
                        logger.error("Peer {} is not connected properly. No sharing of articles with peer.", f.getPeer().getLabel());
                    }
                } else {
                    logger.info("No new articles to share with peer {} that the peer does not already have.", f.getPeer().getLabel());
                }
            } else {
                logger.info("No new articles in this newsgroup {} since last synced {} with peer {}", f.getNewsgroup().getName(), lastSyncTime, f.getPeer().getLabel());
            }
        }
    }

    /**
     * Get the messageIds of those articles that the peer has added to their newsgroup since the last time we synced with them, using the NEWNEWS command.
     * Requires the peer support the NEWNEWS capability.
     */
    protected List<Specification.MessageId> getNewMessageIds(Feed feed) {
        List<Specification.MessageId> result = new ArrayList<>();

        NetworkUtilities.ConnectedPeer connectedPeer = peerConnectionCache.getConnectedPeer(feed.getPeer());

        if (connectedPeer != null && connectedPeer.isConnected()) {
            if (connectedPeer.hasCapability(Specification.NNTP_Server_Capabilities.NEW_NEWS)) {

                // send command: NEWNEWS <newsgroupname> yyyyMMdd hhmmss GMT
                connectedPeer.sendCommand(
                        Specification.NNTP_Request_Commands.NEW_NEWS,
                        feed.getNewsgroup().getName().toString(),
                        DateAndTime.formatTo_yyyyMMdd_hhmmss((feed.getLastSyncTime() != null ? feed.getLastSyncTime() : DateAndTime.EPOCH)));

                try {
                    // check result
                    Specification.NNTP_Response_Code responseCode = connectedPeer.getResponseCode();
                    if (responseCode != null) {
                        if (Specification.NNTP_Response_Code.Code_230.equals(responseCode)) {// read the entire response from peer
                            String peerResponse = connectedPeer.readUntilDotLine();
                            // interpret as a list of messageIds
                            for (String line : peerResponse.split("\r\n")) {
                                if (!".".equals(line)) {
                                    // interpret line as a messageID
                                    try {
                                        Specification.MessageId m = new Specification.MessageId(line.trim());
                                        result.add(m);
                                    } catch (Specification.MessageId.InvalidMessageIdException e) {
                                        logger.error("Invalid MessageId in NEWNEWS response from peer {}: {}", connectedPeer.getAddress(), e.getMessage());
                                    }
                                }
                            }
                        } else {
                            logger.warn("Peer {} returned unexpected response code {} to NEWNEWS command.", connectedPeer.getAddress(), responseCode);
                        }
                    }
                } catch (IllegalArgumentException e) {
                    logger.error("Error reading NEWNEWS response from peer {}: {}", connectedPeer.getAddress(), e.getMessage());
                }
            } else {
                logger.warn("Peer {} does not support NEWNEWS.", connectedPeer.getAddress());
            }
        } else {
            logger.warn("Peer {} is not connected properly.  Skipping sync.", feed.getPeer().getLabel());
        }
        return result;
    }


    /**
     * Contact the peer to get its local data and time.  If the response is not a 211 code or if the result can not
     * be parsed, then return null.
     */
    protected LocalDateTime getTimeAndDateOnPeer(NetworkUtilities.ConnectedPeer peer) {
        if (peer == null) {
            logger.error("Peer should not be null in call to getTimeAndDateOnPeer()");
            return null;    // TODO this or RuntimeException?
        }

        if (peer.hasCapability(Specification.NNTP_Server_Capabilities.READER)) {

            // contact peer for its local date time
            peer.sendCommand(Specification.NNTP_Request_Commands.DATE);

            Specification.NNTP_Response_Code responseCode = peer.getResponseCode();
            if (Specification.NNTP_Response_Code.Code_111.equals(responseCode)) {
                return DateAndTime.parse_yyyMMddHHmmss(peer.readLine());
            } else {
                logger.warn("Peer {} returned unexpected response code {} to DATE command.", peer.getLabel(), responseCode);
            }

            // default value is unix epoch
            return DateAndTime.EPOCH;
        } else {
            logger.warn("Peer {} does not support DATE.", peer.getLabel());
        }
        return null;
    }

    /**
     * Asks the Peer for a list of Newsgroups it maintains.
     */
    protected ExternalNewsgroup[] getNewNewsgroupsList(NetworkUtilities.ConnectedPeer peer) {
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
        LocalDateTime lastFetched = peer.getListLastFetched();

        if (lastFetched == null) {
            lastFetched = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")); // Default to unix epoch if never fetched

            // send NewGroups command to Peer to get its list of Newsgroups, as per RFC-3977 format: NEWGROUPS yyyyMMdd hhmmss
            peer.sendCommand(Specification.NNTP_Request_Commands.NEW_GROUPS, DateAndTime.formatTo_yyyyMMdd_hhmmss(lastFetched));



            // read response from inputStream
            if (Specification.NNTP_Response_Code.Code_231.equals(peer.getResponseCode())) {
                // request accepted.  response follows

                // RFC-3977: Read multi-line response until a line containing only "." is found
                String response = peer.readUntilDotLine();
                // split response into lines
                for (String line : response.split(Specification.CRLF)) {
                    try {
                        // for each line in the response
                        String[] parts = line.split("\\s+");    // format: <groupname> <high> <low> <status>
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
            LocalDateTime peerServerTime = getTimeAndDateOnPeer(peer);
            if (peerServerTime == null) {
                peerServerTime = DateAndTime.EPOCH;     // in the worst case, just keep using unix epoch
            }
            peer.setListLastFetched(peerServerTime);

        }
        return newsgroups.toArray(new ExternalNewsgroup[0]);
    }
}

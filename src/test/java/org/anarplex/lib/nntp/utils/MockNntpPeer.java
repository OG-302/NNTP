package org.anarplex.lib.nntp.utils;

import org.anarplex.lib.nntp.PeerSynchronizer;
import org.anarplex.lib.nntp.Specification;
import org.anarplex.lib.nntp.env.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;


public class MockNntpPeer {

    private static final Logger logger = LoggerFactory.getLogger(MockNntpPeer.class);

    /**
     * Launches an NNTP peer.
     */
    public static void main(String[] args) {

        // Open port for listening
        PolicyService policyService = new MockPolicyService();
        NetworkUtilities networkUtilities = MockNetworkUtilities.getInstance(MockNetworkUtilities.networkEnv);
        PersistenceService persistenceService = new MockPersistenceService();
        persistenceService.init();

        PeerSynchronizer synchronizer = new PeerSynchronizer(persistenceService, policyService, networkUtilities);

        String[] newsgroups = {"test.local"};

        for (String g : newsgroups) {
            try {
                Specification.NewsgroupName name = new Specification.NewsgroupName(g);
                PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(name);

                if (newsgroup == null) {
                    try {
                        newsgroup = persistenceService.addGroup(name, "testing purposes", Specification.PostingMode.Allowed, DateAndTime.EPOCH, "Tester", false);
                    } catch (PersistenceService.ExistingNewsgroupException e) {
                        throw new RuntimeException(e);
                    }
                }
                // get the two peers
                PersistenceService.Peer peer1, peer2;
                Iterator<PersistenceService.Peer> peers = persistenceService.getPeers();
                if (peers == null || !peers.hasNext()) {
                    // add peers
                    try {
                        peer1 = persistenceService.addPeer("NNTP Peer1", "127.0.0.1:119");
                        peer2 = persistenceService.addPeer("NNTP Peer2", "127.0.0.1:2119");
                    } catch (PersistenceService.ExistingPeerException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    peer1 = peers.next();
                    peer2 = peers.next();
                }


                // check to see that the newsgroup has a feed defined
                PersistenceService.Feed[] feeds = newsgroup.getFeeds();
                if (feeds.length == 0) {
                    // add feeds (peer1 and peer2) to this newsgroup
                    try {
                        newsgroup.addFeed(peer1);
                        newsgroup.addFeed(peer2);
                    } catch (PersistenceService.Newsgroup.ExistingFeedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // sync the newsgroup with the peers
                synchronizer.syncNewsgroup(newsgroup);

            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                throw new RuntimeException(e);
            }
        }
        synchronizer.closeAllConnections();
        persistenceService.close();

        System.exit(0);
    }
}

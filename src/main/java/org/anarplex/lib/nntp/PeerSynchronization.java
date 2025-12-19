package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.env.NetworkUtils;
import org.anarplex.lib.nntp.env.PersistenceService;
import org.anarplex.lib.nntp.env.PersistenceService.Peer;
import org.anarplex.lib.nntp.env.PersistenceService.Feed;
import org.anarplex.lib.nntp.env.PersistenceService.Newsgroup;
import org.anarplex.lib.nntp.env.PolicyService;

public class PeerSynchronization {

    private PersistenceService persistenceService;
    private PolicyService policyService;
    private NetworkUtils networkUtils;

    public PeerSynchronization(PersistenceService persistenceService, PolicyService policyService, NetworkUtils networkUtils) {
        this.persistenceService = persistenceService;
        this.policyService = policyService;
        this.networkUtils = networkUtils;
    }


    /* fetchNewsgroupsList connects with the specified Peer and requests a list of all newsgroups created on that Peer
     * since last contacted by fetchNewsgroupsList(), or all its hosted newsgroups if this is the first time.
     * The PolicyServce.isNewsgroupAllowed() method is invoked on each such newsgroup encountered to determine whether
     * that newsgroup should be ignored or not.  Only if not marked Ignored will articles found posted in it be
     * retrieved during later syncNewsgroup() operations.
     * @param peer
     */
    public void fetchNewsgroupsList(Peer peer) {

    }

    /* syncNewsgroup takes the specified Newsgroup (already present in the PersistenceService) and updates it by
     * synchronizing with those Peers who are recorded in the PersistenceService as Feeds for this Newsgroup.
     * Synchronization with a Peer both 1) fetches new articles from that Peer and 2) shares new articles with that Peer.
     * This is achieved by the following algorithm.
     * For each Peer recorded as a Feed for this newsgroup, connect to the Peer and use the NEWNEWS capability to get a
     * list of MessageIds of new Articles.  If the Peer does not support NEWNEWS, then LISTGROUP is used.  Each article
     * so listed but not already found in the local PersistenceService is retrieved from the Peer.
     * In this way, an up-to-date list of Articles is built in the local PersistenceService.
     * Then each Peer recorded as a Feed for this newsgroup is again contacted (via IHAVE command) to see if it is
     * interested in those Articles that are locally present but not found on the Peer.  In this way, by the end of the
     * Sync, all Peers should have identical sets of Articles for this group.
     * The connections opened to Peers in the process are kept open and saved to an internal cache which can be emptied
     * via CloseAllConnections().
     * @param newsgroup
     */
    public void syncNewsgroup(Newsgroup newsgroup) {

    }
}

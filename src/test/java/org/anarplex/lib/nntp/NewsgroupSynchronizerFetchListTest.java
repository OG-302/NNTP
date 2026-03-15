package org.anarplex.lib.nntp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NewsgroupSynchronizer.fetchNewsgroupsList()
 */
public class NewsgroupSynchronizerFetchListTest {

    private MockPolicyService policyService;
    private SyncTestPersistence persistenceService;
    private NetworkService networkService;

    @BeforeEach
    void setup() {
        policyService = new MockPolicyService();
        persistenceService = new SyncTestPersistence();
        networkService = new NetworkService() {
            @Override
            public ConnectionListener registerService(ServiceProvider serviceProvider) { return null; }
            @Override
            ConnectedPeer connectToPeer(PersistenceService.Peer peer) { return null; }
        };
    }

    @Test
    @DisplayName("Null peer: no-op")
    void nullPeerNoop() {
        NewsgroupSynchronizer sync = new NewsgroupSynchronizer(persistenceService, policyService, networkService);
        sync.fetchNewsgroupsList(null);
        assertFalse(persistenceService.groupsExists(), "No groups should be created");
    }

    @Test
    @DisplayName("Disabled peer: no-op")
    void disabledPeerNoop() throws Exception {
        TestPeer peer = new TestPeer("news.example", "peerA");
        peer.setDisabledStatus(true);
        NetworkService.ConnectedPeer cp = newConnectedPeer(scriptWithCapabilitiesOnly(), peer);

        NewsgroupSynchronizer sync = new NewsgroupSynchronizer(persistenceService, policyService, networkService);
        sync.fetchNewsgroupsList(cp);
        assertFalse(persistenceService.groupsExists(), "No groups should be created for disabled peer");
    }

    @Test
    @DisplayName("Disconnected peer: no-op")
    void disconnectedPeerNoop() throws Exception {
        TestPeer peer = new TestPeer("news.example", "peerA");
        NetworkService.ConnectedPeer cp = newConnectedPeer(scriptWithCapabilitiesOnly(), peer);
        // close to simulate disconnected
        cp.close();

        NewsgroupSynchronizer sync = new NewsgroupSynchronizer(persistenceService, policyService, networkService);
        sync.fetchNewsgroupsList(cp);
        assertFalse(persistenceService.groupsExists(), "No groups should be created for disconnected peer");
    }

    @Test
    @DisplayName("Fetches descriptions, creators, highest numbers; creates/updates groups and feeds; updates lastFetched")
    void fetchesAndUpdatesGroups() throws Exception {
        TestPeer peer = new TestPeer("news.example", "peerA");
        // Minimal connected peer instance (not used by overrides beyond identity/connection checks)
        NetworkService.ConnectedPeer cp = newConnectedPeer(scriptWithCapabilitiesOnly(), peer);

        // Prepare scripted maps that the synchronizer will use (overrides avoid regex/path parsing)
        Map<Specification.NewsgroupName, String> desc = new HashMap<>();
        desc.put(new Specification.NewsgroupName("comp.test"), "description-of-comp");
        desc.put(new Specification.NewsgroupName("local.group"), "local description");

        Map<Specification.NewsgroupName, String> creators = new HashMap<>();
        creators.put(new Specification.NewsgroupName("comp.test"), "creatorA");
        creators.put(new Specification.NewsgroupName("local.group"), "creatorB");

        Map<Specification.NewsgroupName, Integer> highs = new HashMap<>();
        highs.put(new Specification.NewsgroupName("comp.test"), 42);
        highs.put(new Specification.NewsgroupName("new.unknown"), 5);

        Instant peerTime = Instant.parse("2026-01-01T12:34:56Z");

        NewsgroupSynchronizer sync = new TestSynchronizer(persistenceService, policyService, networkService, desc, creators, highs, peerTime);
        sync.fetchNewsgroupsList(cp);

        // comp.test should exist, with sanitized description (hyphens removed), creator set, and feed with highest=42
        Specification.NewsgroupName compName = new Specification.NewsgroupName("comp.test");
        PersistenceService.StoredNewsgroup comp = persistenceService.getNewsgroup(compName);
        assertNotNull(comp, "comp.test group should be created");
        assertEquals("descriptionofcomp", comp.getDescription(), "Description should be sanitized and set");
        assertEquals("creatorA", comp.getCreatedBy(), "CreatedBy should be set");

        SyncTestPersistence.TestFeed compFeed = persistenceService.findFeed(compName, peer);
        assertNotNull(compFeed, "Feed for comp.test should exist");
        assertNotNull(compFeed.getHighestArticleNumber(), "Highest article number should be set");
        assertEquals(42, compFeed.getHighestArticleNumber().getValue());

        // local.group should exist, description & creator set, feed exists (added during description loop), but highest may be null
        Specification.NewsgroupName localName = new Specification.NewsgroupName("local.group");
        PersistenceService.StoredNewsgroup local = persistenceService.getNewsgroup(localName);
        assertNotNull(local, "local.group should be created");
        assertEquals("local description", local.getDescription());
        assertEquals("creatorB", local.getCreatedBy());
        assertNotNull(persistenceService.findFeed(localName, peer), "Feed should exist even if no NEWGROUPS entry");

        // new.unknown should be created only from NEWGROUPS with a feed and highest=5, no description/creator
        Specification.NewsgroupName unkName = new Specification.NewsgroupName("new.unknown");
        PersistenceService.StoredNewsgroup unk = persistenceService.getNewsgroup(unkName);
        assertNotNull(unk, "new.unknown should be created from NEWGROUPS");
        assertNull(unk.getDescription(), "No description for unknown group");
        assertNull(unk.getCreatedBy(), "No creator for unknown group");
        SyncTestPersistence.TestFeed unkFeed = persistenceService.findFeed(unkName, peer);
        assertNotNull(unkFeed, "Feed for new.unknown should exist");
        assertNotNull(unkFeed.getHighestArticleNumber());
        assertEquals(5, unkFeed.getHighestArticleNumber().getValue());

        // peer list last fetched should be updated to the DATE value (2026-01-01T12:34:56Z)
        assertEquals(Instant.parse("2026-01-01T12:34:56Z"), peer.getListLastFetched());
    }

    // --- Helpers ---

    private String scriptWithCapabilitiesOnly() {
        return String.join("\n",
                "200 service ready",
                "101 ",
                "list",
                "reader",
                "."
        );
    }

    private static NetworkService.ConnectedPeer newConnectedPeer(String script, PersistenceService.Peer peer) {
        try {
            BufferedReader reader = new BufferedReader(new StringReader(script));
            BufferedWriter writer = new BufferedWriter(new StringWriter());
            return new NetworkService.ConnectedPeer(reader, writer, peer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- Test doubles for this suite ---

    /**
     * Minimal policy service used here as a stub.
     */
    public static class MockPolicyService implements PolicyService {
        @Override public boolean isPostingAllowedBy(IdentityService.Subject submitter) { return true; }
        @Override public boolean isIHaveTransferAllowedBy(IdentityService.Subject submitter) { return true; }
        @Override public ArticleReviewOutcome reviewArticle(Specification.Article article, Specification.ArticleSource articleSource, IdentityService.Subject sender) { return ArticleReviewOutcome.Allow; }
        @Override public void reviewPosting(PersistenceService.PendingArticle submission) { }
        @Override public void close() { }
    }

    /**
     * Simple in-memory persistence that supports groups and feeds for synchronizer tests.
     */
    public static class SyncTestPersistence extends InMemoryPersistence {
        private final Map<Specification.NewsgroupName, TestGroup> syncGroups = new LinkedHashMap<>();

        boolean groupsExists() { return !syncGroups.isEmpty(); }

        @Override
        protected StoredNewsgroup addNewsgroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, String createdBy) {
            TestGroup g = new TestGroup(name, description, postingMode, createdBy);
            syncGroups.put(name, g);
            return g;
        }

        @Override
        public StoredNewsgroup getNewsgroup(Specification.NewsgroupName name) { return syncGroups.get(name); }

        @Override
        public StoredNewsgroup getGroupByName(Specification.NewsgroupName name) { return syncGroups.get(name); }

        TestFeed findFeed(Specification.NewsgroupName name, PersistenceService.Peer peer) {
            TestGroup g = syncGroups.get(name);
            if (g == null) return null;
            return g.findFeed(peer);
        }

        class TestGroup extends InMemoryPersistence.TestPublishedNewsgroup {
            private final List<TestFeed> feeds = new ArrayList<>();

            protected TestGroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, String createdBy) {
                super(name, description, postingMode, createdBy);
            }

            @Override
            protected Feed addFeed(PersistenceService.Peer peer) {
                TestFeed existing = findFeed(peer);
                if (existing != null) return existing;
                TestFeed f = new TestFeed(this, peer);
                feeds.add(f);
                return f;
            }

            TestFeed findFeed(PersistenceService.Peer peer) {
                for (TestFeed f : feeds) {
                    if (f.getPeer() == peer) return f;
                }
                return null;
            }

            @Override
            public Feed[] getFeeds() { return feeds.toArray(new Feed[0]); }
        }

        static class TestFeed extends PersistenceService.Feed {
            private final TestGroup group;
            private final PersistenceService.Peer peer;
            private Instant lastPull;
            private Instant lastPush;
            private Specification.ArticleNumber highest;

            TestFeed(TestGroup group, PersistenceService.Peer peer) {
                this.group = group; this.peer = peer;
            }

            @Override
            protected Instant getLastPullSync() { return lastPull; }

            @Override
            protected void setLastPullSync(Instant time) { this.lastPull = time; }

            @Override
            protected Instant getLastPushSync() { return lastPush; }

            @Override
            protected void setLastPushSync(Instant time) { this.lastPush = time; }

            @Override
            public Specification.ArticleNumber getHighestArticleNumber() { return highest; }

            @Override
            protected void setHighestArticleNumber(int num) {
                try {
                    this.highest = new Specification.ArticleNumber(num);
                } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public PersistenceService.Peer getPeer() { return peer; }

            @Override
            PersistenceService.StoredNewsgroup getNewsgroup() { return group; }
        }
    }

    /**
     * Simple test Peer implementation.
     */
    public static class TestPeer extends PersistenceService.Peer {
        private final String address;
        private String label;
        private boolean disabled;
        private Instant lastFetched;

        public TestPeer(String address, String label) { this.address = address; this.label = label; }

        @Override public String getAddress() { return address; }
        @Override public boolean isDisabled() { return disabled; }
        @Override public void setDisabledStatus(boolean disabled) { this.disabled = disabled; }
        @Override public String getLabel() { return label; }
        @Override public void setLabel(String label) { this.label = label; }
        @Override public Instant getListLastFetched() { return lastFetched; }
        @Override protected void setListLastFetched(Instant lastFetched) { this.lastFetched = lastFetched; }
        @Override public String getPrincipal() { return label; }
        @Override public long getIdentifier() { return address.hashCode(); }
    }

    /**
     * Test subclass that bypasses on-the-wire parsing by returning scripted results
     * and applying the expected side-effect of updating listLastFetched.
     */
    static class TestSynchronizer extends NewsgroupSynchronizer {
        private final Map<Specification.NewsgroupName, String> descriptions;
        private final Map<Specification.NewsgroupName, String> creators;
        private final Map<Specification.NewsgroupName, Integer> highs;
        private final Instant peerTime;

        TestSynchronizer(PersistenceService ps, PolicyService pol, NetworkService ns,
                         Map<Specification.NewsgroupName, String> descriptions,
                         Map<Specification.NewsgroupName, String> creators,
                         Map<Specification.NewsgroupName, Integer> highs,
                         Instant peerTime) {
            super(ps, pol, ns);
            this.descriptions = descriptions;
            this.creators = creators;
            this.highs = highs;
            this.peerTime = peerTime;
        }

        @Override
        protected Map<Specification.NewsgroupName, String> getNewsgroupDescriptions(NetworkService.ConnectedPeer peer) {
            return descriptions;
        }

        @Override
        protected Map<Specification.NewsgroupName, String> getNewsgroupCreatedBy(NetworkService.ConnectedPeer peer) {
            return creators;
        }

        @Override
        protected Map<Specification.NewsgroupName, Integer> getHighestArticleNum(NetworkService.ConnectedPeer peer) {
            // Simulate side-effect of updating the peer's list last fetched time
            peer.setListLastFetched(peerTime);
            return highs;
        }
    }
}

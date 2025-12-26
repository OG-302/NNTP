package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.utils.DateAndTime;
import org.anarplex.lib.nntp.utils.RandomNumber;
import org.anarplex.lib.nntp.Specification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.*;

import static org.anarplex.lib.nntp.Specification.NNTP_Standard_Article_Headers;
import static org.junit.jupiter.api.Assertions.*;

class MockPersistenceServiceTest {

    // For testing purposes
    static final Specification.MessageId nonExistentMessageId;  // should not exist in database

    static {
        try {
            nonExistentMessageId = new Specification.MessageId("<.>");
        } catch (Specification.MessageId.InvalidMessageIdException e) {
            throw new RuntimeException(e);
        }
    }

    static final Specification.MessageId[] validMessageId = new Specification.MessageId[3];
    static final Specification.NewsgroupName[] validGroupName = new Specification.NewsgroupName[3];
    static final LocalDateTime startTime = LocalDateTime.now();
    static MockPersistenceService persistenceService;

    @BeforeAll
    static void initialiseDatabase() throws Specification.MessageId.InvalidMessageIdException, Specification.NewsgroupName.InvalidNewsgroupNameException, PersistenceService.ExistingNewsgroupException, Specification.Article.ArticleHeaders.InvalidArticleHeaderException, PersistenceService.Newsgroup.ExistingArticleException {
        assertNotNull(nonExistentMessageId);

        if (persistenceService == null) {
            persistenceService = new MockPersistenceService();

            persistenceService.init();

            // load database with known values
            String groupName = "local.tmp.test.nntp-lib.g" + RandomNumber.generate10DigitNumber();
            PersistenceService.Newsgroup[] newsgroups = new PersistenceService.Newsgroup[validGroupName.length];

            for (int i = 0; i < validGroupName.length; i++) {
                validGroupName[i] = new Specification.NewsgroupName(groupName + i);

                // create a known group based on ValidGroupName[]
                 newsgroups[i] = persistenceService.addGroup(validGroupName[i],
                        "created for testing purposes",
                        Specification.PostingMode.Allowed,
                        startTime,
                        "test case",
                        false);
            }

            // create some known articles
            for (int i = 0; i < validMessageId.length; i++) {
                validMessageId[i] = new Specification.MessageId("<"+ RandomNumber.generate10DigitNumber()+">");
            }

            Map<String, Set<String>> headers = new HashMap<>();
            headers.put(NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<.>"));
            headers.put(NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("test subject"));
            headers.put(NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("me"));
            headers.put(NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("1 Jan 1970 00:00:01"));
            // headers.put(NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("100"));
            // headers.put(NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("1000"));
            headers.put(NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("path.to.article"));


            for (Specification.MessageId messageId : validMessageId) {
                String body = "This is a test article.\nLine 2.\nLine 3.";
                headers.put(NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(newsgroups[0].getName().getValue()));
                headers.put(NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(messageId.toString()));
                Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(headers);
                newsgroups[0].addArticle(messageId, articleHeaders, body, false);
            }
        }
    }


    @Test
    void hasArticle() {
        boolean result = persistenceService.hasArticle(nonExistentMessageId);
        assertFalse(result);

        result = persistenceService.hasArticle(validMessageId[0]);
        assertTrue(result);
    }

    @Test
    void getArticle() {
        Specification.Article a = persistenceService.getArticle(nonExistentMessageId);
        assertNull(a);
    }

    @Test
    void addArticle() throws Specification.MessageId.InvalidMessageIdException, Specification.Article.ArticleHeaders.InvalidArticleHeaderException, PersistenceService.Newsgroup.ExistingArticleException {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        assertNotNull(newsgroup);

        // Create a unique message ID for this test
        Specification.MessageId newMessageId = new Specification.MessageId("<" + RandomNumber.generate10DigitNumber() + "@test.article>");
        assertNotNull(newMessageId);

        // Create required headers for the article
        Map<String, Set<String>> headers = new HashMap<>();
        headers.put(NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(newMessageId.toString()));
        headers.put(NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(validGroupName[1].getValue()));
        headers.put(NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("Test Article Subject"));
        headers.put(NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("test@example.com"));
        headers.put(NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("1 Jan 2025 12:00:00 GMT"));
        headers.put(NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("test.path"));
        headers.put(NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<parent@test>"));
        headers.put(NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("3"));
        headers.put(NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("42"));

        // Create article headers
        Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(headers);
        assertNotNull(articleHeaders);

        // Create article body
        String bodyContent = "This is a test article.\nLine 2.\nLine 3.";

        // Get the current metrics before adding
        PersistenceService.NewsgroupMetrics metricsBefore = newsgroup.getMetrics();
        int articleCountBefore = metricsBefore.getNumberOfArticles();
        int highestArticleNumBefore = metricsBefore.getHighestArticleNumber().getValue();

        // Add the article to the newsgroup
        PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(newMessageId, articleHeaders, bodyContent, false);

        // Verify the article was added successfully
        assertNotNull(newsgroupArticle);
        assertEquals(newMessageId, newsgroupArticle.getMessageId());
        assertEquals(newsgroup.getName(), newsgroupArticle.getNewsgroup().getName());
        assertEquals(highestArticleNumBefore + 1, newsgroupArticle.getArticleNumber().getValue());

        // Verify the article can be retrieved
        Specification.Article retrievedArticle = persistenceService.getArticle(newMessageId);
        assertNotNull(retrievedArticle);
        assertEquals(newMessageId, retrievedArticle.getMessageId());

        // Verify headers were persisted correctly
        Specification.Article.ArticleHeaders retrievedHeaders = retrievedArticle.getAllHeaders();
        assertNotNull(retrievedHeaders);

        Set<String> subjectHeader = retrievedHeaders.getHeaderValue(NNTP_Standard_Article_Headers.Subject.getValue());
        assertNotNull(subjectHeader);
        assertTrue(subjectHeader.contains("Test Article Subject"));

        Set<String> fromHeader = retrievedHeaders.getHeaderValue(NNTP_Standard_Article_Headers.From.getValue());
        assertNotNull(fromHeader);
        assertTrue(fromHeader.contains("test@example.com"));

        // Verify the article body was persisted correctly
        String body2 = retrievedArticle.getBody();
        assertFalse(body2.isEmpty());
        assertEquals(bodyContent, body2);

        // Verify metrics updated
        PersistenceService.NewsgroupMetrics metricsAfter = newsgroup.getMetrics();
        assertEquals(articleCountBefore + 1, metricsAfter.getNumberOfArticles());
        assertEquals(highestArticleNumBefore + 1, metricsAfter.getHighestArticleNumber().getValue());

        // Verify the article exists
        assertTrue(persistenceService.hasArticle(newMessageId));

        // Verify the article is not rejected
        assertFalse(persistenceService.isRejectedArticle(newMessageId));
    }

    @Test
    void getArticleIdsAfter() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        Iterator<PersistenceService.NewsgroupArticle> i = newsgroup.getArticlesSince(DateAndTime.EPOCH);

        assertNotNull(i);
        assertTrue(i.hasNext());
        PersistenceService.NewsgroupArticle a = i.next();
        assertNotNull(a);
        assertEquals(validMessageId[0], a.getMessageId());
        assertEquals(1, a.getArticleNumber().getValue());
        assertEquals(validGroupName[0], a.getNewsgroup().getName());
    }

    @Test
    void rejectArticle() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        PersistenceService.NewsgroupArticle a = newsgroup.getFirstArticle();

        assertNotNull(a);
    }

    @Test
    void isRejectedArticle() {
        // TODO
    }

    @Test
    void addGroup() throws Specification.NewsgroupName.InvalidNewsgroupNameException, PersistenceService.ExistingNewsgroupException {
        final String groupNameTestSuffix = "_addGroupTest";

        // add groups, one for each group Permission type
        for (int i = 0; i < validGroupName.length; i++) {
            Specification.NewsgroupName n;
            PersistenceService.Newsgroup newsgroup = persistenceService.addGroup(
                    n = new Specification.NewsgroupName( validGroupName[i].getValue() + groupNameTestSuffix),
                    "test group number " + i + " for this test exercise",
                    Specification.PostingMode.values()[i % Specification.PostingMode.values().length],
                    LocalDateTime.now(),
                    "test user",
                    false);
            assertNotNull(newsgroup);
            assertEquals(n.getValue(), newsgroup.getName().getValue());
            assertEquals(Specification.PostingMode.values()[i % Specification.PostingMode.values().length], newsgroup.getPostingMode());
        }
    }

    @Test
    void listAllGroups() {
        Iterator<PersistenceService.Newsgroup> i;
        PersistenceService.Newsgroup g;

        // use hasNext() to check if there are more groups
        i = persistenceService.listAllGroups(false, false);
        assertNotNull(i);
        assertTrue(i.hasNext());

        while (i.hasNext()) {
            g = i.next();
            assertNotNull(g);
            assertNotNull(g.getName());
        }
    }

    @Test
    void listAllGroupsAddedSince() {
        Iterator<PersistenceService.Newsgroup> i;
        PersistenceService.Newsgroup g;

        i = persistenceService.listAllGroupsAddedSince(DateAndTime.EPOCH);
        assertNotNull(i);
        assertTrue(i.hasNext());
        while (i.hasNext()) {
            g = i.next();
            assertNotNull(g);
            assertNotNull(g.getName());
        }

        // use a time in the future to check that no groups are returned
        i = persistenceService.listAllGroupsAddedSince(LocalDateTime.now().plusSeconds(10));  // ten seconds in the future
        assertNotNull(i);
        assertFalse(i.hasNext());

        // use a time in the future to check that no groups are returned
        i = persistenceService.listAllGroupsAddedSince(LocalDateTime.now().plusSeconds(10));  // ten seconds in the future
        assertNotNull(i);
        assertFalse(i.hasNext());
    }

    @Test
    void getGroupByName() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        assertNotNull(newsgroup);
        assertEquals(validGroupName[0], newsgroup.getName());

        PersistenceService.NewsgroupMetrics metrics =  newsgroup.getMetrics();
        assertNotNull(metrics);
        assertTrue(metrics.getNumberOfArticles() > 0);
        if (metrics.getNumberOfArticles() > 0) {
            assertTrue(metrics.getLowestArticleNumber().getValue() <= metrics.getHighestArticleNumber().getValue());
        } else {
            assertEquals(0, metrics.getLowestArticleNumber().getValue());
            assertEquals(-1, metrics.getHighestArticleNumber().getValue());
        }
    }

    @Test
    void addPeer() throws PersistenceService.ExistingPeerException {
        String address = "127.0.0.1";
        String label = "local.tmp.test.peer1."+System.currentTimeMillis();
        PersistenceService.Peer peer = persistenceService.addPeer(label, address);

        assertNotNull(peer);
        assertEquals(label, peer.getLabel());
        assertEquals(address, peer.getAddress());
    }

    @Test
    void removePeer() throws PersistenceService.ExistingPeerException {
        String address = "127.0.0.2";
        String label = "local.tmp.test.peer2."+System.currentTimeMillis();
        PersistenceService.Peer peer = persistenceService.addPeer(label, address);

        assertNotNull(peer);
        assertEquals(label, peer.getLabel());
        assertEquals(address, peer.getAddress());

        persistenceService.removePeer(peer);

        Iterator<PersistenceService.Peer> i = persistenceService.getPeers();

        assertNotNull(i);

        while (i.hasNext()) {
            PersistenceService.Peer p = i.next();
            assertNotNull(p);
            assertNotEquals(peer, p);
        }
    }

    @Test
    void getPeers() {
        Iterator<PersistenceService.Peer> i = persistenceService.getPeers();

        assertNotNull(i);

        while (i.hasNext()) {
            PersistenceService.Peer p = i.next();
            assertNotNull(p);
        }
    }


    @Test
    void getArticleInGroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        assertNotNull(newsgroup);

        Specification.ArticleNumber n = newsgroup.getArticle(validMessageId[0]);
        assertNotNull(n);
        assertEquals(1, n.getValue());
    }

    @Test
    void getArticleNumbered() throws Specification.ArticleNumber.InvalidArticleNumberException {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        assertNotNull(newsgroup);

        // jump to middle of group
        PersistenceService.NewsgroupArticle n = newsgroup.getArticleNumbered(new Specification.ArticleNumber(2));
        assertNotNull(n);
        assertEquals(validMessageId[1], n.getMessageId());
        assertEquals(2, n.getArticleNumber().getValue());
        assertEquals(validGroupName[0], n.getNewsgroup().getName());
    }


    @Test
    void gotoNextArticleInGroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        assertNotNull(newsgroup);

        PersistenceService.NewsgroupArticle a = newsgroup.getFirstArticle();
        assertNotNull(a);
        assertEquals(1, a.getArticleNumber().getValue());
        assertEquals(validMessageId[0], a.getMessageId());

        PersistenceService.NewsgroupArticle b = newsgroup.getNextArticle(a.getArticleNumber());
        assertNotNull(b);   // now current is on the second article
        assertTrue(a.getArticleNumber().getValue() < b.getArticleNumber().getValue());
        assertEquals(validMessageId[1], b.getMessageId());

        PersistenceService.NewsgroupArticle c = newsgroup.getNextArticle(b.getArticleNumber());
        assertNotNull(c);   // now current is on the third article
        assertTrue(b.getArticleNumber().getValue() < c.getArticleNumber().getValue());
        assertEquals(validMessageId[2], c.getMessageId());

        c = newsgroup.getNextArticle(c.getArticleNumber());
        assertNull(c);  // because there is no next article
    }

    @Test
    void gotoPreviousArticleInGroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[0]);
        assertNotNull(newsgroup);

        PersistenceService.NewsgroupArticle a = newsgroup.getFirstArticle();
        assertNotNull(a);
        assertEquals(1, a.getArticleNumber().getValue());
        assertEquals(validMessageId[0], a.getMessageId());

        PersistenceService.NewsgroupArticle b = newsgroup.getPreviousArticle(a.getArticleNumber());
        assertNull(b);  // because there is no previous article

        b = newsgroup.getNextArticle(a.getArticleNumber());
        assertNotNull(b);
        assertTrue(a.getArticleNumber().getValue() < b.getArticleNumber().getValue());
        assertEquals(validMessageId[1], b.getMessageId());

        a = newsgroup.getPreviousArticle(b.getArticleNumber());
        assertNotNull(a);
        assertEquals(1, a.getArticleNumber().getValue());
        assertEquals(validMessageId[0], a.getMessageId());
    }


    @Test
    void setIgnoredOnNewsgroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);

        newsgroup.setIgnored(true);
        assertTrue(newsgroup.isIgnored());

        newsgroup.setIgnored(false);
        assertFalse(newsgroup.isIgnored());
    }

    @Test
    void setPostingModeOnNewsgroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);

        for (Specification.PostingMode mode : Specification.PostingMode.values()) {
            newsgroup.setPostingMode(mode);
            assertEquals(mode, newsgroup.getPostingMode());
        }

        // leave it's mode as Allowed
        newsgroup.setPostingMode(Specification.PostingMode.Allowed);
    }

    @Test
    void articleHeadersPersistence() throws PersistenceService.Newsgroup.ExistingArticleException {
        // create headers to be persisted
        Map<String, Set<String>> headers = new HashMap<>();
        long rn = RandomNumber.generate10DigitNumber();
        String testHeaderName = "testHeader-" + rn;
        Specification.MessageId mId = null;
        try {
            mId = new Specification.MessageId("<" + RandomNumber.generate10DigitNumber() + ">");
        } catch (Specification.MessageId.InvalidMessageIdException e) {
            assertNull(e, "Invalid message ID exception should not be thrown");
        }
        assertNotNull(mId);

        String testGroupString1 = validGroupName[1].getValue();
        String testGroupString2 = validGroupName[2].getValue();
        headers.put(NNTP_Standard_Article_Headers.Newsgroups.getValue(), Collections.singleton(testGroupString1+","+testGroupString2));
        headers.put(NNTP_Standard_Article_Headers.MessageID.getValue(), Collections.singleton(mId.getValue()));

        Specification.Article.ArticleHeaders articleHeaders = null;
        try {
            articleHeaders = new Specification.Article.ArticleHeaders(headers);
        } catch (Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
            // invalid header should happen here because the header was missing a few standard entries
            assertNotNull(e, "Invalid article header exception should be thrown");
        }


        headers.put(NNTP_Standard_Article_Headers.References.getValue(), Collections.singleton("<.>"));
        headers.put(NNTP_Standard_Article_Headers.Subject.getValue(), Collections.singleton("test subject"));
        headers.put(NNTP_Standard_Article_Headers.From.getValue(), Collections.singleton("me"));
        headers.put(NNTP_Standard_Article_Headers.Date.getValue(), Collections.singleton("1 Jan 1970 00:00:00"));
        headers.put(NNTP_Standard_Article_Headers.Lines.getValue(), Collections.singleton("100"));
        headers.put(NNTP_Standard_Article_Headers.Bytes.getValue(), Collections.singleton("1000"));
        headers.put(NNTP_Standard_Article_Headers.Path.getValue(), Collections.singleton("path.to.article"));

        try {
            articleHeaders = new Specification.Article.ArticleHeaders(headers);
        } catch (Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
            assertNull(e, "Invalid article header exception should not be thrown");
        }
        assertNotNull(articleHeaders);  // all standard headers have been defined now

        // add a few more custom headers
        headers.put(testHeaderName, new HashSet<>(Arrays.asList("<th1>;<th2>", "<th3>", "<th4>")));
        try {
            articleHeaders = new Specification.Article.ArticleHeaders(headers);
        } catch (Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
            assertNull(e, "Invalid article header exception should not be thrown");
        }
        assertNotNull(articleHeaders);

        // create a new article and persist
        PersistenceService.Newsgroup n = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(n);


        PersistenceService.NewsgroupArticle na = n.addArticle(mId, articleHeaders, "body of article", false);
        assertNotNull(na);

        // retrieve the article and check its contents
        Specification.Article ar = persistenceService.getArticle(mId);

        assertNotNull(ar);
        assertEquals(mId, ar.getMessageId());

        articleHeaders = ar.getAllHeaders();
        assertNotNull(articleHeaders);

        // check that all the standard headers are present
        for (NNTP_Standard_Article_Headers standardHeader : NNTP_Standard_Article_Headers.values()) {
            if (standardHeader.isMandatory()) {
                assertNotNull(articleHeaders.getHeaderValue(standardHeader.getValue()));
            }
        }

        Set<String> headerValues;

        headerValues = articleHeaders.getHeaderValue(NNTP_Standard_Article_Headers.Newsgroups.getValue());
        assertNotNull(headerValues);
        assertTrue(headerValues.contains(testGroupString1));
        assertTrue(headerValues.contains(testGroupString2));

        headerValues = articleHeaders.getHeaderValue(testHeaderName);
        assertNotNull(headerValues);
        assertTrue(headerValues.contains("<th1>;<th2>"));
        assertTrue(headerValues.contains("<th3>"));
        assertTrue(headerValues.contains("<th4>"));

        headerValues = articleHeaders.getHeaderValue(NNTP_Standard_Article_Headers.Bytes.getValue());
        assertNotNull(headerValues);
        assertTrue(headerValues.contains("1000"));
    }

    @AfterAll
    static void cleanup() {
        MockPersistenceService persistenceService = new MockPersistenceService();
        persistenceService.init();
        persistenceService.close();
    }
}
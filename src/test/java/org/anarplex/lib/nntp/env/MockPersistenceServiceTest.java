package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.utils.RandomNumber;
import org.anarplex.lib.nntp.Specification;
import org.apache.logging.log4j.core.util.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    static final Date startTime = new Date();
    static MockPersistenceService persistenceService;

    @BeforeAll
    static void initialiseDatabase() throws Specification.MessageId.InvalidMessageIdException, Specification.NewsgroupName.InvalidNewsgroupNameException {
        assertNotNull(nonExistentMessageId);

        if (persistenceService == null) {
            persistenceService = new MockPersistenceService();

            persistenceService.init();

            // load database with known values
            for (int i = 0; i < validMessageId.length; i++) {
                validMessageId[i] = new Specification.MessageId("<"+ RandomNumber.generate10DigitNumber()+">");
            }

            String groupName = "local.tmp.test.nntp-lib.g" + RandomNumber.generate10DigitNumber();
            for (int i = 0; i < validGroupName.length; i++) {
                validGroupName[i] = new Specification.NewsgroupName(groupName + i);
            }

            // create some known articles
            int[] existingArticlePK = new int[validMessageId.length];
            for (int i = 0; i < validMessageId.length; i++) {
                existingArticlePK[i] = -1;

                try (PreparedStatement ps = persistenceService.dbConnection.prepareStatement(
                        "insert into PUBLIC.NN_ARTICLES (messageID, insertionTime) values (?, ?)",
                        Statement.RETURN_GENERATED_KEYS)) {
                    ps.setString(1, validMessageId[i].toString());
                    ps.setLong(2, startTime.getTime()/1000);    // seconds since epoch
                    ps.executeUpdate();

                    ResultSet rs = ps.getGeneratedKeys();
                    if (rs.next()) {
                        existingArticlePK[i] = rs.getInt(1);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                assertNotEquals(-1, existingArticlePK[i]);
            }


            // create a known group based on ValidGroupName[1]
            int existingGroupPK = -1;
            try (PreparedStatement ps = persistenceService.dbConnection.prepareStatement(
                    "insert into PUBLIC.NN_NEWSGROUPS (name, description, insertionTime) values (?, 'created for testing purposes', ?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, validGroupName[1].getValue());
                ps.setLong(2, startTime.getTime()/1000);    // seconds since epoch
                ps.executeUpdate();

                ResultSet rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    existingGroupPK = rs.getInt(1);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            assertNotEquals(-1, existingGroupPK);

            // ensure the known articles are part of the known group
            for (int i = 0; i < validMessageId.length; i++) {
                try (PreparedStatement ps = persistenceService.dbConnection.prepareStatement(
                        "insert into PUBLIC.NN_NEWSGROUP_ARTICLES (articleNum, newsgroupPK, articlePK) values (?,?,?)")) {
                    ps.setInt(1, i+1);
                    ps.setInt(2, existingGroupPK);
                    ps.setInt(3, existingArticlePK[i]);
                    int rows = ps.executeUpdate();

                    assertEquals(1, rows);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            // delete the supposedly non-existent Message with ID = <.> if exists
            try (PreparedStatement ps = persistenceService.dbConnection.prepareStatement(
                    "DELETE from PUBLIC.NN_ARTICLES WHERE messageID = ?")) {
                ps.setString(1, nonExistentMessageId.toString());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            // clean out the NN_NEWSGROUPSarticles table of non-existent groups and articles
            // delete the supposedly non-existent Message with ID = <.> if exists
            try {
                persistenceService.dbConnection.createStatement().execute(
                        "DELETE from PUBLIC.NN_NEWSGROUP_ARTICLES WHERE newsgroupPK NOT IN (SELECT newsgroupPK FROM PUBLIC.NN_NEWSGROUPS)");
            } catch (SQLException e) {
                throw new RuntimeException(e);
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
        PersistenceService.Article a = persistenceService.getArticle(nonExistentMessageId);
        assertNull(a);
    }

    @Test
    void addArticle() throws Specification.MessageId.InvalidMessageIdException, Specification.Article.ArticleHeaders.InvalidArticleHeaderException, PersistenceService.Newsgroup.ExistingArticleException {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
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
        StringReader bodyReader = new StringReader(bodyContent);
        
        // Get the current metrics before adding
        PersistenceService.NewsgroupMetrics metricsBefore = newsgroup.getMetrics();
        int articleCountBefore = metricsBefore.getNumberOfArticles();
        int highestArticleNumBefore = metricsBefore.getHighestArticleNumber().getValue();
        
        // Add the article to the newsgroup
        PersistenceService.NewsgroupArticle newsgroupArticle = newsgroup.addArticle(newMessageId, articleHeaders, bodyReader, false);
        
        // Verify the article was added successfully
        assertNotNull(newsgroupArticle);
        assertEquals(newMessageId, newsgroupArticle.getMessageId());
        assertEquals(newsgroup.getName(), newsgroupArticle.getNewsgroup().getName());
        assertEquals(highestArticleNumBefore + 1, newsgroupArticle.getArticleNumber().getValue());
        
        // Verify the article can be retrieved
        PersistenceService.Article retrievedArticle = persistenceService.getArticle(newMessageId);
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
        Reader bodyReader2 = retrievedArticle.getBody();
        assertNotNull(bodyReader2);
        try {
            assertEquals(bodyContent, IOUtils.toString(bodyReader2));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        Iterator<PersistenceService.NewsgroupArticle> i = newsgroup.getArticlesSince(new Date(0));

        assertNotNull(i);
        assertTrue(i.hasNext());
        PersistenceService.NewsgroupArticle a = i.next();
        assertNotNull(a);
        assertEquals(validMessageId[0], a.getMessageId());
        assertEquals(1, a.getArticleNumber().getValue());
        assertEquals(validGroupName[1], a.getNewsgroup().getName());
    }

    @Test
    void rejectArticle() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        PersistenceService.NewsgroupArticle a = newsgroup.getCurrentArticle();

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
                    new Date(),
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
        assertNull(i.next());

        while ((g = i.next()) != null) {
            assertNotNull(g.getName());
        }
        assertFalse(i.hasNext());
    }

    @Test
    void listAllGroupsAddedSince() {
        Iterator<PersistenceService.Newsgroup> i;
        PersistenceService.Newsgroup g;

        i = persistenceService.listAllGroupsAddedSince(new Date(1));
        assertNotNull(i);
        assertTrue(i.hasNext());
        while (i.hasNext()) {
            g = i.next();
            assertNotNull(g);
            assertNotNull(g.getName());
        }
        assertNull(i.next());
        assertFalse(i.hasNext());

        // use a time in the future to check that no groups are returned
        i = persistenceService.listAllGroupsAddedSince(new Date(startTime.getTime() + 10000));  // ten seconds in the future
        assertNotNull(i);
        assertNull(i.next());
        assertFalse(i.hasNext());

        // use a time in the future to check that no groups are returned
        i = persistenceService.listAllGroupsAddedSince(new Date(startTime.getTime() + 10000));  // ten seconds in the future
        assertNotNull(i);
        assertFalse(i.hasNext());
        assertNull(i.next());
    }

    @Test
    void getGroupByName() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);
        assertEquals(validGroupName[1], newsgroup.getName());

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
    void addPeer() {
        String address = "127.0.0.1";
        String label = "local.tmp.test.peer1."+System.currentTimeMillis();
        PersistenceService.Peer peer = persistenceService.addPeer(label, address);

        assertNotNull(peer);
        assertEquals(label, peer.getLabel());
        assertEquals(address, peer.getAddress());
    }

    @Test
    void removePeer() {
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
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);

        Specification.ArticleNumber n = newsgroup.getArticle(validMessageId[1]);
        assertNotNull(n);
        assertEquals(2, n.getValue());
    }

    @Test
    void getArticleNumbered() throws Specification.ArticleNumber.InvalidArticleNumberException {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);

        // jump to middle of group
        PersistenceService.NewsgroupArticle n = newsgroup.getArticleNumbered(new Specification.ArticleNumber(2));
        assertNotNull(n);
        assertEquals(validMessageId[1], n.getMessageId());
        assertEquals(2, n.getArticleNumber().getValue());
        assertEquals(validGroupName[1], n.getNewsgroup().getName());
    }

    @Test
    void getCurrentArticleInGroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);

        // current is always first article
        PersistenceService.NewsgroupArticle a = newsgroup.getCurrentArticle();
        assertNotNull(a);
        assertEquals(1, a.getArticleNumber().getValue());
        assertEquals(validMessageId[0], a.getMessageId());
        assertEquals(validGroupName[1], a.getNewsgroup().getName());
    }

    @Test
    void gotoNextArticleInGroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);

        PersistenceService.NewsgroupArticle a = newsgroup.getCurrentArticle();
        assertNotNull(a); // current is always first article
        assertEquals(1, a.getArticleNumber().getValue());
        assertEquals(validMessageId[0], a.getMessageId());

        a = newsgroup.gotoNextArticle();
        assertNotNull(a);   // now current is on second article
        assertNotNull(newsgroup.getCurrentArticle());
        assertEquals(2, a.getArticleNumber().getValue());
        assertEquals(validMessageId[1], newsgroup.getCurrentArticle().getMessageId());

        a = newsgroup.gotoNextArticle();
        assertNotNull(a);   // now current is on third article
        assertNotNull(newsgroup.getCurrentArticle());
        assertEquals(3, a.getArticleNumber().getValue());
        assertEquals(validMessageId[2], a.getMessageId());

        a = newsgroup.gotoNextArticle();
        assertNull(a);  // because there is no next article

        assertNotNull(newsgroup.getCurrentArticle());   // but current should still be on third article
        assertEquals(3, newsgroup.getCurrentArticle().getArticleNumber().getValue());
        assertEquals(validMessageId[2], newsgroup.getCurrentArticle().getMessageId());
    }

    @Test
    void gotoPreviousArticleInGroup() {
        PersistenceService.Newsgroup newsgroup = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(newsgroup);

        PersistenceService.NewsgroupArticle a = newsgroup.getCurrentArticle();
        assertNotNull(a);   // current is always first article
        assertEquals(1, a.getArticleNumber().getValue());

        a = newsgroup.gotoPreviousArticle();
        assertNull(a);  // because there is no previous article

        a = newsgroup.getCurrentArticle();
        assertNotNull(a); // but current still be on first article
        assertEquals(1, a.getArticleNumber().getValue());

        a = newsgroup.gotoNextArticle();
        assertNotNull(a);   // now current is on second article
        assertEquals(2, a.getArticleNumber().getValue());

        a = newsgroup.gotoNextArticle();
        assertNotNull(a);   // now current is on third article
        assertEquals(3, a.getArticleNumber().getValue());

        a = newsgroup.gotoPreviousArticle();
        assertNotNull(a);   // now current is on second article
        assertEquals(2, newsgroup.getCurrentArticle().getArticleNumber().getValue());
        assertEquals(validMessageId[1], newsgroup.getCurrentArticle().getMessageId());
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
        Map<String, Set<String>> headers = new HashMap<String, Set<String>>();
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
        headers.put(testHeaderName, new HashSet<String>(Arrays.asList("<th1>;<th2>", "<th3>", "<th4>")));
        try {
            articleHeaders = new Specification.Article.ArticleHeaders(headers);
        } catch (Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
            assertNull(e, "Invalid article header exception should not be thrown");
        }
        assertNotNull(articleHeaders);

        // create a new article and persist
        PersistenceService.Newsgroup n = persistenceService.getGroupByName(validGroupName[1]);
        assertNotNull(n);


        PersistenceService.NewsgroupArticle na = n.addArticle(mId, articleHeaders, new StringReader("body of article"), false);
        assertNotNull(na);

        // retrieve the article and check its contents
        MockPersistenceService.ArticleRecord ar = persistenceService.getArticle(mId);

        assertNotNull(ar);
        assertEquals(mId, ar.messageId);

        articleHeaders = ar.getAllHeaders();
        assertNotNull(articleHeaders);

        // check that all the standard headers are present
        for (NNTP_Standard_Article_Headers standardHeader : NNTP_Standard_Article_Headers.values()) {
            if (!standardHeader.isOptional()) {
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
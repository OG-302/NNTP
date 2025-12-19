package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;
import org.anarplex.lib.nntp.utils.ResultSetIterator;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.core.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * An SQL (HSQLDB) implementation of the PersistenceService interface.
 */
public class MockPersistenceService implements PersistenceService, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MockPersistenceService.class);

    private static final String dbName = "dataSources/nntp-db";
    private static final String dbPath = "jdbc:hsqldb:file:";
    private static final String url = dbPath + dbName;

    protected Connection dbConnection = null;

    private static final String db_schema =
        "create table if not exists PUBLIC.NN_ARTICLES (" +
        "   PK            INTEGER identity primary key, " +
        "   INSERTIONTIME BIGINT                not null, " +
        "   MESSAGEID     VARCHAR(250)          not null constraint NN_ARTICLES__MSGID unique, " +
        "   MESSAGE       CLOB, " +
        "   REJECTED      BOOLEAN default FALSE not null" +
        ");" +

        "create table if not exists PUBLIC.NN_ARTICLE_HEADER_KEYS (" +
        "   PK   INTEGER identity primary key, " +
        "   NAME VARCHAR(1024) not null constraint NN_ARTICLE_HEADER_KEYS__NAME unique" +
        ");" +

        "create table if not exists PUBLIC.NN_ARTICLE_HEADER_VALUES (" +
        "   ARTICLEPK       INTEGER       not null, " +
        "   ARTICLEHEADERPK INTEGER       not null, " +
        "   HEADERVALUE     VARCHAR(4096) not null " +
        ");" +

        "create index if not exists PUBLIC.NN_ARTICLE_HEADER_VALUES_ARTICLEPK_ARTICLEHEADERPK_INDEX "+
        "   on PUBLIC.NN_ARTICLE_HEADER_VALUES (ARTICLEPK, ARTICLEHEADERPK);" +

        "create table if not exists PUBLIC.NN_NEWSGROUPS (" +
        "   PK              INTEGER identity primary key," +
        "   NAME            VARCHAR(2048)         not null, " +
        "   INSERTIONTIME   INTEGER default 0     not null, " +
        "   DESCRIPTION     VARCHAR(4096), " +
        "   CREATEDAT       INTEGER, " +
        "   CREATEDBY       VARCHAR(2048), " +
        "   IGNORED         BOOLEAN default FALSE not null, " +
        "   POSTINGALLOWED  INTEGER default 0     not null " +
        ");" +

        "create table if not exists PUBLIC.NN_NEWSGROUP_ARTICLES (" +
        "   ARTICLENUM      INTEGER not null, " +
        "   NEWSGROUPPK     INTEGER not null, " +
        "   ARTICLEPK       INTEGER not null, " +
        "   constraint NN_NEWSGROUPARTICLES_PK primary key (ARTICLENUM, NEWSGROUPPK), " +
        "   constraint NN_NEWSGROUPARTICLES_NEWSGROUPPK_ARTICLEPK_UINDEX unique (NEWSGROUPPK, ARTICLEPK) " +
        ");" +

        "create table if not exists PUBLIC.NN_PEERS (" +
        "   PK                 INTEGER identity primary key, " +
        "   ADDRESS            VARCHAR(1024), " +
        "   LISTLASTFETCHED    INTEGER, " +
        "   CONNECTIONPRIORITY INTEGER default 0, " +
        "   DISABLED           BOOLEAN default FALSE not null, " +
        "   LABEL              VARCHAR(1024) " +
        ");" +

        "create table if not exists PUBLIC.NN_NEWSFEEDS (" +
        "   PK                 INTEGER identity primary key, " +
        "   PEERPK             INTEGER not null constraint NN_NEWSFEEDS_NN_PEERS_PK_FK references PUBLIC.NN_PEERS," +
        "   LASTSYNCTIME       INTEGER, " +
        "   LASTSYNCARTICLENUM INTEGER, " +
        "   NEWGROUPPK         INTEGER not null constraint NN_NEWSFEEDS_NN_NEWSGROUPS_PK_FK references PUBLIC.NN_NEWSGROUPS" +
        ");";




    /**
     * Default constructor.
     */
    public MockPersistenceService() {
        init();
    }

    private static class HsqldbConnectionPool {
        private static final BasicDataSource dataSource;

        static {
            dataSource = new BasicDataSource();
            dataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
            dataSource.setUrl(url);
            // dataSource.setUsername("SA");
            // dataSource.setPassword("");
            dataSource.setMinIdle(1);
            dataSource.setMaxIdle(3);
            dataSource.setMaxTotal(10);
            dataSource.setInitialSize(3);
            dataSource.setTestOnBorrow(true);
            dataSource.setValidationQuery("select 1 from INFORMATION_SCHEMA.SYSTEM_USERS");
        }

        public static BasicDataSource getDataSource() {
            return dataSource;
        }
    }

    public void init() {
        try {
            if (dbConnection == null || dbConnection.isClosed() || !dbConnection.isValid(2)) {
                    if (dbConnection != null && !dbConnection.isClosed()) {
                        dbConnection.close();
                    }
                    dbConnection = HsqldbConnectionPool.getDataSource().getConnection();
                    dbConnection.isValid(1);
                    logger.debug("Connected to database: {}", dbName);

                    checkSchema();
                    garbageCollection();
            }

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            dbConnection = null;
        }
    }

    @Override
    public void commit() {
        try {
            dbConnection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkSchema() {
        logger.debug("Creating database schema: {}", dbName);
        try (Statement stmt = dbConnection.createStatement()) {
            for (String sql : db_schema.split(";")) {
                if (!sql.trim().isEmpty()) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * garbageCollect first deletes unaccessible data, and then runs a VACUUM on the database.
     */
    private void garbageCollection() {
        logger.debug("Cleaning out stale data in database: {}", dbName);

        // delete all test data.  this accumulates in local.tmp.test
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("DELETE FROM NN_NEWSGROUPS where name like 'local.tmp.test%'");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("DELETE FROM NN_PEERS where label like 'local.tmp.test.peer%'");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        // delete the associations of articles to newsgroups when that newsgroup has been deleted (occurs often with testing)
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("DELETE FROM NN_NEWSGROUP_ARTICLES where newsgroupPK NOT IN (SELECT pk FROM NN_NEWSGROUPS);");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        // delete articles not found in any newsgroup (occurs often with testing)
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("DELETE FROM NN_ARTICLES where pk NOT IN (SELECT articlePK FROM NN_NEWSGROUP_ARTICLES);");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        // delete headerValues not belonging to any article (occurs often with testing)
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("DELETE FROM NN_ARTICLE_HEADER_VALUES where articlePK NOT IN (SELECT PK FROM NN_ARTICLES);");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        // delete headerKeys not belonging to any headerValue (occurs often with testing)
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("DELETE FROM NN_ARTICLE_HEADER_KEYS where pk NOT IN (SELECT articleHeaderPK FROM NN_ARTICLE_HEADER_VALUES);");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

    }

    public void close() {
        try {
            if (dbConnection != null) {
                dbConnection.close();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }


    public static class ArticleRecord extends Specification.Article implements Article {
        final private Connection dbConnection;
        final int articlePK;
        final Specification.MessageId messageId;
        private ArticleHeaders headerValues;

        ArticleRecord(Connection dbConnection, int articlePK, String messageId) throws Specification.MessageId.InvalidMessageIdException {
            this.dbConnection = dbConnection;
            this.articlePK = articlePK;
            this.messageId = new Specification.MessageId(messageId);
        }


        /* Calling the constructor is forbidden.  New instances are returned from various methods. */
        private ArticleRecord() {
            throw new IllegalStateException("Not used");
        }

        @Override
        public Specification.MessageId getMessageId() {
            return messageId;
        }

        @Override
        public ArticleHeaders getAllHeaders() {
            if (headerValues == null) {
                try {
                    headerValues = new ArticleHeaders(fetchHeaders(dbConnection, articlePK));
                } catch (ArticleHeaders.InvalidArticleHeaderException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            return headerValues;
        }

        private static Map<String, Set<String>> fetchHeaders(Connection dbConnection, int articlePK) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "select name, headerValue " +
                            " from NN_ARTICLE_HEADER_KEYS, NN_ARTICLE_HEADER_VALUES " +
                            " where articlePK = ? " +
                            "  and NN_ARTICLE_HEADER_KEYS.pk = NN_ARTICLE_HEADER_VALUES.articleHeaderPK ")) {

                ps.setInt(1, articlePK);
                ResultSet rs = ps.executeQuery();

                Map<String, Set<String>> headerMap = new HashMap<>();
                while (rs.next()) {
                    Set<String> values = headerMap.computeIfAbsent(rs.getString("name"), k -> new HashSet<>());
                    values.add(rs.getString("headerValue"));
                }
                return headerMap;
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }


        @Override
        public Reader getBody() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT message FROM NN_ARTICLES WHERE pk = ?")) {

                ps.setInt(1, articlePK);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    String message = rs.getString(1);
                    if (!rs.wasNull() && message != null) {
                        return new java.io.StringReader(message);
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            return null;
        }


        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof ArticleRecord that)) return false;

            return messageId != null && messageId.equals(that.messageId);
        }

        @Override
        public int hashCode() {
            return messageId == null ? 0 : messageId.hashCode();
        }
    }


    /**
     * hasArticle determines whether the specified message id exists in the database.
     *
     * @param messageId
     * @return true if the message id exists, false otherwise
     */
    @Override
    public boolean hasArticle(Specification.MessageId messageId) {
        try (PreparedStatement ps = dbConnection.prepareStatement(
                "SELECT COUNT(*) FROM NN_ARTICLES WHERE messageID = ? LIMIT 1")) {

            ps.setString(1, messageId.toString());

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * getArticle returns the article with the specified article or null if it does not exist.
     *
     * @param messageId
     * @return the article with the specified article or null if it does not exist
     */
    @Override
    public ArticleRecord getArticle(Specification.MessageId messageId) {
        try (PreparedStatement ps = dbConnection.prepareStatement(
                "SELECT pk, messageID FROM NN_ARTICLES WHERE messageID = ?")) {

            ps.setString(1, messageId.toString());

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new ArticleRecord(dbConnection, rs.getInt("pk"), rs.getString("messageID"));
            }
        } catch (SQLException | Specification.MessageId.InvalidMessageIdException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * getArticleIdsAfter returns an iterator over all message ids that were added after the specified time.
     *
     * @param after
     * @return an iterator over all message ids that were added after the specified time
     */
    @Override
    public Iterator<Specification.MessageId> getArticleIdsAfter(Date after) {
        try {
            PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT messageID FROM NN_ARTICLES WHERE insertionTime > ? ORDER BY insertionTime");
            ps.setLong(1, after.getTime() / 1000);  // seconds since epoch
            ResultSet rs = ps.executeQuery();

            return new ResultSetIterator<Specification.MessageId>(rs, (ResultSetIterator.ResultSetMapper<Specification.MessageId>) resultSet -> {
                    try {
                        return new Specification.MessageId(rs.getString("messageID"));
                    } catch (Specification.MessageId.InvalidMessageIdException | SQLException e) {
                        logger.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
            }
            );

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * rejectArticle marks the specified message id as rejected.
     *
     * @param messageId
     */
    @Override
    public void rejectArticle(Specification.MessageId messageId) {
        Article a = getArticle(messageId);
        if (a != null) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "UPDATE NN_ARTICLES SET rejected = true WHERE messageID = ?")) {
                ps.setString(1, messageId.toString());
                ps.executeUpdate();
                return;
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        } else {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "INSERT INTO NN_ARTICLES (messageID, insertionTime, rejected) VALUES (?, ?, true)")) {
                ps.setString(1, messageId.toString());
                ps.setLong(2, new Date().getTime() / 1000);
                ps.executeUpdate();
                return;
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * isRejectedArticle returns true if the specified message id has been marked as rejected.
     *
     * @param messageId
     * @return true if the specified message id has been marked as rejected, false otherwise, null if no such message id exists.
     */
    @Override
    public Boolean isRejectedArticle(Specification.MessageId messageId) {
        try (PreparedStatement ps = dbConnection.prepareStatement(
                "SELECT rejected FROM NN_ARTICLES WHERE messageID = ?")) {
            ps.setString(1, messageId.toString());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * addGroup adds a new newsgroup to the database or returns null if one with that name already exists.
     *
     * @param name
     * @param description
     * @param postingMode
     * @param createdAt
     * @param createdBy
     * @param toBeIgnored
     * @return the newly created Newsgroup or null if the newsgroup already exists
     */
    @Override
    public Newsgroup addGroup(Specification.NewsgroupName name, String description, Specification.PostingMode postingMode, Date createdAt, String createdBy, boolean toBeIgnored)
        throws ExistingNewsgroupException
    {
        try (PreparedStatement ps = dbConnection.prepareStatement(
                "INSERT INTO NN_NEWSGROUPS (name, description, postingAllowed, insertionTime, createdAt, createdBy, ignored) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS)) {
            // first check to see if the newsgroup already exists
            if (getGroupByName(name) != null) {
                throw new ExistingNewsgroupException(">" + name.getValue() + "< already exists in the database.");
            } else {
                // add a new newsgroup to the database
                ps.setString(1, name.getValue());
                ps.setString(2, description);
                ps.setInt(3, postingMode.getValue());
                ps.setLong(4, new Date().getTime() / 1000); // insertionTime
                ps.setLong(5, createdAt.getTime() / 1000);
                ps.setString(6, createdBy);
                ps.setBoolean(7, toBeIgnored);

                ps.executeUpdate();
                ResultSet rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    int pk = rs.getInt(1);
                    if (!rs.wasNull()) {
                        return new NewsgroupRecord(dbConnection,
                                pk,
                                name.getValue(),
                                description,
                                createdAt.getTime() / 1000,
                                createdBy);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * @param subscribedOnly
     * @param includeIgnored
     * @return
     */
    @Override
    public Iterator<Newsgroup> listAllGroups(boolean subscribedOnly, boolean includeIgnored) {
        try {
            PreparedStatement ps;
            if (subscribedOnly) {
                // only newsgroups that are subscribed to by at least one peer are returned
                ps = dbConnection.prepareStatement(
                        "SELECT NN_NEWSGROUPS.pk, name, description, createdAt, createdBy " +
                                " FROM NN_NEWSGROUPS, NN_NEWSFEEDS, NN_PEERS " +
                                " WHERE (ignored IS NULL OR ignored = FALSE OR ignored = ?) " +
                                " AND 0 < (SELECT COUNT(newsgroupPK) FROM NN_NEWSFEEDS, NN_PEERS WHERE NN_NEWSFEEDS.newsgroupPK = NN_NEWSGROUPS.pk AND NN_NEWSFEEDS.peerPK == NN_PEERS.pk AND NN_PEERS.disabled = FALSE LIMIT 1)  " +
                                " ORDER BY insertionTime DESC");
            } else {
                // all newsgroups are returned
                ps = dbConnection.prepareStatement(
                        "SELECT NN_NEWSGROUPS.pk, name, description, createdAt, createdBy " +
                                " FROM NN_NEWSGROUPS " +
                                " WHERE (ignored IS NULL OR ignored = FALSE OR ignored = ?) ORDER BY insertionTime DESC");
            }

            ps.setBoolean(1, includeIgnored);
            ResultSet rs = ps.executeQuery();

            return new ResultSetIterator<Newsgroup>(rs, (ResultSetIterator.ResultSetMapper<Newsgroup>) getNewsgroupResultSetMapper());

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * @param insertedTime
     * @return
     */
    @Override
    public Iterator<Newsgroup> listAllGroupsAddedSince(Date insertedTime) {
        try {
            PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT * FROM NN_NEWSGROUPS WHERE insertionTime >= ? AND ignored != TRUE ORDER BY insertionTime");
            ps.setLong(1, insertedTime.getTime() / 1000);   // seconds since epoch
            ResultSet rs = ps.executeQuery();

            return new ResultSetIterator<>(rs, getNewsgroupResultSetMapper());
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private ResultSetIterator.ResultSetMapper<Newsgroup> getNewsgroupResultSetMapper() {
        return resultSet ->
        {
            try {
                return new NewsgroupRecord(dbConnection,
                        resultSet.getInt("pk"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getLong("createdAt"),
                        resultSet.getString("createdBy"));
            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException | SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        };
    }

    /**
     * @param name
     * @return
     */
    @Override
    public Newsgroup getGroupByName(Specification.NewsgroupName name) {
        try (PreparedStatement ps = dbConnection.prepareStatement("SELECT * FROM NN_NEWSGROUPS WHERE name = ?")) {
            ps.setString(1, name.getValue());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new NewsgroupRecord(
                        dbConnection,
                        rs.getInt("pk"),
                        rs.getString("name"),
                        rs.getString("description"),
                        rs.getLong("createdAt"),
                        rs.getString("createdBy"));
            }
        } catch (SQLException | Specification.NewsgroupName.InvalidNewsgroupNameException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * addPeer adds a new Peer to the database.
     *
     * @param label
     * @param address
     * @return the newly created Peer
     */
    @Override
    public Peer addPeer(String label, String address) {
        try (PreparedStatement ps = dbConnection.prepareStatement(
                "INSERT INTO NN_PEERS (label, address) VALUES (?, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, label);
            ps.setString(2, address);
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs != null && rs.next()) {
                return new PeerRecord(dbConnection, rs.getInt(1), label, address);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * RemovePeer removes the specified Peer from the database.  As a consequence, its also REMOVES all Feeds
     * associated with this Peer and the associated state (such as the last sync time for each newsgroup feed, etc.).
     * Consider using Peer.SetDisabled(true) if you simply do not want the Peer to be interrogated during syncs.
     *
     * @param peer
     */
    @Override
    public void removePeer(Peer peer) {
        try (PreparedStatement psFeeds = dbConnection.prepareStatement(
                "DELETE FROM NN_NEWSFEEDS WHERE peerPK = ?");
             PreparedStatement psPeers = dbConnection.prepareStatement(
                "DELETE FROM NN_PEERS WHERE pk = ?")) {

            // Delete associated feeds first (foreign key constraint)
            psFeeds.setInt(1, peer.getPk());
            psFeeds.executeUpdate();

            // Then delete the peer
            psPeers.setInt(1, peer.getPk());
            psPeers.executeUpdate();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * getPeers returns an iterator over all Peers in the database.
     *
     * @return an iterator over all Peers in the database.
     */
    @Override
    public Iterator<Peer> getPeers() {
        try {
            PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT pk, label, address FROM NN_PEERS ORDER BY pk");
            ResultSet rs = ps.executeQuery();
            return new ResultSetIterator<Peer>(rs, (ResultSetIterator.ResultSetMapper<Peer>) resultSet ->
            {
                try {
                    return new PeerRecord(dbConnection,
                            rs.getInt("pk"),
                            rs.getString("label"),
                            rs.getString("address"));
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            );
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


    public static class NewsgroupRecord implements Newsgroup {
        private final Connection dbConnection;
        private final int pk;
        private final Specification.NewsgroupName name;
        private final String description;
        private final Date createdAt;
        private final String createdBy;
        private Specification.ArticleNumber currentArticleNumber;

        NewsgroupRecord(Connection dbConnection, int pk, String name, String description, Long createdAt, String createdBy) throws Specification.NewsgroupName.InvalidNewsgroupNameException {
            this.dbConnection = dbConnection;
            this.pk = pk;
            this.name = new Specification.NewsgroupName(name);
            this.description = description;
            this.createdAt = (createdAt != null ? new Date(createdAt) : null);
            this.createdBy = createdBy;

            // set the current Article Number to the first article number of the newsgroup.
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT MIN(articleNum)  FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ?")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    int n = rs.getInt(1);
                    currentArticleNumber = !rs.wasNull() ? new Specification.ArticleNumber(n) : null;
                }
            } catch (SQLException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Specification.NewsgroupName getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public Specification.PostingMode getPostingMode() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT postingAllowed FROM NN_NEWSGROUPS WHERE pk = ?")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return Specification.PostingMode.valueOf(rs.getInt(1));
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public void setPostingMode(Specification.PostingMode postingMode) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "UPDATE NN_NEWSGROUPS SET postingAllowed = ? WHERE pk = ?")) {
                ps.setInt(1, postingMode.ordinal());
                ps.setInt(2, pk);
                ps.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Date getCreatedAt() {
            return createdAt;
        }

        @Override
        public String getCreatedBy() {
            return createdBy;
        }

        @Override
        public NewsgroupMetrics getMetrics() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT Count(*), MIN(articleNum), MAX(articleNum) from NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ?")) {

                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    int numArticles = rs.getInt(1);
                    if (numArticles > 0) {
                        return new NewsgroupMetrics(  // actual values
                                numArticles,
                                new Specification.ArticleNumber(rs.getInt(2)),
                                new Specification.ArticleNumber(rs.getInt(3))
                        );
                    } else {
                        // no articles in this newsgroup.  send back default values
                        return new NewsgroupMetrics(0,
                                Specification.NoArticlesLowestNumber.getInstance(),
                                Specification.NoArticlesHighestNumber.getInstance());
                    }
                }
            } catch (SQLException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            return new NewsgroupMetrics();  // default values
        }


        public static class NewsgroupMetrics implements PersistenceService.NewsgroupMetrics {
            private final int numberOfArticles;
            private final Specification.ArticleNumber lowestArticleNumber;
            private final Specification.ArticleNumber highestArticleNumber;

            private NewsgroupMetrics(int numberOfArticles, Specification.ArticleNumber lowestArticleNumber, Specification.ArticleNumber highestArticleNumber) {
                this.numberOfArticles = numberOfArticles;
                this.lowestArticleNumber = lowestArticleNumber;
                this.highestArticleNumber = highestArticleNumber;
            }

            private NewsgroupMetrics() {
                this.numberOfArticles = 0;
                this.lowestArticleNumber = null;
                this.highestArticleNumber = null;
            }

            @Override
            public int getNumberOfArticles() {
                return numberOfArticles;
            }

            @Override
            public Specification.ArticleNumber getLowestArticleNumber() {
                return lowestArticleNumber;
            }

            @Override
            public Specification.ArticleNumber getHighestArticleNumber() {
                return highestArticleNumber;
            }
        }

        @Override
        public boolean isIgnored() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT ignored FROM NN_NEWSGROUPS where pk = ?")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
                throw new RuntimeException("Failed to locate newsgroup in database: " + pk);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void setIgnored(boolean isIgnored) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "UPDATE NN_NEWSGROUPS SET ignored = ? WHERE pk = ?")) {
                ps.setBoolean(1, isIgnored);
                ps.setInt(2, pk);
                ps.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Feed addFeed(Peer peer) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "INSERT INTO NN_NEWSFEEDS (peerPK, newsgroupPK) VALUES (?, ?)",
                    Statement.RETURN_GENERATED_KEYS)) {

                ps.setInt(1, peer.getPk());
                ps.setInt(2, pk);
                ps.executeUpdate();

                // Retrieve the generated keys
                ResultSet rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    int generatedId = rs.getInt(1);
                    if (rs.wasNull()) {
                        throw new RuntimeException("Failed to insert newsfeed in database: " + peer.getPk());
                    }
                    return new FeedRecord(dbConnection, generatedId, peer);
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public Feed[] getFeeds() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT NN_NEWSFEEDS.pk, peerPK, label, address FROM NN_NEWSFEEDS, NN_PEERS " +
                            " WHERE newsgroupPK = ? AND peerPK = NN_PEERS.pk ORDER BY connectionPriority desc")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();

                List<FeedRecord> feeds = new ArrayList<>();
                while (rs.next()) {
                    feeds.add(new FeedRecord(
                            dbConnection,
                            rs.getInt("pk"),
                            new PeerRecord(
                                    dbConnection,
                                    rs.getInt("peerPK"),
                                    rs.getString("label"),
                                    rs.getString("address"))));
                }

                return feeds.toArray(new FeedRecord[feeds.toArray().length]);

            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public NewsgroupArticle addArticle(Specification.MessageId messageId, Specification.Article.ArticleHeaders headers, Reader body, boolean isRejected) {

            try (PreparedStatement insertArticle = dbConnection.prepareStatement(
                    "INSERT INTO NN_ARTICLES (insertionTime, messageID, message, rejected) VALUES (?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
                 PreparedStatement incrementArticleNumber = dbConnection.prepareStatement(
                         "INSERT INTO NN_NEWSGROUP_ARTICLES (newsgroupPK, articlePK, articleNum) " +
                                 " VALUES (?, ?, (SELECT coalesce(Max(articleNum), 0) +1 FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ?))");
                 PreparedStatement getArticleNumber = dbConnection.prepareStatement(
                         "SELECT Max(articleNum) FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ? AND articlePK = ?");
                 PreparedStatement queryHeaderName = dbConnection.prepareStatement(
                         "SELECT pk FROM NN_ARTICLE_HEADER_KEYS WHERE name = ?");
                 PreparedStatement insertNewHeaderName = dbConnection.prepareStatement(
                         "INSERT INTO NN_ARTICLE_HEADER_KEYS (name) VALUES (?)",
                         Statement.RETURN_GENERATED_KEYS);
                 PreparedStatement insertHeaderValue = dbConnection.prepareStatement(
                         "INSERT INTO NN_ARTICLE_HEADER_VALUES (articlePK, articleHeaderPK, headerValue) VALUES (?, ?, ?)");
            ) {
                ResultSet rs;

                // this operation will update several SQL Tables
                dbConnection.setAutoCommit(false);

                insertArticle.setLong(1, new Date().getTime() / 1000);
                insertArticle.setString(2, messageId.toString());
                insertArticle.setString(3, IOUtils.toString(body));
                insertArticle.setBoolean(4, isRejected);
                insertArticle.executeUpdate();

                rs = insertArticle.getGeneratedKeys();
                if (!rs.next()) {
                    throw new RuntimeException("Failed to insert article in database: " + messageId);
                }

                int articlePK = rs.getInt(1);
                if (rs.wasNull()) {
                    throw new RuntimeException("Failed to insert article in database: " + messageId);
                }

                // store article's header fields

                // for each header key, store the header key and values
                for (Map.Entry<String, Set<String>> header : headers.entrySet()) {
                    String headerKey = header.getKey();
                    Set<String> headerValues = header.getValue();

                    // check if header key exists
                    queryHeaderName.setString(1, headerKey);
                    ResultSet headerNameQueryResults = queryHeaderName.executeQuery();
                    int headerKeyPK;

                    if (!headerNameQueryResults.next()) {
                        // insert new header key
                        insertNewHeaderName.setString(1, headerKey);
                        insertNewHeaderName.executeUpdate();
                        ResultSet headerNameInsertResults = insertNewHeaderName.getGeneratedKeys();
                        if (!headerNameInsertResults.next()) {
                            throw new RuntimeException("Failed to insert header key");
                        }
                        headerKeyPK = headerNameInsertResults.getInt(1);
                    } else {
                        // header key already exists
                        headerKeyPK = headerNameQueryResults.getInt("pk");
                    }

                    // insert header values
                    for (String value : headerValues) {
                        insertHeaderValue.setInt(1, articlePK);
                        insertHeaderValue.setInt(2, headerKeyPK);
                        insertHeaderValue.setString(3, value);
                        insertHeaderValue.executeUpdate();
                    }
                }

                // enough data to form the article record
                Specification.Article article = new ArticleRecord(dbConnection, articlePK, messageId.toString());

                // include the article in this newsgroup
                Specification.ArticleNumber articleNumber = registerArticle(messageId, this);

                // save all the changes
                dbConnection.commit();

                return new NewsgroupArticleRecord(
                        new ArticleRecord(dbConnection, articlePK, messageId.toString()),
                        articleNumber,
                        this);

            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                try {
                    dbConnection.rollback();
                } catch (SQLException _) {
                }
                throw new RuntimeException(e);
            } catch (IOException | Specification.MessageId.InvalidMessageIdException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Specification.ArticleNumber includeArticle(NewsgroupArticle article) {
            return registerArticle(article.getMessageId(), article.getNewsgroup());
        }

        /**
         * This method is private because adding an Article to a Newsgroup based just on messageId would be incorrect
         * if the article had not already been entered into the database.  See addArticle() or includeArticle().
         *
         * @param messageId
         * @param newsgroup
         * @return
         */
        private Specification.ArticleNumber registerArticle(Specification.MessageId messageId, PersistenceService.Newsgroup newsgroup) {
            try (
                PreparedStatement incrementArticleNumber = dbConnection.prepareStatement(
                    "INSERT INTO NN_NEWSGROUP_ARTICLES (newsgroupPK, articlePK, articleNum) " +
                            " VALUES (?, (SELECT PK FROM NN_ARTICLES WHERE messageID = ?), " +
                            "(SELECT coalesce(Max(articleNum), 0) +1 FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ?)); ");
                PreparedStatement getArticleNumber = dbConnection.prepareStatement(
                        "SELECT articleNum FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ? and articlePK = (SELECT PK FROM NN_ARTICLES WHERE messageID = ?)");
            ) {
                // add this article to the current newsgroup by allocating a new articleNumber for it
                incrementArticleNumber.setInt(1, pk);   // newsgroupPK
                incrementArticleNumber.setString(2, messageId.toString());
                incrementArticleNumber.setInt(3, pk);   // newsgroupPK
                incrementArticleNumber.executeUpdate();

                getArticleNumber.setInt(1, pk);     // newsgroupPK
                getArticleNumber.setString(2, messageId.toString());
                ResultSet rs = getArticleNumber.executeQuery();
                if (!rs.next()) {
                    throw new RuntimeException("Failed to locate article in database: " + messageId);
                } else {
                    int articleNumber = rs.getInt(1);
                    if (rs.wasNull()) {
                        throw new RuntimeException("Failed to locate article in database: " + messageId);
                    } else {
                        return new Specification.ArticleNumber(articleNumber);
                    }
                }

            } catch (SQLException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Specification.ArticleNumber getArticle(Specification.MessageId messageId) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT articleNum FROM NN_NEWSGROUP_ARTICLES, NN_ARTICLES WHERE newsgroupPK = ? AND messageID = ? and articlePK = PK limit 1")) {
                ps.setInt(1, pk);
                ps.setString(2, messageId.toString());
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    // change the current article number to the one we just found
                    return currentArticleNumber = new Specification.ArticleNumber(rs.getInt("articleNum"));
                }
                return null;
            } catch (SQLException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public NewsgroupArticle getArticleNumbered(Specification.ArticleNumber articleNumber) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT articlePK, messageID  FROM NN_NEWSGROUP_ARTICLES, NN_ARTICLES WHERE newsgroupPK = ? AND articleNum = ? and articlePK = PK limit 1")) {
                ps.setInt(1, pk);
                ps.setInt(2, articleNumber.getValue());
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    ArticleRecord article = new ArticleRecord(dbConnection, rs.getInt("articlePK"), rs.getString("messageID"));
                    // change the current article number to the one we just found
                    currentArticleNumber = articleNumber;
                    return new NewsgroupArticleRecord(article, articleNumber, this);
                }
                return null;    // no such article exists.
            } catch (SQLException | Specification.MessageId.InvalidMessageIdException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<NewsgroupArticle> getArticlesNumbered(Specification.ArticleNumber lowerBound, Specification.ArticleNumber upperBound) {
            try {
                PreparedStatement ps = dbConnection.prepareStatement(
                        "SELECT NN_ARTICLES.pk, messageID, articleNum  FROM NN_ARTICLES, NN_NEWSGROUP_ARTICLES, NN_NEWSGROUPS " +
                                "WHERE articlePK = NN_ARTICLES.pk AND newsgroupPK = NN_NEWSGROUPS.pk AND newsgroupPK = ? AND ? <= articleNum AND articleNum <= ? ORDER BY articleNum");

                ps.setInt(1, pk);
                ps.setInt(2, lowerBound.getValue());
                ps.setInt(3, upperBound.getValue());
                ResultSet rs = ps.executeQuery();

                return new ResultSetIterator<NewsgroupArticle>(rs, (ResultSetIterator.ResultSetMapper<NewsgroupArticle>) resultSet ->
                {
                    try {
                        return new NewsgroupArticleRecord(
                                new ArticleRecord(dbConnection,
                                        resultSet.getInt("pk"),
                                        resultSet.getString("messageID")),
                                new Specification.ArticleNumber(resultSet.getInt("articleNum")),
                                NewsgroupRecord.this);
                    } catch (Specification.ArticleNumber.InvalidArticleNumberException |
                             Specification.MessageId.InvalidMessageIdException |
                             SQLException e) {
                        logger.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
                );
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<NewsgroupArticle> getArticlesSince(Date insertionTime) {
            try {
                PreparedStatement ps = dbConnection.prepareStatement(
                        "SELECT NN_ARTICLES.pk, messageID, articleNum  FROM NN_ARTICLES, NN_NEWSGROUP_ARTICLES, NN_NEWSGROUPS " +
                                "WHERE articlePK = NN_ARTICLES.pk AND newsgroupPK = NN_NEWSGROUPS.pk AND newsgroupPK = ? AND NN_ARTICLES.insertionTime >= ?");

                ps.setInt(1, pk);
                ps.setLong(2, insertionTime.getTime());
                ResultSet rs = ps.executeQuery();

                return new ResultSetIterator<NewsgroupArticle>(rs, (ResultSetIterator.ResultSetMapper<NewsgroupArticle>) resultSet ->
                {
                    try {
                        return new NewsgroupArticleRecord(
                                new ArticleRecord(dbConnection,
                                        resultSet.getInt("pk"),
                                        resultSet.getString("messageID")),
                                        new Specification.ArticleNumber(
                                            resultSet.getInt("articleNum")),
                                NewsgroupRecord.this);
                    } catch (Specification.ArticleNumber.InvalidArticleNumberException |
                             Specification.MessageId.InvalidMessageIdException |
                             SQLException e) {
                        logger.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
                );
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public NewsgroupArticle getCurrentArticle() {
            return (currentArticleNumber != null) ? getArticleNumbered(currentArticleNumber) : null;
        }

        @Override
        public NewsgroupArticle gotoNextArticle() {
            if (currentArticleNumber != null) {
                try (PreparedStatement ps = dbConnection.prepareStatement(
                        "SELECT MIN(articleNum) FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ? AND articleNum > ?")) {
                    ps.setInt(1, pk);
                    ps.setInt(2, currentArticleNumber.getValue());
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        int n = rs.getInt(1);
                        if (!rs.wasNull()) {
                            currentArticleNumber = new Specification.ArticleNumber(rs.getInt(1)); // only move the currentArticleNumber if there is a next article.
                            return getArticleNumbered(currentArticleNumber);
                        }
                    }
                } catch (SQLException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            return null;
        }

        @Override
        public NewsgroupArticle gotoPreviousArticle() {
            if (currentArticleNumber != null) {
                try (PreparedStatement ps = dbConnection.prepareStatement(
                        "SELECT MAX(articleNum) FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ? AND articleNum < ?")) {
                    ps.setInt(1, pk);
                    ps.setInt(2, currentArticleNumber.getValue());
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        int n = rs.getInt(1);
                        if (!rs.wasNull()) {
                            currentArticleNumber = new Specification.ArticleNumber(n); // only move the currentArticleNumber if there is a previous article.
                            return getArticleNumbered(currentArticleNumber);
                        }
                    }
                } catch (SQLException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            return null;
        }

        @Override
        public NewsgroupArticle gotoArticleWithNumber(Specification.ArticleNumber articleNumber) {
            if (currentArticleNumber != null) {
                try (PreparedStatement ps = dbConnection.prepareStatement(
                        "SELECT COUNT(*) FROM NN_NEWSGROUP_ARTICLES WHERE newsgroupPK = ? AND articleNum = ? LIMIT 1")) {
                    ps.setInt(1, pk);
                    ps.setInt(2, articleNumber.getValue());
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        if (rs.getInt(1) > 0) {
                            currentArticleNumber = articleNumber;
                            return getArticleNumbered(currentArticleNumber);
                        }
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            return null;
        }
    }

    public static class NewsgroupArticleRecord implements NewsgroupArticle {
        private final ArticleRecord article;
        private final Specification.ArticleNumber articleNumber;
        private final Newsgroup newsgroup;


        NewsgroupArticleRecord(ArticleRecord article, Specification.ArticleNumber articleNumber, Newsgroup newsgroup) {
            this.article = article;
            this.articleNumber = articleNumber;
            this.newsgroup = newsgroup;
        }

        @Override
        public Specification.ArticleNumber getArticleNumber() {
            return articleNumber;
        }

        @Override
        public Specification.MessageId getMessageId() {
            return article.getMessageId();
        }

        @Override
        public Article getArticle() {
            return article;
        }

        @Override
        public Newsgroup getNewsgroup() {
            return newsgroup;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof NewsgroupArticleRecord that)) return false;

            return article.messageId.equals(that.article.messageId) && articleNumber.equals(that.articleNumber) && newsgroup.equals(that.newsgroup);
        }

        @Override
        public int hashCode() {
            int result = article.messageId.hashCode();
            result = 31 * result + articleNumber.hashCode();
            result = 31 * result + newsgroup.hashCode();
            return result;
        }
    }

    public static class FeedRecord implements Feed {
        private final Connection dbConnection;
        private final int pk;
        private final Peer peer;

        protected FeedRecord(Connection dbConnection, int pk, Peer peer) {
            this.dbConnection = dbConnection;
            this.pk = pk;
            this.peer = peer;
        }

        @Override
        public Specification.ArticleNumber getLastSyncArticleNum() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT lastSyncArticleNum FROM NN_NEWSFEEDS WHERE pk = ?")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    int articleNum = rs.getInt(1);
                    if (!rs.wasNull()) {
                        return new Specification.ArticleNumber(articleNum);
                    }
                }
            } catch (SQLException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }

            return null;
        }

        @Override
        public Date getLastSyncTime() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT lastSyncArticleNum FROM NN_NEWSFEEDS WHERE pk = ?")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    long time = rs.getLong(1);
                    if (!rs.wasNull()) {
                        return new Date(time * 1000);
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }

            return null;
        }

        @Override
        public void setLastSyncTime(Date time) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "UPDATE NN_NEWSFEEDS SET lastSyncTime = ? WHERE pk = ?")) {

                ps.setLong(1, time.getTime() / 1000);  // convert to seconds since epoch
                ps.setInt(2, pk);
                ps.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void setLastSyncArticleNum(Specification.ArticleNumber num) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "UPDATE NN_NEWSFEEDS SET lastSyncArticleNum = ? WHERE pk = ?")) {

                ps.setInt(1, num.getValue());
                ps.setInt(2, pk);
                ps.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Peer GetPeer() {
            return peer;
        }
    }

    public static class PeerRecord implements Peer {
        private final Connection dbConnection;
        private final int pk;
        private final String label;
        private final String address;

        PeerRecord(Connection dbConnection, int pk, String label, String address) {
            this.dbConnection = dbConnection;
            this.pk = pk;
            this.label = label;
            this.address = address;
        }

        @Override
        public int getPk() {
            return pk;
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public String getLabel() {
            return label;
        }

        @Override
        public boolean getDisabledStatus() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT disabled FROM NN_PEERS WHERE pk = ?")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
                throw new RuntimeException("Failed to locate peer in database: " + pk);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void setDisabledStatus(boolean disabled) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "UPDATE NN_PEERS SET disabled = ? WHERE pk = ?")) {
                ps.setBoolean(1, disabled);
                ps.setInt(2, pk);
                ps.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Date getListLastFetched() {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "SELECT listLastFetched FROM NN_PEERS WHERE pk = ?")) {
                ps.setInt(1, pk);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    long time = rs.getLong(1);
                    if (!rs.wasNull()) {
                        return new Date(time * 1000);  // convert from seconds to milliseconds
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public void setListLastFetched(Date lastFetched) {
            try (PreparedStatement ps = dbConnection.prepareStatement(
                    "UPDATE NN_PEERS SET listLastFetched = ? WHERE pk = ?")) {

                ps.setLong(1, lastFetched.getTime() / 1000);  // convert to seconds since epoch
                ps.setInt(2, pk);
                ps.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }
}

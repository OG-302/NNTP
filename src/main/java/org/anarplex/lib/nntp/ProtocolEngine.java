package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.Specification.NNTP_Response_Code;
import org.anarplex.lib.nntp.env.IdentityService;
import org.anarplex.lib.nntp.env.NetworkUtilities;
import org.anarplex.lib.nntp.env.PersistenceService;
import org.anarplex.lib.nntp.env.PersistenceService.Newsgroup;
import org.anarplex.lib.nntp.env.PersistenceService.NewsgroupArticle;
import org.anarplex.lib.nntp.env.PolicyService;
import org.anarplex.lib.nntp.utils.DateAndTime;
import org.anarplex.lib.nntp.utils.WildMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Function;

/**
 * This class implements the NNTP Server protocol as defined in RFC 3977.  It is designed to be used in a single-threaded
 * manner.
 */
public class ProtocolEngine {

    public static final String NNTP_VERSION = "2";  // RFC 3977 is NNTP Version 2.0
    public static final String NNTP_SERVER = "Postus";  // this NNTP-lib implementation
    public static final String NNTP_SERVER_VERSION = "0.7"; // build version

    private static final Logger logger = LoggerFactory.getLogger(ProtocolEngine.class);

    private final CommandDispatcher dispatcher;

    /**
     * Creates a new NNTP Server instance for use by a single NNTP Client.  This method is NOT thread-safe and
     * should only be called once per client connection and not shared between multiple threads or clients.
     */
    public ProtocolEngine(PersistenceService persistenceService, IdentityService identityService, PolicyService policyService, NetworkUtilities.ProtocolStreams protocolStreams) {
        if (persistenceService != null
                && identityService != null
                && policyService != null
                && protocolStreams != null) {

            dispatcher = new CommandDispatcher(new ClientContext(persistenceService, identityService, policyService, protocolStreams));
            setCommandHandlers(dispatcher);

            try {
                // ready persistence service for use
                persistenceService.init();

                // update local log newsgroup: local.nntp.postus.log
                Specification.NewsgroupName logGroupName = new Specification.NewsgroupName("local.nntp.postus.log");
                PersistenceService.Newsgroup logGroup = persistenceService.getGroupByName(logGroupName);
                if (logGroup == null) {
                    logGroup = persistenceService.addGroup(logGroupName, "Local group for logging server activity", Specification.PostingMode.Prohibited, LocalDateTime.now(), NNTP_SERVER, false);
                }
                logGroup.setPostingMode(Specification.PostingMode.Allowed);
                Map<String, Set<String>> articleHeaders = new HashMap<>();
                articleHeaders.put(Specification.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Set.of(logGroupName.getValue()));
                articleHeaders.put(Specification.NNTP_Standard_Article_Headers.Subject.getValue(), Set.of("Server activity log"));
                articleHeaders.put(Specification.NNTP_Standard_Article_Headers.From.getValue(), Set.of("Postus Service<none@nowhere.com>"));
                articleHeaders.put(Specification.NNTP_Standard_Article_Headers.Date.getValue(), Set.of(DateAndTime.format1(LocalDateTime.now())));
                articleHeaders.put(Specification.NNTP_Standard_Article_Headers.Path.getValue(), Set.of(identityService.getHostIdentifier() + "!not-for-email"));

                Specification.MessageId msgID = identityService.createMessageID(articleHeaders);
                articleHeaders.put(Specification.NNTP_Standard_Article_Headers.MessageID.getValue(), Set.of(msgID.getValue()));
                String body = "Server started at " + articleHeaders.get(Specification.NNTP_Standard_Article_Headers.Date.getValue()).iterator().next() + "\r\n";


                Specification.Article.ArticleHeaders articleHeadersObj = new Specification.Article.ArticleHeaders(articleHeaders);
                logGroup.addArticle(msgID, articleHeadersObj, body, false);
                logGroup.setPostingMode(Specification.PostingMode.Prohibited);

            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException |
                     PersistenceService.ExistingNewsgroupException |
                     Specification.Article.ArticleHeaders.InvalidArticleHeaderException |
                     Newsgroup.ExistingArticleException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new NullPointerException("persistenceService, identityService, policyService, inputStream and outputStream must not be null");
        }
    }

    /**
     * Consumes a stream of NNTP Requests from a client and replies with a stream of NNTP Responses.
     * Before returning, this method will close the supplied persistence, identity, and policy services and the supplied
     * protocol streams.
     * If the connected client terminates the dialog gracefully (via the QUIT command), then true is returned.
     * If an unrecoverable error is encountered during request processing, false is returned.
     *
     * @return true if the dialog with the client ended without errors, false otherwise
     */
    public boolean start() {

        boolean errorEncountered = false;   // whether an error was encountered during processing of the request

        // this is the initial part of the connection.  check that the server is ready for processing
        if (!onNewConnection()) {
            return false;
        }

        try {
            if (dispatcher.clientContext.policyService.isPostingAllowed(null)) {
                // Posting allowed
                sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_200, NNTP_SERVER, NNTP_SERVER_VERSION);
            } else {
                // Posting prohibited
                sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_201, NNTP_SERVER, NNTP_SERVER_VERSION);
            }

            String line;
            while ((line = dispatcher.clientContext.requestStream.readLine()) != null) {

                // fetch the next request line
                logger.info("Received request line: {}", line);
                // submit the request to the dispatcher to be executed by the corresponding command handler
                boolean result = dispatcher.applyHandler(line);

                // check to see if the session has finished or if the recently executed command resulted in an error
                if (!result || dispatcher.clientContext.isTerminated()) {
                    // exit request processing loop with status of recently executed command
                    return (errorEncountered = result);
                }
                dispatcher.clientContext.responseStream.flush();
                dispatcher.clientContext.persistenceService.commit();
            }
            return true;
        } catch (SocketException e) {
            // any encountered exception is fatal and results in an end to request processing
            logger.error(e.getMessage(), e);
            return false;
        } catch (IOException ex) {
            logger.error("Error reading from client socket: {}", ex.getMessage(), ex);
            return false;
        } catch (Exception e) {
            // any encountered exception is fatal and results in a 500 response and an end to request processing
            logger.error(e.getMessage(), e);
            sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_500);
            return false;
        } finally {
            if (!errorEncountered) {
                // no errors, do a graceful shutdown
                dispatcher.clientContext.persistenceService.commit();
            } else {
                // unrecoverable error encountered
                dispatcher.clientContext.persistenceService.rollback();
            }
            try {
                // explicitly flush and close the streams and services, eventhough some implement Closable interface
                dispatcher.clientContext.responseStream.flush();
                dispatcher.clientContext.requestStream.close();
                dispatcher.clientContext.responseStream.close();
                dispatcher.clientContext.protocolStreams.closeConnection();
                dispatcher.clientContext.identityService.close();
                dispatcher.clientContext.policyService.close();
                dispatcher.clientContext.persistenceService.close();
            } catch (Exception _) {
                // ignore any exceptions encountered during stream/service close
            }
        }
    }

    protected boolean onNewConnection() {
        // check that the dispatcher has the minimum (mandatory) set of command handlers
        if (!Specification.NNTP_Server_Capabilities._MANDATORY.isSufficientSet(dispatcher.nntpCommandHandlers.keySet())) {
            // one or more mandatory commands are not supported by the server
            sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_400);
            return false;
        }

        // check that the persistence service is ready for use
        if (!dispatcher.clientContext.persistenceService.listAllGroups(false, true).hasNext()) {
            // one or more mandatory commands are not supported by the server
            sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_400);
            return false;
        }
        return true;
    }

    private void setCommandHandlers(CommandDispatcher dispatcher) {
        dispatcher.clearHandlers();

        dispatcher.addHandler(Specification.NNTP_Request_Commands.ARTICLE, this::handleArticle);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.BODY, this::handleBody);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.CAPABILITIES, this::handleCapabilities);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.DATE, this::handleDate);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.GROUP, this::handleGroup);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.HEAD, this::handleHead);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.HELP, this::handleHelp);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.IHAVE, this::handleIHave);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LAST, this::handleLast);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST, this::handleList);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST_ACTIVE, this::handleListActive);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST_ACTIVE_TIMES, this::handleListActiveTimes);
        // dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST_DISTRIBUTION_PATS, this::handleListDistributionPats); -- not implemented
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST_NEWSGROUPS, this::handleListNewsgroups);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST_OVERVIEW_FMT, this::handleListOverviewFmt);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.HDR, this::handleHDR);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST_HEADERS, this::handleListHeaders);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.LIST_GROUP, this::handleListGroup);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.MODE_READER, this::handleModeReader);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.NEW_GROUPS, this::handleNewgroups);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.NEW_NEWS, this::handleNewNews);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.NEXT, this::handleNext);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.OVERVIEW, this::handleOverview);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.POST, this::handlePost);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.QUIT, this::handleQuit);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.STAT, this::handleStat);
        dispatcher.addHandler(Specification.NNTP_Request_Commands.XOVER, this::handleXOver);
    }

    /*
     * --- Beginning of Command Handlers ---
     */

    /**
     * Handles the ARTICLE command in the NNTP protocol, which retrieves both the
     * headers and the body of a specified article. This method delegates the
     * request processing to the {@code articleRequest} method with appropriate
     * parameters to ensure that both headers and body are sent.
     *
     * @param c the client context containing services and I/O streams needed to
     *          process the ARTICLE command
     * @return true if the article is successfully retrieved and the response is
     * properly sent, false otherwise
     */
    protected boolean handleArticle(ClientContext c) {
        return articleRequest(c, true, true);
    }

    /**
     * Handles the BODY command in the NNTP protocol to retrieve the body of an article
     * without including the headers. It uses the {@code articleRequest} method
     * with predefined parameters to process the request.
     *
     * @param c the client context, which includes services and streams
     *          required to handle the command
     * @return true if the body of the article is successfully retrieved,
     * false if an error occurs
     */
    protected boolean handleBody(ClientContext c) {
        return articleRequest(c, false, true);
    }

    /**
     * handleCapabilities checks the current command set registered with the Engine's Dispatcher to determine which
     * of the NNTP Capabilities are currently supported by the Engine.
     * The response must list VERSION first in the list.
     */
    protected boolean handleCapabilities(ClientContext c) {
        sendResponse(c.responseStream, NNTP_Response_Code.Code_101, "Current Capabilities:");

        // determine the currently supported commands of the engine
        Set<Specification.NNTP_Request_Commands> commandSet = dispatcher.getHandlerNames();

        // VERSION must appear first in the response
        c.responseStream.printf("VERSION %s\r\n", NNTP_VERSION);

        // List other Capabilities (if any) presently supported by the engine
        for (Specification.NNTP_Server_Capabilities cap : Specification.NNTP_Server_Capabilities.values()) {
            if (cap.isSufficientSet(commandSet)) {
                // engine's commandSet is enough to support this capability
                if (!cap.name().startsWith("_")) {  // enums whose names start with '_' are ignored.
                    c.responseStream.printf("%s\r\n", cap.getValue());
                }
            }
        }
        c.responseStream.printf(Specification.DOT_LINE);
        c.responseStream.flush();
        return true;
    }

    protected boolean handleDate(ClientContext c) {
        // DATE command takes no arguments
        if (c.requestParts.length > 1) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        // Get the current date /time in UTC
        java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String dateTimeString = dateFormat.format(new Date());

        // Send response: 111 yyyyMMddHHmmss
        return sendResponse(c.responseStream, NNTP_Response_Code.Code_111, dateTimeString);
    }


    /* Sends back information about the specified Newsgroup.
     * As per RFC-3977: When a valid group is selected by means of this command, the currently selected newsgroup
     * MUST be set to that group, and the current article number MUST be set to the first article in the group
     * (this applies even if the group is already the currently selected newsgroup).
     */
    protected boolean handleGroup(ClientContext c) {
        String[] args = c.requestParts;

        // GROUP command requires exactly one argument: the newsgroup name
        if (args.length != 2) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        String groupNameStr = args[1];

        try {
            // Validate and parse newsgroup name
            Specification.NewsgroupName groupName = new Specification.NewsgroupName(groupNameStr);

            // Search for the newsgroup in the persistence service
            PersistenceService.Newsgroup newsgroup = c.persistenceService.getGroupByName(groupName);

            // Check if the newsgroup exists
            if (newsgroup == null || newsgroup.isIgnored()) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_411);  // no such newsgroup
            }

            // Set as current newsgroup, which also has the side effect of setting the current article pointer to the
            // first article in the group
            c.currentArticleAndGroup.setCurrentGroup(newsgroup);

            // Get newsgroup metrics
            PersistenceService.NewsgroupMetrics metrics = newsgroup.getMetrics();

            // Send response: 211 count low high group
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_211,
                    metrics.getNumberOfArticles(),
                    metrics.getLowestArticleNumber(),
                    metrics.getHighestArticleNumber(),
                    groupName.getValue());

        } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error - invalid newsgroup name
        } catch (Exception e) {
            logger.error("Error in GROUP command: {}", e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_403);  // internal fault
        }
    }

    /**
     * Handles the HEAD command for fetching the header information of an article
     * in the NNTP protocol. This method uses the {@code articleRequest} method
     * internally with specific parameters to retrieve only the headers without the body.
     *
     * @param c the client context including the necessary services and streams
     *          required to process the request
     * @return true if the request can be processed successfully, false otherwise
     */
    protected boolean handleHead(ClientContext c) {
        return articleRequest(c, true, false);
    }

    protected boolean handleHelp(ClientContext c) {
        // HELP command takes no arguments
        if (c.requestParts.length > 1) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        try {
            // Send initial response: 100 help text follows
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_100)) {
                return false;
            }

            // Send help text describing available commands
            c.responseStream.write("The following commands are (in the general case) supported by this service:\r\n");
            c.responseStream.write("\r\n");
            c.responseStream.write("ARTICLE [message-id|number]  Retrieve article headers and body\r\n");
            c.responseStream.write("BODY [message-id|number]     Retrieve article body\r\n");
            c.responseStream.write("CAPABILITIES                 List server capabilities\r\n");
            c.responseStream.write("DATE                         Get server date and time\r\n");
            c.responseStream.write("GROUP newsgroup              Select a newsgroup\r\n");
            c.responseStream.write("HEAD [message-id|number]     Retrieve article headers\r\n");
            c.responseStream.write("HELP                         Display this help text\r\n");
            c.responseStream.write("IHAVE message-id             Transfer an article to the server\r\n");
            c.responseStream.write("LAST                         Select previous article\r\n");
            c.responseStream.write("LIST [ACTIVE|NEWSGROUPS]     List newsgroups\r\n");
            c.responseStream.write("LISTGROUP [newsgroup]        List article numbers in newsgroup\r\n");
            c.responseStream.write("MODE READER                  Set reader mode\r\n");
            c.responseStream.write("NEWSGROUPS date time         List new newsgroups\r\n");
            c.responseStream.write("NEWNEWS newsgroups date time List new articles\r\n");
            c.responseStream.write("NEXT                         Select next article\r\n");
            c.responseStream.write("OVER [range|message-id]      Get overview information\r\n");
            c.responseStream.write("POST                         Post a new article\r\n");
            c.responseStream.write("QUIT                         Close connection\r\n");
            c.responseStream.write("STAT [message-id|number]     Check article status\r\n");
            c.responseStream.write("XOVER [range]                Get overview information (legacy)\r\n");
            c.responseStream.write("\r\n");
            c.responseStream.write("Server Version: " + NNTP_SERVER + " " + NNTP_SERVER_VERSION + "\r\n");
            c.responseStream.write("NNTP Version: " + NNTP_VERSION + "\r\n");

            // Send termination line
            c.responseStream.write(Specification.DOT_LINE);
            c.responseStream.flush();
            return true;

        } catch (Exception e) {
            logger.error("Error in HELP command: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * HandleIHave handles the IHAVE command.
     * Articles submitted to a moderated group are run past the policy service and are then added to the newsgroup -
     * either marked as Rejected or Not Rejected depending on the policy service's decision.  Articles can subsequently be
     * marked as not rejected by the moderator at a later time.
     * <p>
     * RFC-3977 on page 60 notes an example -
     * "Note that the message-id in the IHAVE command is different from the one in the article headers; while this is bad
     * practice and SHOULD NOT be done, it is not forbidden."
     * In this implementation, the message-id in the IHAVE command CAN be different from the one in the article's headers.
     */
    protected boolean handleIHave(ClientContext c) {
        String[] args = c.requestParts;

        // IHAVE command requires exactly one argument: the message-id
        if (args.length != 2) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        String messageIdStr = args[1];  // message-id is always taken from the IHAVE command, not from the article headers

        IdentityService.Subject submitter = null; // TODO obtain submitter's subject from the client
        if (!c.policyService.isIHaveTransferAllowed(submitter)) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_437); // transfer rejected
        }

        // The article from the client (headers and body) after validation
        Specification.Article streamedArticle;

        try {
            // Validate message-id provided by the client in the IHAVE command request
            Specification.MessageId messageId = new Specification.MessageId(messageIdStr);

            // Check if the article already exists in the persistence service, included rejected versions
            if (c.persistenceService.hasArticle(messageId) ||
                    c.persistenceService.isRejectedArticle(messageId)) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_435);  // the article isn't wanted
            }

            // Article is wanted - request the client to send it
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_335)) {  // send article to be transferred
                return false;
            }

            Specification.ProtoArticle protoArticle = Specification.ProtoArticle.readFrom(c.requestStream);

            // validate headers
            Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(protoArticle.getHeadersLowerCase());

            // compare the Message-ID in the article header with the message-id in the IHAVE command
            Specification.MessageId headerMessageId = articleHeaders.getMessageId();
            if (!headerMessageId.equals(messageId)) {
                // Message-ID mismatch or missing.  Discouraged by RFC-3977, but not forbidden. :-/
                // later, the isAllowed() callout will have a chance to disallow this article if such is the desired application rule
                logger.warn("MessageId from IHAVE command {} does not match Message-ID header in article: {}", messageIdStr, headerMessageId.getValue());
            }

            // construct a representation of the article suitable for use with the persistent service
            streamedArticle = new Specification.Article(messageId, articleHeaders, protoArticle.getBodyText());

        } catch (IOException e) {
            logger.error("Error reading article: {}", e.getMessage(), e);
            sendResponse(c.responseStream, NNTP_Response_Code.Code_436); // transfer failed
            return false;   // close connection upon IOExceptions
        } catch (Specification.Article.InvalidArticleFormatException |
                 Specification.Article.ArticleHeaders.InvalidArticleHeaderException |
                 Specification.MessageId.InvalidMessageIdException e ) {
            logger.error("Error reading article: {}", e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_437); // transfer rejected
        }

        // once added to the persistence service, this variable will track the persisted article
        PersistenceService.NewsgroupArticle addedArticle = null;

        // add the article to the persistence store
        try {
            c.persistenceService.checkpoint();

            for (Specification.NewsgroupName n : streamedArticle.getAllHeaders().getNewsgroups()) {
                // add the article into each newsgroup mentioned in its Newsgroup header

                if (Specification.NewsgroupName.isLocal(n)) {
                    // don't allow peers to add to a "local." newsgroup via IHAVE.  Can only use POST!
                    continue;
                }

                // Check if this newsgroup exists in our store
                PersistenceService.Newsgroup newsgroup = c.persistenceService.getGroupByName(n);
                if (newsgroup == null || newsgroup.isIgnored()) {
                    // Newsgroup doesn't exist, or we don't track it anymore - continue with the next newsgroup
                    continue;
                }

                // Check policy if the article is allowed
                boolean isApproved = c.policyService.isArticleAllowed(streamedArticle.messageId, streamedArticle.getAllHeaders(), streamedArticle.getBody(), newsgroup.getName(), newsgroup.getPostingMode(), submitter);

                // Add the article to the newsgroup, or if already added, then include it in other ones
                try {
                    if (addedArticle == null) {
                        // not currently in the store, so add it
                        addedArticle = newsgroup.addArticle(streamedArticle.messageId, streamedArticle.getAllHeaders(), streamedArticle.getBody(), !isApproved);
                    } else {
                        newsgroup.includeArticle(addedArticle);
                    }
                } catch (PersistenceService.Newsgroup.ExistingArticleException e) {
                    // Article already exists in this newsgroup - that shouldn't happen, but ok, skip it
                    logger.error("Article already exists in newsgroup {}", n.getValue());
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_403); // transfer failed
                }
            }

            if (addedArticle == null) {
                // Article wasn't added to any newsgroup
                logger.info("Article {} wasn't destined for any of our newsgroups", messageIdStr);
            }

            c.persistenceService.commit();

            // Article successfully transferred
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_235);  // article transferred OK (i.e., persisted)

        } catch (Exception e) {
            logger.error("Error in IHAVE command: {}", e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_436);  // transfer failed
        } finally {
            if (addedArticle == null) {
                c.persistenceService.rollback();
            } else {
                c.persistenceService.commit();
            }
        }
    }

    protected boolean handleLast(ClientContext c) {
        if (c.currentArticleAndGroup.getCurrentGroup() != null) {
            if (c.currentArticleAndGroup.getCurrentArticle() != null) {
                NewsgroupArticle a = c.currentArticleAndGroup.getCurrentGroup().getPreviousArticle(
                        c.currentArticleAndGroup.getCurrentArticle().getArticleNumber());  // go to the previous newsgroup article
                if (a != null) {
                    c.currentArticleAndGroup.setCurrentArticle(a);
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_223, a.getArticleNumber(), a.getMessageId());
                } else {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_422);   // no previous article in this group
                }
            } else {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_420);    // the current article number is invalid
            }
        } else {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_412);    // no newsgroup selected
        }
    }

    /**
     * As per RFC-3977 (Sec 7.6.1) "The LIST command allows the server to provide blocks of information to the client.
     * This information may be global or may be related to newsgroups; in the latter case, the information may be returned
     * either for all groups or only for those matching a wildmat.  Each block of information is represented by a different
     * keyword.  The command returns the specific information identified by the keyword."
     * Request format: LIST [keyword [wildmat|argument]]
     * where keyword = ACTIVE | ACTIVE.TIMES | DISTRIB.PATS | HEADERS | NEWSGROUPS | OVERVIEW.FMT.
     * In this implementation, there is a separate command handler for each LIST keyword, since some handlers belong to
     * different capabilities classes and thus maybe removed from the dispatcher while other handlers (capabilities)
     * remain in the dispatcher.
     * Request format: LIST [wildmat]
     */
    protected boolean handleList(ClientContext c) {
        if (c.requestParts.length > 2) {
            // this variant of LIST (i.e. one with no keywords) does not accept any arguments.
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        // process wildmat argument if provided
        WildMatcher wildMatcher = null;
        String wildmatStr = "";
        if (c.requestParts.length == 2) {
            wildMatcher = new WildMatcher(wildmatStr = c.requestParts[1]);
        }

        // Send initial response
        if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_215, "LIST", wildmatStr)) {
            return false;
        }

        return listNewsgroups(c, wildMatcher);
    }

    /**
     * See the LIST command above and also RFC-3977 Sec 7.6.3.
     * Request format: LIST ACTIVE [wildmat]
     * Response format: 215 ACTIVE
     * Response format: <newsgroup> <last> <first> <mode>
     */
    protected boolean handleListActive(ClientContext c) {
        if (c.requestParts.length > 3) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        // process wildmat argument if provided
        WildMatcher wildMatcher = null;
        String wildmatStr = "";
        if (c.requestParts.length == 3) {
            wildMatcher = new WildMatcher(wildmatStr = c.requestParts[2]);
        }

        // Send initial response
        if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_215, "LIST ACTIVE", wildmatStr)) {
            return false;
        }

        return listNewsgroups(c, wildMatcher);
    }

    /**
     * See the LIST command above and also RFC-3977 Sec 7.6.4 "The active.times list is maintained by some NNTP servers
     * to contain information about who created a particular newsgroup and when.  Each line of this list consists of
     * three fields separated from each other by one or more spaces.  The first field is the name of the newsgroup.
     * The second is the time when this group was created on this news server, measured in seconds since the start of
     * January 1, 1970.  The third is plain text intended to describe the entity that created the newsgroup; it is often
     * a mailbox as defined in RFC 2822 [RFC2822]."
     * Request format: LIST ACTIVE.TIMES [wildmat]
     * Response format: 215 ACTIVE.TIMES
     * Response format: <newsgroup> <seconds since epoch> <description>
     */
    protected boolean handleListActiveTimes(ClientContext c) {
        if (c.requestParts.length > 3) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        // process wildmat argument if provided
        WildMatcher wildMatcher = null;
        String wildMatStr = "";
        if (c.requestParts.length == 3) {
            wildMatcher = new WildMatcher(c.requestParts[2]);
        }

        if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_215, "LIST ACTIVE.TIMES", wildMatStr)) {
            return false;
        }

        // Get all newsgroups
        Iterator<Newsgroup> n = c.persistenceService.listAllGroups(false, false);

        // Send each newsgroup in the format: group time createdBy
        while (n.hasNext()) {
            PersistenceService.Newsgroup group = n.next();
            if (wildMatcher == null || wildMatcher.matches(group.getName().getValue())) {

                // Format: <newsgroupname> <seconds since epoch> <description>
                c.responseStream.write(String.format("%s %d %s\r\n",
                        group.getName().getValue(),
                        group.getCreatedAt().toEpochSecond(ZoneOffset.UTC),
                        group.getCreatedBy()));
            }
        }

        // Send termination line
        c.responseStream.write(Specification.DOT_LINE);
        c.responseStream.flush();
        return true;
    }

    /**
     * See the LIST command above and also RFC-3977 Sec 7.6.5.
     */
    protected boolean handleListDistributionPats(ClientContext c) {
        // TODO.  Not implemented yet.
        return true;
    }

    /**
     * As per RFC-3977, LIST NEWSGROUPS (Sec 7.6.6).  "The newsgroups list is maintained by NNTP servers to contain the
     * name of each newsgroup that is available on the server and a short description about the purpose of the group.
     * The list MAY omit newsgroups for which the information is unavailable and MAY include groups not available on the
     * server.  The client MUST NOT assume that the list is complete or that it matches the list returned by LIST ACTIVE."
     * This implementation returns the list of known newsgroups except local newsgroups (i.e. "local.") and any
     * newsgroup that has been marked as ignored.
     * Request format: LIST NEWSGROUPS [wildmat]
     */
    protected boolean handleListNewsgroups(ClientContext c) {
        if (c.requestParts.length > 3) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        // process wildmat argument if provided
        WildMatcher wildMatcher = null;
        String wildMatStr = "";
        if (c.requestParts.length == 3) {
            wildMatcher = new WildMatcher(wildMatStr = c.requestParts[2]);
        }

        // Get all newsgroups
        Iterator<Newsgroup> newsgroups = c.persistenceService.listAllGroups(false, false);

        // Send initial response
        if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_215, "LIST NEWSGROUPS", wildMatStr)) {
            return false;
        }

        while (newsgroups.hasNext()) {
            PersistenceService.Newsgroup n = newsgroups.next();

            if (wildMatcher == null || wildMatcher.matches(n.getName().getValue())) {
                c.responseStream.write(String.format("%s %s\r\n", n.getName().getValue(), n.getDescription()));
            }
        }

        // Send termination line
        c.responseStream.write(Specification.DOT_LINE);
        c.responseStream.flush();
        return true;
    }

    protected boolean listNewsgroups(ClientContext c, WildMatcher wildMatcher) {
        // Get all newsgroups
        Iterator<Newsgroup> groups = c.persistenceService.listAllGroups(false, false);

        // Send each newsgroup in the format: group high low status
        while (groups.hasNext()) {
            PersistenceService.Newsgroup group = groups.next();
            if (wildMatcher == null || wildMatcher.matches(group.getName().getValue())) {

                PersistenceService.NewsgroupMetrics metrics = group.getMetrics();

                // Determine posting status character
                char status = switch (group.getPostingMode()) {
                    case Allowed -> 'y';
                    case Moderated -> 'm';
                    default -> 'n';
                };

                // Format: groupname high low status
                c.responseStream.write(String.format("%s %d %d %c\r\n",
                        group.getName().getValue(),
                        (metrics.getHighestArticleNumber() != null ? metrics.getHighestArticleNumber().getValue() : Specification.NoArticlesHighestNumber),
                        (metrics.getHighestArticleNumber() != null ? metrics.getLowestArticleNumber().getValue() : Specification.NoArticlesLowestNumber),
                        status));
            }
        }

        // Send termination line
        c.responseStream.write(Specification.DOT_LINE);
        c.responseStream.flush();
        return true;
    }


    protected boolean handleListOverviewFmt(ClientContext c) {
        // TODO
        return false;
    }

    protected boolean handleHDR(ClientContext c) {
        // TODO
        return false;
    }

    protected boolean handleListHeaders(ClientContext c) {
        // TODO
        return false;
    }

    protected boolean handleListGroup(ClientContext c) {
        String[] args = c.requestParts;

        // LISTGROUP [newsgroup]
        if (args.length > 2) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
        }

        if (args.length == 2) {
            // Use the provided newsgroup argument
            try {
                Specification.NewsgroupName groupName = new Specification.NewsgroupName(args[1]);
                Newsgroup newsgroup = c.persistenceService.getGroupByName(groupName);
                if (newsgroup == null || newsgroup.isIgnored()) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_411); // no such newsgroup
                }
                c.currentArticleAndGroup.setCurrentGroup(newsgroup);
            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
            }
        } else {
            // No argument. There must be a currently selected group
            if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_412); // no newsgroup selected
            }

            // set current article pointer to first article in the group
            c.currentArticleAndGroup.setCurrentArticle(c.currentArticleAndGroup.getCurrentGroup().getFirstArticle());
        }

        try {
            PersistenceService.NewsgroupMetrics metrics = c.currentArticleAndGroup.getCurrentGroup().getMetrics();

            // Initial response: 211 count low high group
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_211,
                    metrics.getNumberOfArticles(),
                    metrics.getLowestArticleNumber(),
                    metrics.getHighestArticleNumber(),
                    c.currentArticleAndGroup.getCurrentGroup().getName().getValue())) {
                return false;
            }

            // Now list article numbers, one per line, then terminating dot
            Specification.ArticleNumber low = metrics.getLowestArticleNumber();
            Specification.ArticleNumber high = metrics.getHighestArticleNumber();

            if (low != null && high != null && metrics.getNumberOfArticles() > 0) {
                Iterator<PersistenceService.NewsgroupArticle> it =
                        c.currentArticleAndGroup.getCurrentGroup().getArticlesNumbered(low, high);
                while (it.hasNext()) {
                    PersistenceService.NewsgroupArticle a = it.next();
                    c.responseStream.write(Integer.toString(a.getArticleNumber().getValue()));
                    c.responseStream.write("\r\n");
                }
            }

            c.responseStream.write(Specification.DOT_LINE);
            c.responseStream.flush();
            return true;
        } catch (Exception e) {
            logger.error("Error handling LISTGROUP: {}", e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_403);
        }
    }

    /**
     * As per RFC-3977 Sec 5.3: The MODE READER command instructs a mode-switching server to switch modes, as described in Section 3.4.2.
     * Switching modes to READER MODE removes all TRANSIT MODE command handlers from the Dispatcher.
     * This implementation continues to allow the following capabilities in READER_MODE:
     * - LIST, NEW_NEWS, OVER, and POST.
     */
    protected boolean handleModeReader(ClientContext c) {
        // remove all command handlers except MANDATORY ones and those that are part of the READER capability
        for (Specification.NNTP_Request_Commands command : dispatcher.getHandlerNames()) {
            if (!(Specification.NNTP_Server_Capabilities._MANDATORY.isRequiredCommand(command)
                    || Specification.NNTP_Server_Capabilities.READER.isRequiredCommand(command)
                    || Specification.NNTP_Server_Capabilities.LIST.isRequiredCommand(command)
                    || Specification.NNTP_Server_Capabilities.NEW_NEWS.isRequiredCommand(command)
                    || Specification.NNTP_Server_Capabilities.OVER.isRequiredCommand(command)
                    || Specification.NNTP_Server_Capabilities.POST.isRequiredCommand(command)
            )) {
                dispatcher.removeHandler(command);
            }
        }

        // respond to the client
        if (c.policyService.isPostingAllowed(null)) {
            // posting allowed
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_200);
        } else {
            // posting prohibited
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_201);
        }
    }

    protected boolean handleNewgroups(ClientContext c) {
        String[] args = c.requestParts;

        // Expected: NEWGROUPS date time [GMT] [distributions]
        if (args.length < 3) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
        }

        String dateStr = args[1];
        String timeStr = args[2];
        boolean gmt = false;

        // Optional GMT token
        int idx = 3;
        if (args.length > idx && args[idx].equalsIgnoreCase("GMT")) {
            gmt = true;
            idx++;
        }
        // Any additional args (distributions/wildmats) are ignored for now.

        // Parse datetime as UTC. Support compact forms yyyyMMdd and HHmmss
        if (dateStr == null || timeStr == null) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }
        // Accept either 8-digit date and 6-digit time
        if (dateStr.length() != 8 || timeStr.length() != 6) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        LocalDateTime since;
        try {
            since = DateAndTime.parse1(dateStr + timeStr);
        } catch (DateTimeParseException e) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        try {
            // Query persistence for groups added since the given date/time
            Iterator<PersistenceService.Newsgroup> groups = c.persistenceService.listAllGroupsAddedSince(since);

            // Send initial response 231
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_231)) {
                return false;
            }

            while (groups != null && groups.hasNext()) {
                PersistenceService.Newsgroup group = groups.next();
                if (group == null || group.isIgnored()) {
                    continue;
                }
                PersistenceService.NewsgroupMetrics metrics = group.getMetrics();

                // Determine posting status character consistent with LIST ACTIVE
                char status = switch (group.getPostingMode()) {
                    case Allowed -> 'y';
                    case Moderated -> 'm';
                    default -> 'n';
                };

                c.responseStream.write(String.format("%s %d %d %c\r\n",
                        group.getName().getValue(),
                        (metrics.getHighestArticleNumber() != null ? metrics.getHighestArticleNumber().getValue() : Specification.NoArticlesHighestNumber),
                        (metrics.getHighestArticleNumber() != null ? metrics.getLowestArticleNumber().getValue() : Specification.NoArticlesLowestNumber),
                        status));
            }

            // Terminate
            c.responseStream.write(Specification.DOT_LINE);
            c.responseStream.flush();
            return true;
        } catch (Exception e) {
            logger.error("Error handling NEWSGROUPS: {}", e.getMessage(), e);
            return false;
        }
    }

    protected boolean handleNewNews(ClientContext c) {
        String[] args = c.requestParts;

        // Expected: NEWNEWS newsgroups date time [GMT] [distributions]
        if (args == null || args.length < 4) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        String newsgroupsWildmat = args[1];
        String dateStr = args[2];
        String timeStr = args[3];

        // Optional GMT token (ignored in this implementation since we parse as UTC)
        int idx = 4;
        if (args.length > idx && "GMT".equalsIgnoreCase(args[idx])) {
            idx++;
        }
        // Any remaining args are distributions; RFC allows them, but this implementation ignores them.

        // Validate date/time formats: yyyymmdd and hhmmss
        if (dateStr == null || timeStr == null || dateStr.length() != 8 || timeStr.length() != 6) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        LocalDateTime since;
        try {
            since = DateAndTime.parse1(dateStr + timeStr);
        } catch (DateTimeParseException e) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        try {
            // Prepare matcher for wildmat on newsgroup names
            WildMatcher matcher = new WildMatcher(newsgroupsWildmat);

            // Start multiline response
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_230)) {
                return false;
            }

            // Use a set to avoid duplicates if an article appears in multiple groups
            Set<String> emittedIds = new LinkedHashSet<>();

            Iterator<PersistenceService.Newsgroup> groups = c.persistenceService.listAllGroups(false, false);
            while (groups != null && groups.hasNext()) {
                PersistenceService.Newsgroup group = groups.next();
                if (group == null || group.isIgnored()) {
                    continue;
                }
                String groupName = group.getName().getValue();
                if (!matcher.matches(groupName)) {
                    continue;
                }

                Iterator<PersistenceService.NewsgroupArticle> arts = group.getArticlesSince(since);
                while (arts != null && arts.hasNext()) {
                    PersistenceService.NewsgroupArticle nga = arts.next();
                    if (nga == null || nga.getMessageId() == null) {
                        continue;
                    }
                    String mid = nga.getMessageId().getValue();
                    if (mid == null) {
                        continue;
                    }
                    if (emittedIds.add(mid)) {
                        c.responseStream.write(mid + "\r\n");
                    }
                }
            }

            // Terminate with a single dot line
            c.responseStream.write(Specification.DOT_LINE);
            c.responseStream.flush();
            return true;
        } catch (Exception e) {
            logger.error("Error handling NEWNEWS: {}", e.getMessage(), e);
            return false;
        }
    }

    protected boolean handleNext(ClientContext c) {
        if (c.currentArticleAndGroup.getCurrentGroup() != null) {
            if (c.currentArticleAndGroup.getCurrentArticle() != null) {
                NewsgroupArticle a = c.currentArticleAndGroup.getCurrentGroup().getNextArticle(
                        c.currentArticleAndGroup.getCurrentArticle().getArticleNumber());  // proceed to the next newsgroup article
                if (a != null) {
                    c.currentArticleAndGroup.setCurrentArticle(a);
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_223, a.getArticleNumber(), a.getMessageId());
                } else {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_421);   // no next article in this group
                }
            } else {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_420);    // the current article number is invalid
            }
        } else {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_412);    // no newsgroup selected
        }
    }

    protected boolean handleOverview(ClientContext c) {
        String[] args = c.requestParts;

        try {
            if (args.length == 1) {
                // OVER with no arguments use the current article of the current group
                if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_412); // no newsgroup selected
                }
                if (c.currentArticleAndGroup.getCurrentArticle() == null) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_420); // current article number invalid
                }

                // Send 224 then exactly one overview line and terminator
                if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_224)) {
                    return false;
                }
                PersistenceService.NewsgroupArticle a = c.currentArticleAndGroup.getCurrentArticle();
                writeOverviewLine(c, a.getArticleNumber().getValue(), a);
                c.responseStream.write(Specification.DOT_LINE);
                c.responseStream.flush();
                return true;

            } else if (args.length == 2) {
                String argument = args[1];

                // Message-ID form
                if (argument.startsWith("<") && argument.endsWith(">")) {
                    Specification.MessageId messageId;
                    try {
                        messageId = new Specification.MessageId(argument);
                    } catch (IllegalArgumentException | Specification.MessageId.InvalidMessageIdException e) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_430); // invalid message-id
                    }

                    Specification.Article article = c.persistenceService.getArticle(messageId);
                    if (article == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_430); // no article with that message-id
                    }

                    // Determine the article number within the current group, if any
                    int articleNumber = 0;
                    if (c.currentArticleAndGroup.getCurrentGroup() != null) {
                        Specification.ArticleNumber n = c.currentArticleAndGroup.getCurrentGroup().getArticle(messageId);
                        if (n != null) articleNumber = n.getValue();
                    }

                    if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_224)) {
                        return false;
                    }
                    writeOverviewLine(c, articleNumber, article);
                    c.responseStream.write(Specification.DOT_LINE);
                    c.responseStream.flush();
                    return true;

                } else {
                    // Range form requires a selected group
                    if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_412); // no newsgroup selected
                    }

                    // Parse range: n-m or n-
                    Integer low = null;
                    Integer high = null;
                    try {
                        if (argument.contains("-")) {
                            String[] parts = argument.split("-", -1);
                            if (parts.length != 2) {
                                return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
                            }
                            if (!parts[0].isEmpty()) {
                                low = Integer.parseInt(parts[0]);
                            }
                            if (!parts[1].isEmpty()) {
                                high = Integer.parseInt(parts[1]);
                            }
                        } else {
                            // Single number means a single article
                            low = Integer.parseInt(argument);
                            high = low;
                        }
                    } catch (NumberFormatException e) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
                    }

                    // Determine numeric bounds
                    Specification.ArticleNumber lowerBound;
                    Specification.ArticleNumber upperBound;
                    try {
                        if (low == null) {
                            // format "-m" not supported: treat this as a syntax error per conservative approach
                            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
                        }
                        lowerBound = new Specification.ArticleNumber(low);

                        if (high == null) {
                            // format "n-" → up to the current group's highest
                            PersistenceService.NewsgroupMetrics metrics = c.currentArticleAndGroup.getCurrentGroup().getMetrics();
                            Specification.ArticleNumber highest = metrics.getHighestArticleNumber();
                            if (highest == null) {
                                return sendResponse(c.responseStream, NNTP_Response_Code.Code_423);
                            }
                            upperBound = highest;
                        } else {
                            upperBound = new Specification.ArticleNumber(high);
                        }
                    } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
                    }

                    Iterator<PersistenceService.NewsgroupArticle> it = c.currentArticleAndGroup.getCurrentGroup().getArticlesNumbered(lowerBound, upperBound);
                    if (it == null || !it.hasNext()) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_423); // no article in that range
                    }

                    if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_224, lowerBound, upperBound)) {
                        return false;
                    }
                    while (it.hasNext()) {
                        PersistenceService.NewsgroupArticle nga = it.next();
                        writeOverviewLine(c, nga.getArticleNumber().getValue(), nga);
                    }
                    c.responseStream.write(Specification.DOT_LINE);
                    c.responseStream.flush();
                    return true;
                }
            } else {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
            }

        } catch (IOException e) {
            logger.error("Error in OVER command: {}", e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_403); // internal fault
        }
    }

    protected boolean handlePost(ClientContext c) {
        String[] args = c.requestParts;

        // check command validity
        if (args.length != 1) {
            // the POST command should have exactly zero arguments
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        // check the client's permission to post
        IdentityService.Subject submitter = null; // TODO obtain submitter's identity from the client session
        if (!c.policyService.isPostingAllowed(submitter)) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_440); // posting is not allowed
        }

        // Article is wanted - ask the client to send it
        if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_340)) {
            return false;
        }

        // read in the article from the client (into a proto article), validate it, and assign it to streamedArticle
        Specification.Article streamedArticle = null;

        try {
            // Read the article from the client stream (headers and body).  Very little validation done on the headers at this point.
            Specification.ProtoArticle protoArticle = Specification.ProtoArticle.readFrom(c.requestStream);

            // the messageId to be associated with this article
            Specification.MessageId messageId = null;

            // get header values associated with messageID from the proto article
            Set<String> messageIdFieldValues = protoArticle.getHeadersLowerCase().get(Specification.NNTP_Standard_Article_Headers.MessageID.getValue());

            if (messageIdFieldValues == null || messageIdFieldValues.isEmpty()) {
                // client did not include a MessageId in the article being posted.  Create one.
                messageId = c.identityService.createMessageID(protoArticle.getHeadersLowerCase());
                // include this in the headers (headers will not be valid without this required field).
                protoArticle.addFieldIfNotPresent(Specification.NNTP_Standard_Article_Headers.MessageID, messageId.getValue());
            } else {
                // client did provide a MessageId in the article being posted.  Validate it.
                if (messageIdFieldValues.size() > 1) {
                    logger.error("Error in article being Posted.  Multiple Message-Ids: {}", messageIdFieldValues.size());
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_441); // transfer rejected
                } else {
                    // client specified one header field value for message-id
                    String messageIdStr = messageIdFieldValues.iterator().next();

                    // validate the supplied messageId
                    messageId = new Specification.MessageId(messageIdStr);

                    // check to see if an article with this message-id already exists in the persistence service, possibly even as a rejected article.
                    if (c.persistenceService.hasArticle(messageId) || c.persistenceService.isRejectedArticle(messageId)) {
                        logger.error("Error in article being Posted.  Supplied Message-Id already exists: {}", messageIdStr);
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);    // transfer rejected
                    }
                }
            }

            // add the Date header field if not present
            protoArticle.addFieldIfNotPresent(Specification.NNTP_Standard_Article_Headers.Date,
                    DateAndTime.format1(LocalDateTime.now(ZoneId.of("UTC"))));

            // add Path header field if not present
            protoArticle.addFieldIfNotPresent(Specification.NNTP_Standard_Article_Headers.Path,
                    c.identityService.getHostIdentifier());

            Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(protoArticle.getHeadersLowerCase());
            // assign a streamed article's validated contents to streamedArticle
            streamedArticle = new Specification.Article(messageId, articleHeaders, protoArticle.getBodyText());
        } catch (IOException e) {
            logger.error("Error in POST command: {}", e.getMessage(), e);
            sendResponse(c.responseStream, NNTP_Response_Code.Code_441);
            return false;  // IOException results in the connection with the client being closed
        } catch (Specification.Article.InvalidArticleFormatException |
                 Specification.Article.ArticleHeaders.InvalidArticleHeaderException |
                 Specification.MessageId.InvalidMessageIdException e) {
            logger.error("Error in POST command: {}", e.getMessage());
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_441); // transfer rejected
        }

        // a reference to the article which has been persisted
        PersistenceService.NewsgroupArticle addedArticle = null;

        // add the validated article (streamedArticle) to the persistent store
        try {
            c.persistenceService.checkpoint();

            for (Specification.NewsgroupName n : streamedArticle.getAllHeaders().getNewsgroups()) {
                // add the article into each newsgroup mentioned in its Newsgroup header

                // Check if this newsgroup exists in our store
                PersistenceService.Newsgroup newsgroup = c.persistenceService.getGroupByName(n);
                if (newsgroup == null || newsgroup.isIgnored()) {
                    // Newsgroup doesn't exist, or we don't track it anymore - skip it
                    continue;
                }

                // Check if the article is approved to be added to the newsgroup
                if (newsgroup.getPostingMode().getValue() != Specification.PostingMode.Prohibited.getValue()) {
                    logger.debug("Not POSTING article {} to Posting-prohibited newsgroup {}", streamedArticle.messageId, n);
                    continue;
                }

                // Check policy if the article is allowed in this newsgroup
                boolean isApproved = c.policyService.isArticleAllowed(streamedArticle.messageId, streamedArticle.getAllHeaders(), streamedArticle.getBody(), newsgroup.getName(), newsgroup.getPostingMode(), submitter);

                // Add the article to the newsgroup
                try {
                    if (addedArticle == null) {
                        // not currently in the store, so add it
                        addedArticle = newsgroup.addArticle(streamedArticle.messageId, streamedArticle.getAllHeaders(), streamedArticle.getBody(), !isApproved);
                    } else {
                        // already in the store, so only need to include it in subsequent newsgroups
                        newsgroup.includeArticle(addedArticle);
                    }
                } catch (PersistenceService.Newsgroup.ExistingArticleException e) {
                    // this should NOT have happened.  we checked for this already based on MessageId.
                    logger.error("Article already exists in newsgroup {}", n.getValue());
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_403); // transfer failed
                }
            }

            if (addedArticle == null) {
                // Article wasn't added to any newsgroup
                logger.warn("Article wasn't added to any of our newsgroups");
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer rejected
            }

            // Article successfully transferred
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_240);  // article transferred OK

        } catch (Exception e) {
            logger.error("Error in POST command: {}", e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer failed
        } finally {
            if (addedArticle == null) {
                c.persistenceService.rollback();
            } else {
                c.persistenceService.commit();
            }
        }
    }

    protected boolean handleQuit(ClientContext c) {
        // mark the session as terminated
        c.setTerminated();
        // return response to indicate the end of this NNTP session
        return sendResponse(c.responseStream, NNTP_Response_Code.Code_205);
    }

    /**
     * Handles the STAT command in the NNTP protocol. This command is used to check
     * the existence of an article without transmitting its contents. The method
     * utilizes the {@code articleRequest} method internally with specific flags
     * indicating no headers or body are transmitted.
     *
     * @param c the client context, which includes necessary services and streams
     *          required to process the STAT command
     * @return true if the request was successfully processed and the article exists,
     * false otherwise
     */
    protected boolean handleStat(ClientContext c) {
        return articleRequest(c, false, false);
    }

    /* RFC 3977: XOVER is retained for backward compatibility as an alias of OVER
     * Delegate to the OVER/Overview handler.
     */
    protected boolean handleXOver(ClientContext c) {
        return handleOverview(c);
    }

    /*
     * --- End of NNTP Command Handlers ---
     */

    /**
     * Write one OVER/OVERVIEW line per RFC 3977 / XOVER compatibility.
     * Format (fields are tab-separated):
     * number TAB subject TAB from TAB date TAB message-id TAB references TAB bytes TAB lines
     * Missing optional fields are emitted as empty.
     */
    private static void writeOverviewLine(ClientContext c, int articleNumber, Specification.Article article) throws IOException {
        // fetch all headers of the article
        Specification.Article.ArticleHeaders headers = article.getAllHeaders();

        // extract individual standard headers from the article
        String subject = headerFirst(headers, Specification.NNTP_Standard_Article_Headers.Subject.getValue());
        String from = headerFirst(headers, Specification.NNTP_Standard_Article_Headers.From.getValue());
        String date = headerFirst(headers, Specification.NNTP_Standard_Article_Headers.Date.getValue());
        String messageId = article.getMessageId() != null ? article.getMessageId().toString() :
                headerFirst(headers, Specification.NNTP_Standard_Article_Headers.MessageID.getValue());
        String references = headerJoined(headers, Specification.NNTP_Standard_Article_Headers.References.getValue());
        String bytes = headerFirst(headers, Specification.NNTP_Standard_Article_Headers.Bytes.getValue());
        String lines = headerFirst(headers, Specification.NNTP_Standard_Article_Headers.Lines.getValue());

        // Ensure non-null strings and strip CR/LF and TAB characters per Overview constraints
        subject = sanitizeOverviewValue(subject);
        from = sanitizeOverviewValue(from);
        date = sanitizeOverviewValue(date);
        messageId = sanitizeOverviewValue(messageId);
        references = sanitizeOverviewValue(references);
        bytes = sanitizeOverviewValue(bytes);
        lines = sanitizeOverviewValue(lines);

        String s = String.valueOf(articleNumber) + "\t"
                + subject + "\t"
                + from + "\t"
                + date + "\t"
                + messageId + "\t"
                + references + "\t"
                + bytes + "\t"
                + lines + "\r\n";

        c.responseStream.write(s);
    }

    private static String headerFirst(Specification.Article.ArticleHeaders headers, String name) {
        if (headers == null) return "";
        Set<String> v = headers.getHeaderValue(name);
        if (v == null || v.isEmpty()) return "";
        // take deterministic first
        return v.iterator().next();
    }

    private static String headerJoined(Specification.Article.ArticleHeaders headers, String name) {
        if (headers == null) return "";
        Set<String> v = headers.getHeaderValue(name);
        if (v == null || v.isEmpty()) return "";
        // References may contain multiple values; join with a single space
        StringBuilder b = new StringBuilder();
        boolean first = true;
        for (String s : v) {
            if (!first) b.append(' ');
            b.append(s);
            first = false;
        }
        return b.toString();
    }

    /**
     * Removes CR and LF, and replaces tabs with spaces to keep tab-separated output compatible with XOVER.
     * Replaces null string with empty string.
     */
    private static String sanitizeOverviewValue(String s) {
        if (s == null) return "";
        return s.replaceAll("\r", " ").replaceAll("\n", " ").replaceAll("\t", " ");
    }

    /**
     * articleRequest handles the ARTICLE, HEAD, BODY and STAT commands as they are all very similar.
     * ARTICLE -> (sendHeaders, sendBody) == true, true
     * HEAD -> (sendHeaders, sendBody) == true, false
     * BODY -> (sendHeaders, sendBody) == false, true
     * STAT -> (sendHeaders, sendBody) == false, false
     * When the ARTICLE/HEAD/BODY/STAT request specifies an article number, then the current article pointer is updated
     * to that article within the selected (current) group.
     */
    private boolean articleRequest(ClientContext c, boolean sendHeaders, boolean sendBody) {

        String[] args = c.requestParts;

        Specification.Article article = null;
        int articleNumberInReply = 0;

        // get the article requested by the client
        if (args.length == 1) {
            // RFC's Third Form.  No argument provided - use the current article

            // Check if a newsgroup is currently selected
            if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_412);  // no newsgroup selected
            }

            // Check if the current newsgroup has a current article
            if (c.currentArticleAndGroup.getCurrentArticle() == null) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_420); // the current article number is invalid
            }

            NewsgroupArticle a = c.currentArticleAndGroup.getCurrentArticle();
            articleNumberInReply = a.getArticleNumber().getValue();
            article = a;

        } else if (args.length == 2) {
            String argument = args[1];

            // Check if the argument is a message-id (starts with '<' and ends with '>')
            if (argument.startsWith("<") && argument.endsWith(">")) {
                // RFC's First Form.  Argument is a message-id
                try {
                    Specification.MessageId messageId = new Specification.MessageId(argument);

                    // look up the specified article.  Rejected articles will not be returned.
                    article = c.persistenceService.getArticle(messageId);

                    // Check if an article exists in the persistence service
                    if (article == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_430);  // no article with that message-id
                    }

                    // as per RFC, the server MUST NOT alter the currently selected newsgroup or current article number in this case
                } catch (IllegalArgumentException | Specification.MessageId.InvalidMessageIdException e) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_430);  // invalid message-id format
                }
            } else {
                // RFC's Second Form.  Argument should be an article number
                try {
                    int articleNumber = Integer.parseInt(argument);
                    Specification.ArticleNumber articleNum = new Specification.ArticleNumber(articleNumber);

                    // this mode presumes a current group is selected
                    // Check if a newsgroup is currently selected
                    if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_412);  // no newsgroup selected
                    }

                    // If there is an article with that number in the currently selected newsgroup, the
                    // server MUST set the current article number to that number.
                    NewsgroupArticle numberedArticle = c.currentArticleAndGroup.getCurrentGroup().getArticleNumbered(articleNum);
                    if (numberedArticle == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_423);  // no article with that number
                    }
                    c.currentArticleAndGroup.setCurrentArticle(numberedArticle);
                    article = numberedArticle;
                    // article number is to be included in the response
                    articleNumberInReply = articleNum.getValue();
                } catch (NumberFormatException | Specification.ArticleNumber.InvalidArticleNumberException e) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
                }
            }
        } else {
            // Too many arguments
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        // send the response line: 220/221/222/223 <messageId>
        if (!sendResponse(c.responseStream,
                (sendHeaders)
                        // ARTICLE -> response (220).  HEAD -> response (221)
                        ? ((sendBody) ? NNTP_Response_Code.Code_220 : NNTP_Response_Code.Code_221)
                        // BODY -> response (222). STAT -> response (223)
                        : ((sendBody) ? NNTP_Response_Code.Code_222 : NNTP_Response_Code.Code_223),
                articleNumberInReply,
                article.getMessageId())) {
            return false;
        }

        try {
            if (sendHeaders) {
                // Send headers
                Specification.Article.ArticleHeaders headers = article.getAllHeaders();
                if (headers != null) {
                    headers.writeTo(c.responseStream);
                } else {
                    // headers must be present for an article to be sent
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_500);
                }
            }

            if (sendHeaders && sendBody) {
                // When sending headers and a body, include a line separating headers from the Body
                c.responseStream.write("\r\n");
            }

            if (sendBody) {
                // Send body
                String body = article.getBody();
                if (body != null) {
                    body = body.replaceAll("(?m)^\\.", "..");
                    c.responseStream.write(body);
                }
                c.responseStream.write("\r\n");
            }


            if (sendHeaders || sendBody) {
                // Send termination line (a single dot on a line by itself)
                c.responseStream.write(Specification.DOT_LINE);
                c.responseStream.flush();
            }
            return true;

        } catch (Exception e) {
            logger.error("Error sending article: {}", e.getMessage(), e);
            return false;
        }
    }


    // every NNTP Client connection is given its own context object.  None of these services are shared between clients.
    protected static class ClientContext {
        protected void setTerminated() {
            isTerminated = true;
        }

        protected boolean isTerminated() {
            return isTerminated;
        }

        /* In NNTP there is a concept of a current newsgroup and current article.  These are considered contexts
         * for some commands, so that group and article number don't need to be explicitly specified.
         * The following NNTP commands change the current article pointer:
         * The ARTICLE command, when used with an article number, sets the current article pointer to the specified
         * article within the selected newsgroup.
         * The STAT command, when used with an article number, serves to set the current article pointer without returning any text.
         * The GROUP command sets the newsgroup pointer to this newsgroup and sets the current article pointer to the first article in that group.
         * The NEXT command advances the current article pointer to the next message in the newsgroup.
         * The LAST command sets the current article pointer to the last message in the current newsgroup.
         * The HEAD and BODY commands, when used with an article number, set the current article pointer to the specified article.
         * The LISTGROUP command, when used with a group name, sets the current article pointer to the first article in
         * the specified group, and the current group pointer to the specified newsgroup.
         * Such updates to the current article pointer are done through the setCurrentArticle() method.
         */
        protected static class CurrentArticleAndGroup {
            // in NNTP there is a notion of a current group. i.e. a context for some commands.
            private PersistenceService.Newsgroup cg;
            // in NNTP there is a notion of a current article. i.e. a context for some commands.
            private NewsgroupArticle ca;

            // accessors and mutators for the private fields, because there is logic to enforce
            protected Newsgroup getCurrentGroup() {
                return cg;
            }

            protected void setCurrentGroup(Newsgroup currentGroup) {
                // a new Current Group resets the Current Article to the first article (if any) of the Current Newsgroup.
                this.cg = currentGroup;
                this.ca = (currentGroup == null ? null : currentGroup.getFirstArticle());
            }

            protected NewsgroupArticle getCurrentArticle() {
                return ca;
            }

            protected void setCurrentArticle(NewsgroupArticle currentArticle) {
                this.ca = currentArticle;
            }
        }

        ClientContext(PersistenceService persistenceService, IdentityService identityService,
                      PolicyService policyService, NetworkUtilities.ProtocolStreams protocolStreams) {
            this.persistenceService = persistenceService;
            this.identityService = identityService;
            this.policyService = policyService;
            this.protocolStreams = protocolStreams;
            // RFC-3977 specifies that all communication channels use UTF-8 encoding.
            this.requestStream = protocolStreams.getReader();
            this.responseStream = protocolStreams.getWriter();
        }


        // Each client session maintains a state of its current newsgroup and current article (if any).
        protected final CurrentArticleAndGroup currentArticleAndGroup = new CurrentArticleAndGroup();

        // client's authentication token if they are authenticated
        protected Long authenticationToken;

        // Store the parsed request arguments
        private String[] requestParts;

        // Indicates whether this session has been marked as terminated
        private boolean isTerminated = false;

        // various services needed by the protocol engine and that are external (arbitrary) to this implementation.
        private final PersistenceService persistenceService;
        private final IdentityService identityService;
        private final PolicyService policyService;
        private final NetworkUtilities.ProtocolStreams protocolStreams;

        // the engine uses buffered readers and writers for efficiency
        private final BufferedReader requestStream;
        private final PrintWriter responseStream;
    }


    /**
     * CommandDispatcher maintains an internal map of NNTP_Request_Commands to their respective handlers.
     * The handlers read an input stream of requests and write an output stream of responses.
     * The set of handlers maybe changed during the lifetime of a CommandDispatcher instance, for example when the Client requests MODE_READER.
     */
    static private class CommandDispatcher {
        /**
         * Constructs a new CommandDispatcher instance to process the given ClientContext.
         */
        CommandDispatcher(ClientContext clientContext) {
            this.clientContext = clientContext;
        }

        /**
         * Removes all handlers from the dispatcher.
         */
        void clearHandlers() {
            nntpCommandHandlers.clear();
        }

        /**
         * Returns the names of the handlers that were added to this dispatcher.
         */
        Set<Specification.NNTP_Request_Commands> getHandlerNames() {
            return nntpCommandHandlers.keySet();
        }

        /**
         * Adds the supplied handler to this dispatcher and replaces an existing handler if already presently associated with the same command.
         */
        private void addHandler(Specification.NNTP_Request_Commands name, Function<ClientContext, Boolean> func) {
            nntpCommandHandlers.put(name, func);
        }

        /**
         * Removes the handler associated with the supplied command name.
         */
        private void removeHandler(Specification.NNTP_Request_Commands name) {
            nntpCommandHandlers.remove(name);       // remove the handler from being associated with this name (key)
            nntpCommandHandlers.remove(name, null); // remove the name (key) as well because we don't have a handler
        }

        private boolean applyHandler(String requestLine) {
            // determine the NNTP command from the request line
            Specification.NNTP_Request_Commands command = Specification.NNTP_Request_Commands.getCommand(requestLine);
            if (command != null) {
                Function<ClientContext, Boolean> handler = nntpCommandHandlers.get(command);
                if (handler != null) {
                    clientContext.requestParts = requestLine.split(" "); // split the request line into arguments
                    return handler.apply(clientContext);
                } else {
                    // no handler for command
                    return sendResponse(clientContext.responseStream, NNTP_Response_Code.Code_502);
                }
            } else {
                // 502 in this context means: command not permitted (and there is no way for the client to change this)
                return sendResponse(clientContext.responseStream, NNTP_Response_Code.Code_502);
            }
        }

        private static final Logger logger = LoggerFactory.getLogger(CommandDispatcher.class);

        private final ClientContext clientContext;
        private final EnumMap<Specification.NNTP_Request_Commands, Function<ClientContext, Boolean>> nntpCommandHandlers = new EnumMap<>(Specification.NNTP_Request_Commands.class);
    }


    /**
     * SendResponse writes a response to the response stream.
     *
     * @param responseStream the stream to write to
     * @param response       the response code to write (e.g. NNTP_Response_Code.Code_200)
     * @param args           and additional arguments to write (e.g. article number) on the response line
     * @return true if the response was written successfully, false otherwise
     */
    protected static boolean sendResponse(Writer responseStream, NNTP_Response_Code response, Object... args) {
        if (response.getValue() < 400) {
            logger.info("Sending response: {} {}", response, Arrays.toString(args));
        } else  {
            logger.warn("Sending response: {} {}", response, Arrays.toString(args));
        }
        try {
            responseStream.write(String.format("%d", response.getValue())); // write the NNTP response code
            for (Object arg : args) {
                responseStream.write(String.format(" %s", arg.toString())); // write out each of the variable arguments (if any)
            }
            responseStream.write("\r\n");   // terminate the response line
            responseStream.flush();
            return true;
        } catch (IOException e) {
            logger.error("Error sending response: {}", e.getMessage(), e);
            return false;
        }
    }
}

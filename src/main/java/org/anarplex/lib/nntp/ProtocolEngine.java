package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.Specification.NNTP_Response_Code;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Consumer;

/**
 * This class implements the NNTP Server protocol as defined in RFC 3977.  It is designed to be used in a single-threaded
 * manner.
 */
class ProtocolEngine {
    public static final String NNTP_VERSION = "2";  // RFC 3977 is NNTP Version 2.0
    public static final String NNTP_SERVER = "Postus";  // this NNTP-lib implementation
    public static final String NNTP_SERVER_VERSION = "0.82"; // build version
    private static final Logger logger = LoggerFactory.getLogger(ProtocolEngine.class);
    private final CommandDispatcher dispatcher;

    // every NNTP Client connection is given its own context object.  None of these services are shared between clients.
    static class ClientContext {
        // Each client session maintains a state of its current newsgroup and current article (if any).
        protected final CurrentArticleAndGroup currentArticleAndGroup = new CurrentArticleAndGroup();

        // client's authentication token if they are authenticated
        private Long authenticationToken;
        private String pendingUser;

        // Store the parsed request arguments
        String[] requestParts;

        // various services needed by the protocol engine and that are external (arbitrary) to this implementation.
        final PersistenceService persistenceService;
        final IdentityService identityService;
        final PolicyService policyService;
        final NetworkService.ConnectedClient connectedClient;

        /**
         * Constructs a new CommandDispatcher instance to process the given ClientContext.
         */
        public ClientContext(PersistenceService persistenceService,
                      IdentityService identityService,
                      PolicyService policyService,
                      NetworkService.ConnectedClient connectedClient) {
            this.persistenceService = persistenceService;
            this.identityService = identityService;
            this.policyService = policyService;
            this.connectedClient = connectedClient;
        }

        protected Long getAuthenticationToken() {
            return authenticationToken;
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
            private PersistenceService.PublishedNewsgroup cg;
            // in NNTP there is a notion of a current article. i.e. a context for some commands.
            private PersistenceService.PublishedArticle ca;

            // accessors and mutators for the private fields, because there is logic to enforce
            protected PersistenceService.PublishedNewsgroup getCurrentGroup() {
                return cg;
            }

            /**
             * Sets the current group to the supplied published group AND sets the current article to the first
             * (published) article in that group.  A null group is also permitted and this causes the current group
             * to be null, and the current article to be null.
             */
            protected void setCurrentGroup(PersistenceService.PublishedNewsgroup currentGroup) {
                this.cg = currentGroup;
                this.ca = (currentGroup != null ? currentGroup.getFirstArticle() : null);
            }

            protected void setCurrentArticle(PersistenceService.PublishedArticle currentArticle) {
                this.ca = currentArticle;
            }
            protected PersistenceService.PublishedArticle getCurrentArticle() {
                if (cg != null && ca != null) {
                    // ensures that the current group and current article are both still publicly available (published)
                    return cg.getArticleNumbered(ca.getArticleNumber());
                } else {
                    // cg is null, therefore, ca should be null
                    ca = null;
                }
                return null;
            }
        }
    }

    /**
     * CommandDispatcher maintains an internal map of NNTP_Request_Commands -> handlers which can process the requests.
     * The handlers read an input stream of requests and write an output stream of responses.
     * The set of handlers maybe changed during the lifetime of a CommandDispatcher instance, for example, when the
     * Client requests MODE_READER.
     * The CommandDispatcher's context (i.e. its persistence service, networkClient, command handlers, etc.) is
     * specific to this instance, and changes made here to that context do not affect other CommandDispatcher instance
     * contexts.
     */
    private static class CommandDispatcher {
        private final ClientContext clientContext;
        // private final ClientContext clientContext;
        private final EnumMap<Specification.NNTP_Request_Commands, Consumer<ClientContext>> nntpCommandHandlers
                = new EnumMap<>(Specification.NNTP_Request_Commands.class);

        CommandDispatcher(ClientContext clientContext, ProtocolEngine protocolEngine) {
            this.clientContext = clientContext;

            clearHandlers();
            addHandler(Specification.NNTP_Request_Commands.ARTICLE, protocolEngine::handleArticle);
            addHandler(Specification.NNTP_Request_Commands.AUTHINFO, protocolEngine::handleAuthInfo);
            addHandler(Specification.NNTP_Request_Commands.BODY, protocolEngine::handleBody);
            addHandler(Specification.NNTP_Request_Commands.CAPABILITIES, protocolEngine::handleCapabilities);
            addHandler(Specification.NNTP_Request_Commands.DATE, protocolEngine::handleDate);
            addHandler(Specification.NNTP_Request_Commands.GROUP, protocolEngine::handleGroup);
            addHandler(Specification.NNTP_Request_Commands.HEAD, protocolEngine::handleHead);
            addHandler(Specification.NNTP_Request_Commands.HELP, protocolEngine::handleHelp);
            addHandler(Specification.NNTP_Request_Commands.IHAVE, protocolEngine::handleIHave);
            addHandler(Specification.NNTP_Request_Commands.LAST, protocolEngine::handleLast);
            addHandler(Specification.NNTP_Request_Commands.LIST, protocolEngine::handleList);
            addHandler(Specification.NNTP_Request_Commands.LIST_ACTIVE, protocolEngine::handleListActive);
            addHandler(Specification.NNTP_Request_Commands.LIST_ACTIVE_TIMES, protocolEngine::handleListActiveTimes);
            // addHandler(Specification.NNTP_Request_Commands.LIST_DISTRIBUTION_PATS, protocolEngine::handleListDistributionPats); -- not implemented
            addHandler(Specification.NNTP_Request_Commands.LIST_NEWSGROUPS, protocolEngine::handleListNewsgroups);
            addHandler(Specification.NNTP_Request_Commands.LIST_OVERVIEW_FMT, protocolEngine::handleListOverviewFmt);
            addHandler(Specification.NNTP_Request_Commands.HDR, protocolEngine::handleHDR);
            addHandler(Specification.NNTP_Request_Commands.LIST_HEADERS, protocolEngine::handleListHeaders);
            addHandler(Specification.NNTP_Request_Commands.LIST_GROUP, protocolEngine::handleListGroup);
            addHandler(Specification.NNTP_Request_Commands.MODE_READER, protocolEngine::handleModeReader);
            addHandler(Specification.NNTP_Request_Commands.NEW_GROUPS, protocolEngine::handleNewgroups);
            addHandler(Specification.NNTP_Request_Commands.NEW_NEWS, protocolEngine::handleNewNews);
            addHandler(Specification.NNTP_Request_Commands.NEXT, protocolEngine::handleNext);
            addHandler(Specification.NNTP_Request_Commands.OVERVIEW, protocolEngine::handleOverview);
            addHandler(Specification.NNTP_Request_Commands.POST, protocolEngine::handlePost);
            addHandler(Specification.NNTP_Request_Commands.QUIT, protocolEngine::handleQuit);
            addHandler(Specification.NNTP_Request_Commands.STAT, protocolEngine::handleStat);
            addHandler(Specification.NNTP_Request_Commands.XOVER, protocolEngine::handleXOver);
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
        private void addHandler(Specification.NNTP_Request_Commands name, Consumer<ClientContext> func) {
            nntpCommandHandlers.put(name, func);
        }

        /**
         * Removes the handler associated with the supplied command name.
         */
        private void removeHandler(Specification.NNTP_Request_Commands name) {
            nntpCommandHandlers.remove(name);       // remove the handler from being associated with this name (key)
            nntpCommandHandlers.remove(name, null); // remove the name (key) as well because we don't have a handler
        }

        /**
         * Dispatches the command found on the supplied requestLine to the appropriate handler for processing
         */
        private void applyHandler(String requestLine) {
            // determine the NNTP command from the request line
            Specification.NNTP_Request_Commands command = Specification.NNTP_Request_Commands.getCommand(requestLine);
            if (command != null) {
                Consumer<ClientContext> handler = nntpCommandHandlers.get(command);
                if (handler != null) {
                    // normalize the command line as much as possible and split into request parts
                    clientContext.requestParts = requestLine.split("(?U)\\s+"); // split the request line into arguments
                    // dispatch the command to the handler
                    handler.accept(clientContext);
                } else {
                    // no handler for command.  possibly removed by a MODE command
                    clientContext.connectedClient.sendResponse(NNTP_Response_Code.Code_502); // 502 = command not permitted
                }
            } else {
                // command isn't recognized
                clientContext.connectedClient.sendResponse(NNTP_Response_Code.Code_500); // 500 = unknown command
            }
        }
    }

    /**
     * Creates a new NNTP Server instance for use by a single NNTP Client.  This method is NOT thread-safe and
     * should only be called once per client connection and not shared between multiple threads or clients.
     */
    ProtocolEngine(PersistenceService persistenceService,
                   IdentityService identityService,
                   PolicyService policyService,
                   NetworkService.ConnectedClient connectedClient) {

        // wire in this ProtocolEngine's dispatcher
        dispatcher = new CommandDispatcher(
                        new ClientContext(persistenceService,
                                            identityService,
                                            policyService,
                                            connectedClient),
                             this);

    }

    /**
     * Consumes a stream of NNTP Requests from a client and replies to the client with a stream of NNTP Responses.
     * Before returning, this method will close the supplied services (persistence, identity, and policy) and the
     * connection with the client.
     */
    public boolean start() {
        boolean result;
        try {
            if (dispatcher.clientContext.policyService.isPostingAllowedBy(
                    dispatcher.clientContext.connectedClient.getSubject())) {
                // Posting allowed
                dispatcher.clientContext.connectedClient.sendResponse(NNTP_Response_Code.Code_200, NNTP_SERVER, NNTP_SERVER_VERSION);
            } else {
                // Posting prohibited
                dispatcher.clientContext.connectedClient.sendResponse(NNTP_Response_Code.Code_201, NNTP_SERVER, NNTP_SERVER_VERSION);
            }

            String requestLine;
            while ((requestLine = dispatcher.clientContext.connectedClient.readNextLine()) != null) {
                // fetch the next request line
                logger.info("Received request line: {}", requestLine);

                // submit the request to the dispatcher to be executed by the corresponding command handler
                dispatcher.applyHandler(requestLine);
            }
            result = true;
        } catch (Exception e) {
            // any encountered exception is fatal and results in a 500 response and an end to request processing
            logger.error(e.getMessage(), e);
            dispatcher.clientContext.connectedClient.sendResponse(NNTP_Response_Code.Code_500);
            result = false;
        } finally {
            try {
                // explicitly flush and close the streams and services, even though some implement Closable interface
                dispatcher.clientContext.connectedClient.close();
                dispatcher.clientContext.identityService.close();
                dispatcher.clientContext.policyService.close();
                dispatcher.clientContext.persistenceService.close();
            } catch (Exception _) {
                // ignore any exceptions encountered during stream/service close
            }
        }
        return result;
    }

    /*
     * --- Beginning of Command Handlers ---
     */

    /**
     * AuthInfo as defined in RFC4643.  This method cannot be pipelined.  Hence, after being handed AUTHINFO USER,
     * this method will expect the subsequent command AUTHINFO PASS immediately.
     * AUTHINFO USER username
     * AUTHINFO PASS password
     */
    protected void handleAuthInfo(ClientContext c) {
        // TODO implement AUTHINFO command
        String[] args = c.requestParts;

        if (args.length < 3) {
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error
            return;
        }

        String subCommand = args[1].toUpperCase();
        String argument = args[2];

        if ("USER".equals(subCommand)) {
            // clear out any prior USER state
            c.pendingUser = null;

            if (argument.isEmpty()) {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);    // syntax error
                return;
            }

            // check to see if this USER can be authenticated based on username alone
            IdentityService.Subject subject = c.identityService.newSubject(argument);

            Boolean passwordRequired = c.identityService.requiresPassword(subject);
            if (passwordRequired != null) {
                if (passwordRequired) {
                    // Identity Service does not support authentication on username alone
                    c.pendingUser = subject.getPrincipal();
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_381); // password required
                } else {
                    // no password required for this user, but still need to get an authentication token
                    c.authenticationToken = c.identityService.authenticate(subject, null);
                    if (c.authenticationToken != null && c.identityService.isValid(c.authenticationToken)) {
                        logger.info("Authenticated user {} with no password", subject.getPrincipal());
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_281); // authentication successful
                    } else {
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_481); // authentication failed/rejected
                    }
                }
            } else {
                // no such user
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_481); // authentication failed/rejected
            }
        } else if ("PASS".equals(subCommand)) {
            if (c.pendingUser != null) {
                // Authenticate using the identity service
                IdentityService.Subject subject = c.identityService.newSubject(c.pendingUser);
                c.authenticationToken = c.identityService.authenticate(subject, argument);
                if (c.authenticationToken != null && c.identityService.isValid(c.authenticationToken)) {
                    logger.info("Authenticated user {} with password", subject.getPrincipal());
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_281); // authentication successful
                } else {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_481); // authentication failed/rejected
                }
            } else {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_482); // authentication out of sequence
            }
        } else {
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // unknown subcommand
        }
    }


    /**
     * Handles the ARTICLE command in the NNTP protocol, which retrieves both the
     * headers and the body of a specified article. This method delegates the
     * request processing to the {@code articleRequest} method with appropriate
     * parameters to ensure that both headers and body are sent.
     */
    protected void handleArticle(ClientContext c) {
        articleRequest(c, true, true);
    }

    /**
     * Handles the BODY command in the NNTP protocol to retrieve the body of an article
     * without including the headers. It uses the {@code articleRequest} method
     * with predefined parameters to process the request.
     */
    protected void handleBody(ClientContext c) {
        articleRequest(c, false, true);
    }

    /**
     * handleCapabilities checks the current command set registered with the Engine's Dispatcher to determine which
     * of the NNTP Capabilities are currently supported by the Engine.
     * The response must list VERSION first in the list.
     */
    protected void handleCapabilities(ClientContext c) {
        c.connectedClient.sendResponse(NNTP_Response_Code.Code_101, "Current Capabilities:");

        // determine the currently supported commands of the engine
        Set<Specification.NNTP_Request_Commands> commandSet = dispatcher.getHandlerNames();

        // VERSION must appear first in the response
        c.connectedClient.printf("VERSION %s\r\n", NNTP_VERSION);

        // List other Capabilities (if any) presently supported by the engine
        for (Specification.NNTP_Server_Capabilities cap : Specification.NNTP_Server_Capabilities.values()) {
            if (cap.isSufficientSet(commandSet)) {
                // engine's commandSet is enough to support this capability
                if (!cap.name().startsWith("_")) {  // enums whose names start with '_' are ignored.
                    c.connectedClient.printf("%s\r\n", cap.getValue());
                }
            }
        }
        c.connectedClient.sendDotLine();
    }

    protected void handleDate(ClientContext c) {
        // DATE command takes no arguments
        if (c.requestParts.length == 1) {
            // Get the current date /time in UTC
            java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            String dateTimeString = dateFormat.format(new Date());

            // Send response: 111 yyyyMMddHHmmss
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_111, dateTimeString);
        } else {
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error
        }
    }

    /* Sends back information about the specified Newsgroup.
     * As per RFC-3977: When this command selects a valid group, the currently selected newsgroup
     * MUST be set to that group, and the current article number MUST be set to the first article in the group
     * (this applies even if the group is already the currently selected newsgroup). So, this method can only select
     * published newsgroups and published articles.
     */
    protected void handleGroup(ClientContext c) {
        String[] args = c.requestParts;

        // GROUP command requires exactly one argument: the newsgroup name
        if (args.length == 2) {
            String groupNameStr = args[1];

            try {
                // Validate and parse newsgroup name
                Specification.NewsgroupName groupName = new Specification.NewsgroupName(groupNameStr);

                // Search for an Accepted Newsgroup in the persistence service
                PersistenceService.StoredNewsgroup newsgroup = c.persistenceService.getGroupByName(groupName);

                // Check if the newsgroup exists
                if (newsgroup instanceof PersistenceService.PublishedNewsgroup p) {
                    // Set as current newsgroup, which has the side effect of setting the current article pointer
                    // to the first article in the group
                    c.currentArticleAndGroup.setCurrentGroup(p);

                    // Get newsgroup metrics
                    PersistenceService.PublishedNewsgroup.Metrics metrics = p.getMetrics();

                    // Send response: 211 count low high groupName
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_211,
                            Integer.toString(metrics.numPublishedArticles()),
                            metrics.getLowestArticleNumber().toString(),
                            metrics.getHighestArticleNumber().toString(),

                            groupName.getValue());
                } else {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_411);  // no such newsgroup
                }
            } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error - invalid newsgroup name
            }
        } else {
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error
        }
    }

    /**
     * Handles the HEAD command for fetching the header information of an article
     * in the NNTP protocol. This method uses the {@code articleRequest} method
     * internally with specific parameters to retrieve only the headers without the body.
     *
     * @param c the client context including the necessary services and streams
     *          required to process the request
     */
    protected void handleHead(ClientContext c) {
        articleRequest(c, true, false);
    }

    protected void handleHelp(ClientContext c) {
        // HELP command takes no arguments, just the command
        if (c.requestParts.length == 1) {
            // Send initial response: 100 help text follows
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_100);

            // Send help text describing available commands
            c.connectedClient.printf("The following commands are (in the general case) supported by this service:\r\n");
            c.connectedClient.printf("\r\n");
            c.connectedClient.printf("ARTICLE [message-id|number]  Retrieve article headers and body\r\n");
            c.connectedClient.printf("BODY [message-id|number]     Retrieve article body\r\n");
            c.connectedClient.printf("CAPABILITIES                 List server capabilities\r\n");
            c.connectedClient.printf("DATE                         Get server date and time\r\n");
            c.connectedClient.printf("GROUP newsgroup              Select a newsgroup\r\n");
            c.connectedClient.printf("HEAD [message-id|number]     Retrieve article headers\r\n");
            c.connectedClient.printf("HELP                         Display this help text\r\n");
            c.connectedClient.printf("IHAVE message-id             Transfer an article to the server\r\n");
            c.connectedClient.printf("LAST                         Select previous article\r\n");
            c.connectedClient.printf("LIST [ACTIVE|NEWSGROUPS]     List newsgroups\r\n");
            c.connectedClient.printf("LISTGROUP [newsgroup]        List article numbers in newsgroup\r\n");
            c.connectedClient.printf("MODE READER                  Set reader mode\r\n");
            c.connectedClient.printf("NEWSGROUPS date time         List new newsgroups\r\n");
            c.connectedClient.printf("NEWNEWS newsgroups date time List new articles\r\n");
            c.connectedClient.printf("NEXT                         Select next article\r\n");
            c.connectedClient.printf("OVER [range|message-id]      Get overview information\r\n");
            c.connectedClient.printf("POST                         Post a new article\r\n");
            c.connectedClient.printf("QUIT                         Close connection\r\n");
            c.connectedClient.printf("STAT [message-id|number]     Check article status\r\n");
            c.connectedClient.printf("XOVER [range]                Get overview information (legacy)\r\n");
            c.connectedClient.printf("\r\n");
            c.connectedClient.printf("Server Version: " + NNTP_SERVER + " " + NNTP_SERVER_VERSION + "\r\n");
            c.connectedClient.printf("NNTP Version: " + NNTP_VERSION + "\r\n");
            c.connectedClient.sendDotLine();    // termination line
        } else {
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error
        }
    }

    /**
     * HandleIHave handles the IHAVE command.
     * An article submitted via IHAVE is added to every newsgroup it mentions in its header.  Each newsgroup inclusion
     * is followed by a call to the PolicyService.reviewArticleSubmission() which can immediately promote the article
     * to accepted or rejected status (or even erased if desired).  Any newsgroup mentioned in the header which does
     * not already exist in the database (and has never existed in the database; i.e. wasn't Erased) is created and is
     * in the Pending state until the newsgroup is Accepted, or an article included in that newsgroup is Accepted which
     * causes the newsgroup to change to Accepted.
     * In this implementation, the message-id in the IHAVE command CAN be different from the one in the article's headers
     * and this is not rejected.
     * RFC-3977 on page 60 notes an example -
     * "Note that the message-id in the IHAVE command is different from the one in the article headers; while this is bad
     * practice and SHOULD NOT be done, it is not forbidden."
     * In this library, the message-id argument of an NNTP command is always taken as the authoritative source, not an
     * article's header field.
     */
    protected void handleIHave(ClientContext c) {
        String[] args = c.requestParts;
        Boolean articlePosted = null;

        // IHAVE command requires exactly one argument: the message-id
        if (args.length == 2) {
            String messageIdStr = args[1];  // message-id is always taken from the IHAVE command, not from the article's headers

            // check submitter's permissions to transfer articles via IHAVE command
            IdentityService.Subject submitter = c.connectedClient.getSubject();
            if (c.policyService.isIHaveTransferAllowedBy(submitter)) {
                try {
                    // Validate message-id provided by the client in the IHAVE command request
                    Specification.MessageId messageId = new Specification.MessageId(messageIdStr);

                    // Only articles never processed before are allowed to be added to the database
                    if (!c.persistenceService.isKnown(messageId)) {

                        // This messageId not seen before. Request the client to send the article.
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_335);  // send article to be transferred

                        // read in content until a dot-line is encountered
                        Specification.ProtoArticle protoArticle = Specification.ProtoArticle.fromString(c.connectedClient.readStream());

                        // validate headers
                        Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(protoArticle.getHeadersLowerCase());

                        // compare the Message-ID in the article header with the message-id in the IHAVE command
                        Specification.MessageId headerMessageId = articleHeaders.getMessageId();
                        if (!headerMessageId.equals(messageId)) {
                            // Message-ID mismatch or missing.  Discouraged by RFC-3977, but not forbidden. :-/
                            // The PolicyService.reviewArticleSubmission() call below will offer the opportunity to
                            // disallow this article if such is the desire of the app
                            logger.warn("MessageId {} from IHAVE command does not match Message-ID header in article: {}", messageIdStr, headerMessageId.getValue());
                        }

                        // construct a representation of the article suitable for use with the persistent service
                        Specification.Article streamedArticle = new Specification.Article(messageId, articleHeaders, protoArticle.getBodyText());

                        // streamedArticle successfully received
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_235);  // article transferred OK

                        // have the PolicyService do a review of whether to allow this article into the datastore and subsequent processing or not
                        switch (c.policyService.reviewArticle(streamedArticle, Specification.ArticleSource.IHaveTransfer, submitter)) {
                            case Allow -> {
                                // save the article for a later review by PolicyService.reviewPosting()
                                c.persistenceService.addArticle(streamedArticle, Specification.ArticleSource.IHaveTransfer, submitter);
                            }
                            case Ignore -> {
                                // do nothing
                            }
                            case Ban -> {
                                c.persistenceService.ban(messageId);
                            }
                        }
                    } else {
                        // we already have this article or seen it in the past
                        logger.info("Article {} already exists in persistent store.", messageIdStr);
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_435, messageId.getValue());  // the article isn't wanted
                    }
                } catch (Specification.Article.InvalidArticleFormatException |
                         Specification.Article.ArticleHeaders.InvalidArticleHeaderException |
                         Specification.MessageId.InvalidMessageIdException e ) {
                    logger.error("Error reading article: {}", e.getMessage(), e);
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error
                }
            } else {
                // permission denied
                logger.info("Submitter {} denied permission to transfer articles via IHAVE command.", submitter.toString());
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_436, "permission denied");
            }
        } else {
            // IHAVE command requires exactly one argument: the message-id
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error
        }
    }

    protected void handleLast(ClientContext c) {
        if (c.currentArticleAndGroup.getCurrentGroup() != null) {
            if (c.currentArticleAndGroup.getCurrentArticle() != null) {
                PersistenceService.PublishedArticle a = c.currentArticleAndGroup.getCurrentGroup().getPreviousArticle(
                        c.currentArticleAndGroup.getCurrentArticle().getArticleNumber());  // go to the previous newsgroup article
                if (a != null) {
                    c.currentArticleAndGroup.setCurrentArticle(a);
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_223, a.getArticleNumber().toString(), a.getArticle().getMessageId().getValue());
                } else {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_422);   // no previous article in this group
                }
            } else {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_420);    // the current article number is invalid
            }
        } else {
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_412);    // no newsgroup selected
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
    protected void handleList(ClientContext c) {
        if (c.requestParts.length <= 2) {
            // process wildmat argument if provided
            WildMatcher wildMatcher = null;
            String wildmatStr = "";

            if (c.requestParts.length == 2) {
                wildMatcher = new WildMatcher(wildmatStr = c.requestParts[1]);
            }

            // Send initial response
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_215, "LIST", wildmatStr);

            // send list
            listNewsgroups(c, wildMatcher);
        } else {
            // invalid number of arguments
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
        }
    }

    /**
     * See the LIST command above and also RFC-3977 Sec 7.6.3.
     * Request format: LIST ACTIVE [wildmat]
     * Response format: 215 ACTIVE
     * Response format: <newsgroup> <last> <first> <mode>
     */
    protected void handleListActive(ClientContext c) {
        if (c.requestParts.length <= 3) {
            // process wildmat argument if provided
            WildMatcher wildMatcher = null;
            String wildmatStr = "";

            if (c.requestParts.length == 3) {
                wildMatcher = new WildMatcher(wildmatStr = c.requestParts[2]);
            }

            // Send initial response
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_215, "LIST ACTIVE", wildmatStr);

            // send the list
            listNewsgroups(c, wildMatcher);
        } else {
            // invalid number of arguments
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
        }
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
    protected void handleListActiveTimes(ClientContext c) {
        if (c.requestParts.length <= 3) {
            // process wildmat argument if provided
            WildMatcher wildMatcher = null;
            String wildMatStr = "";

            if (c.requestParts.length == 3) {
                wildMatcher = new WildMatcher(c.requestParts[2]);
            }

            c.connectedClient.sendResponse(NNTP_Response_Code.Code_215, "LIST ACTIVE.TIMES", wildMatStr);

            // Get all newsgroups
            Iterator<PersistenceService.PublishedNewsgroup> n = c.persistenceService.listPublishedGroups();

            // Send each newsgroup in the format: group time createdBy
            while (n.hasNext()) {
                PersistenceService.PublishedNewsgroup group = n.next();
                if (wildMatcher == null || wildMatcher.matches(group.getName().getValue())) {

                    // Format: <newsgroupname> <seconds since epoch> <description>
                    c.connectedClient.printf("%s %d %s\r\n",
                            group.getName().getValue(),
                            group.getCreatedAt().getEpochSecond(),
                            group.getCreatedBy());
                }
            }
            // mark the end of the list
            c.connectedClient.sendDotLine();
        } else {
            // invalid number of arguments
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
        }
    }

    /**
     * See the LIST command above and also RFC-3977 Sec 7.6.5.
     */
    protected void handleListDistributionPats(ClientContext c) {
        // TODO.  Not implemented yet.
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
    protected void handleListNewsgroups(ClientContext c) {
        if (c.requestParts.length <= 3) {
            // process wildmat argument if provided
            WildMatcher wildMatcher = null;
            String wildMatStr = "";

            if (c.requestParts.length == 3) {
                wildMatcher = new WildMatcher(wildMatStr = c.requestParts[2]);
            }

            // Get all newsgroups
            Iterator<PersistenceService.PublishedNewsgroup> newsgroups = c.persistenceService.listPublishedGroups();

            // Send initial response
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_215, "LIST NEWSGROUPS", wildMatStr);

            while (newsgroups.hasNext()) {
                PersistenceService.PublishedNewsgroup n = newsgroups.next();

                if (wildMatcher == null || wildMatcher.matches(n.getName().getValue())) {
                    c.connectedClient.printf("%s %s\r\n", n.getName().getValue(), n.getDescription());
                }
            }

            // end the list
            c.connectedClient.sendDotLine();
        } else {
            // invalid number of arguments
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
        }
    }

    protected void listNewsgroups(ClientContext c, WildMatcher wildMatcher) {
        // Get all newsgroups
        Iterator<PersistenceService.PublishedNewsgroup> groups = c.persistenceService.listPublishedGroups();

        // Send each newsgroup in the format: group high low status
        while (groups.hasNext()) {
            PersistenceService.PublishedNewsgroup group = groups.next();
            if (wildMatcher == null || wildMatcher.matches(group.getName().getValue())) {

                PersistenceService.PublishedNewsgroup.Metrics metrics = group.getMetrics();

                // Format: groupName high low status
                c.connectedClient.printf("%s %d %d %c\r\n",
                        group.getName().getValue(),
                        metrics.getHighestArticleNumber(),
                        metrics.getLowestArticleNumber(),
                        group.getPostingMode().toChar());
            }
        }
        // Send termination line
        c.connectedClient.sendDotLine();
    }

    protected void handleListOverviewFmt(ClientContext c) {
        // TODO
    }

    protected void handleHDR(ClientContext c) {
        // TODO
    }

    protected void handleListHeaders(ClientContext c) {
        // TODO
    }

    protected void handleListGroup(ClientContext c) {
        String[] args = c.requestParts;

        // LISTGROUP [newsgroup]
        if (args.length <= 2) {
            if (args.length == 2) {
                // Use the provided newsgroup argument
                try {
                    Specification.NewsgroupName groupName = new Specification.NewsgroupName(args[1]);
                    PersistenceService.StoredNewsgroup newsgroup = c.persistenceService.getGroupByName(groupName);
                    if (newsgroup instanceof PersistenceService.PublishedNewsgroup pn) {
                        c.currentArticleAndGroup.setCurrentGroup(pn);
                    } else {
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_411); // no such newsgroup
                        return;
                    }
                } catch (Specification.NewsgroupName.InvalidNewsgroupNameException e) {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error
                    return;
                }
            } else {
                // No argument. There must be a currently selected group
                if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_412); // no newsgroup selected
                    return;
                } else {
                    // set the current article pointer to the first article in the group
                    c.currentArticleAndGroup.setCurrentArticle(c.currentArticleAndGroup.getCurrentGroup().getFirstArticle());
                }
            }

            if (c.currentArticleAndGroup.getCurrentGroup() != null) {
                PersistenceService.PublishedNewsgroup.Metrics metrics = c.currentArticleAndGroup.getCurrentGroup().getMetrics();

                // Initial response: 211 count low high group
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_211,
                        Integer.toString(metrics.numPublishedArticles()),
                        Integer.toString(metrics.numLowestArticle()),
                        Integer.toString(metrics.numHighestArticle()),
                        c.currentArticleAndGroup.getCurrentGroup().getName().getValue());

                // Now list article numbers, one per line, then terminating dot
                Specification.ArticleNumber low = metrics.getLowestArticleNumber();
                Specification.ArticleNumber high = metrics.getHighestArticleNumber();

                if (metrics.numPublishedArticles() > 0) {
                    Iterator<PersistenceService.PublishedArticle> it =
                            c.currentArticleAndGroup.getCurrentGroup().getArticlesNumbered(low, high);
                    while (it.hasNext()) {
                        PersistenceService.PublishedArticle a = it.next();
                        c.connectedClient.printf("%d\r\n", a.getArticleNumber().getValue());
                    }
                }
                // end of list
                c.connectedClient.sendDotLine();
            }
        } else {
            // invalid number of arguments
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
        }
    }

    /**
     * As per RFC-3977 Sec 5.3: The MODE READER command instructs a mode-switching server to switch modes, as described in Section 3.4.2.
     * Switching modes to READER MODE removes all TRANSIT MODE command handlers from the Dispatcher.
     * This implementation continues to allow the following capabilities in READER_MODE:
     * - LIST, NEW_NEWS, OVER, and POST.
     */
    protected void handleModeReader(ClientContext c) {
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

        // respond to the client with response code depending upon the submitter's privilege
        c.connectedClient.sendResponse(
                c.policyService.isPostingAllowedBy(null)    // TODO find the submitter
                        ? NNTP_Response_Code.Code_200   // posting allowed
                        : NNTP_Response_Code.Code_201); // posting is not allowed
    }

    protected void handleNewgroups(ClientContext c) {
        String[] args = c.requestParts;

        // Expected: NEWGROUPS date time [GMT] [distributions]
        if (3 <= args.length) {
            String dateStr = args[1];
            String timeStr = args[2];
            // Parse datetime as UTC. Support compact forms yyyyMMdd and HHmmss
            if (dateStr != null && timeStr != null && (dateStr.length() == 8 || dateStr.length() == 6) && timeStr.length() == 6) {

                try {
                    // normalize the dateStr if only 6 characters are provided
                    if (dateStr.length() == 6) {
                        // determine the current year
                        int year = Calendar.getInstance().get(Calendar.YEAR);
                        // determine
                        int yy = Integer.parseInt(dateStr.substring(0, 2));
                        // the missing century is the current century if yy is less than or equal to the current year, otherwise its the previous century.
                        int cc = (yy <= year % 100 ? year / 100 :  (year / 100) -1);
                        // make dateStr explicit: i.e., 8 characters
                        dateStr = cc + dateStr;
                    }

                    Instant since = Utilities.DateAndTime.parse_yyyMMddHHmmss(dateStr + timeStr);

                    // Any additional args (distributions/wildmats) are ignored for now.
                    // TODO process arg == 5 => distributions

                    // Query persistence for groups added since the given date/time
                    Iterator<PersistenceService.PublishedNewsgroup> groups = c.persistenceService.listGroupsAddedSince(since);

                    // Send initial response 231
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_231);

                    while (groups.hasNext()) {
                        PersistenceService.PublishedNewsgroup group = groups.next();
                        PersistenceService.PublishedNewsgroup.Metrics metrics = group.getMetrics();

                        // format is: newsgroupName highestNum lowestNum postingMode
                        c.connectedClient.printf("%s %d %d %c\r\n",
                                group.getName().getValue(),
                                metrics.numHighestArticle(),
                                metrics.numLowestArticle(),
                                group.getPostingMode().toChar());
                    }

                    // Terminate response list
                    c.connectedClient.sendDotLine();

                } catch (DateTimeParseException e) {
                    logger.error("Invalid date/time format: {}", e.getMessage(), e);
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
                }
            } else {
                logger.error("Invalid NEWGROUPS command format: {}", Arrays.toString(args));
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
            }
        } else {
            logger.error("Invalid NEWGROUPS command format: {}", Arrays.toString(args));
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error
        }
    }

    protected void handleNewNews(ClientContext c) {
        String[] args = c.requestParts;

        // Expected: NEWNEWS wildmat date time [GMT] [distributions]
        if (args != null && 4 <= args.length) {
            String newsgroupsWildmat = args[1];
            String dateStr = args[2];
            String timeStr = args[3];

            // TODO implement arg[4] distributions; RFC allows them, but this implementation ignores them.

            // Validate date/time formats: yyyymmdd and hhmmss
            if (dateStr != null && timeStr != null && dateStr.length() == 8 && timeStr.length() == 6) {
                try {
                    Instant since = Utilities.DateAndTime.parse_yyyMMddHHmmss(dateStr + timeStr);

                    // Prepare matcher for wildmat on newsgroup names
                    WildMatcher matcher = new WildMatcher(newsgroupsWildmat);

                    // Start multiline response
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_230, args[1], args[2], args[3]);

                    // Use a set to avoid duplicates if an article appears in multiple groups
                    Set<Specification.MessageId> emittedIds = new HashSet<>();

                    Iterator<PersistenceService.PublishedNewsgroup> groups = c.persistenceService.listPublishedGroups();
                    while (groups.hasNext()) {
                        // for each group
                        PersistenceService.PublishedNewsgroup group = groups.next();
                        if (matcher.matches(group.getName().getValue())) {
                            // and whose name matches the wildmat argument
                            // get articles added to that group since the specified time
                            Iterator<PersistenceService.PublishedArticle> articlesSince = group.getArticlesSince(since);
                            while (articlesSince.hasNext()) {
                                PersistenceService.PublishedArticle pa = articlesSince.next();
                                Specification.MessageId msgId = pa.getArticle().getMessageId();
                                if (emittedIds.add(msgId)) {
                                    // this messageID was not emitted before, so send it
                                    c.connectedClient.printf("%s\r\n", msgId.getValue());
                                }
                            }
                        }
                    }
                    // Terminate results
                    c.connectedClient.sendDotLine();
                } catch (DateTimeParseException e) {
                    logger.error("Invalid date/time format: {}", e.getMessage());
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
                }
            } else {
                logger.error("NEWNEWS request has invalid date/time format: {}", Arrays.toString(args));
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
            }
        } else {
            logger.error("NEWNEWS request has invalid number of arguments: {}", Arrays.toString(args));
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
        }
    }

    protected void handleNext(ClientContext c) {
        if (c.currentArticleAndGroup.getCurrentGroup() != null) {
            if (c.currentArticleAndGroup.getCurrentArticle() != null) {
                PersistenceService.PublishedArticle a = c.currentArticleAndGroup.getCurrentGroup().getNextArticle(
                        c.currentArticleAndGroup.getCurrentArticle().getArticleNumber());  // proceed to the next newsgroup article
                if (a != null) {
                    c.currentArticleAndGroup.setCurrentArticle(a);
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_223, a.getArticleNumber().toString(), a.getArticle().getMessageId().getValue());
                } else {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_421);   // no next article in this group
                }
            } else {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_420);    // the current article number is invalid
            }
        } else {
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_412);    // no newsgroup selected
        }
    }

    protected void handleOverview(ClientContext c) {
        String[] args = c.requestParts;

        if (args.length == 1) {
            // OVER with no arguments use the current article of the current group
            if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_412); // no newsgroup selected
                return;
            }
            if (c.currentArticleAndGroup.getCurrentArticle() == null) {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_420); // current article number invalid
                return;
            }

            // Send 224 then exactly one overview line and terminator
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_224);
            PersistenceService.PublishedArticle a = c.currentArticleAndGroup.getCurrentArticle();
            writeOverviewLine(c, a.getArticleNumber().getValue(), a.getArticle());
            c.connectedClient.sendDotLine();

        } else if (args.length == 2) {
            String argument = args[1];

            // Message-ID form
            if (argument.startsWith("<") && argument.endsWith(">")) {
                Specification.MessageId messageId;
                try {
                    messageId = new Specification.MessageId(argument);
                } catch (IllegalArgumentException | Specification.MessageId.InvalidMessageIdException e) {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_430); // invalid message-id
                    return;
                }

                if (!c.persistenceService.isKnown(messageId)) {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_430); // no article with that message-id
                    return;
                }

                // Determine the article number within the current group, if any
                if (c.currentArticleAndGroup.getCurrentGroup() != null) {
                    PersistenceService.PublishedArticle a = c.currentArticleAndGroup.getCurrentGroup().getPublishedArticle(messageId);
                    if (a != null) {
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_224);    // Overview information follows
                        writeOverviewLine(c,
                                a.getArticleNumber().getValue(),
                                a.getArticle());
                        c.connectedClient.sendDotLine();
                    } else {
                        // article is pending, rejected or deleted.
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_430); // no article with that message-id
                    }
                }
            } else {
                // Range form requires a selected group
                if (c.currentArticleAndGroup.getCurrentGroup() == null) {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_412); // no newsgroup selected
                    return;
                }

                // Parse range: n-m or n-
                Integer low = null;
                Integer high = null;
                try {
                    if (argument.contains("-")) {
                        String[] parts = argument.split("-", -1);
                        if (parts.length != 2) {
                            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error
                            return;
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
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error
                    return;
                }

                // Determine numeric bounds
                try {
                    Specification.ArticleNumber lowerBound;
                    Specification.ArticleNumber upperBound;

                    if (low == null) {
                        // format "-m" not supported: treat this as a syntax error per conservative approach
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);
                        return;
                    }
                    lowerBound = new Specification.ArticleNumber(low);

                    if (high == null) {
                        // format "n-" → up to the current group's highest
                        PersistenceService.StoredNewsgroup.Metrics metrics = c.currentArticleAndGroup.getCurrentGroup().getMetrics();
                        Specification.ArticleNumber highest = metrics.getHighestArticleNumber();
                        if (highest == null) {
                            c.connectedClient.sendResponse(NNTP_Response_Code.Code_423);
                            return;
                        }
                        upperBound = highest;
                    } else {
                        upperBound = new Specification.ArticleNumber(high);
                    }

                    Iterator<PersistenceService.PublishedArticle> it = c.currentArticleAndGroup.getCurrentGroup().getArticlesNumbered(lowerBound, upperBound);
                    if (it == null || !it.hasNext()) {
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_423);    // no article in that range
                    } else {
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_224);    // Overview information follows
                        while (it.hasNext()) {
                            PersistenceService.PublishedArticle a = it.next();
                            writeOverviewLine(c, a.getArticleNumber().getValue(), a.getArticle());
                        }
                        c.connectedClient.sendDotLine();
                    }

                } catch (Specification.ArticleNumber.InvalidArticleNumberException e) {
                    logger.info("Error handling OVERVIEW: {}", e.getMessage(), e);
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error in request
                }
            }
        } else {
            logger.error("Invalid OVERVIEW command format: {}", Arrays.toString(args));
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error in request
        }
    }

    protected void handlePost(ClientContext c) {
        String[] args = c.requestParts;
        Boolean articlePosted = null;

        // check command validity
        if (args.length == 1) {

            // check the client's permission to post
            IdentityService.Subject submitter = c.connectedClient.getSubject();
            if (c.policyService.isPostingAllowedBy(submitter)) {
                // the Article is wanted - ask the client to send it
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_340);

                // the messageId to be associated with this article
                Specification.MessageId messageId = null;

                try {
                    // Read the article from the client stream (headers and body).  Minimal validation is done on the headers at this point.
                    Specification.ProtoArticle protoArticle = Specification.ProtoArticle.fromString(c.connectedClient.readStream());

                    // get header values associated with messageID from the proto article
                    Set<String> messageIdFieldValues = protoArticle.getHeadersLowerCase().get(Specification.NNTP_Standard_Article_Headers.MessageID.getValue());

                    if (messageIdFieldValues == null || messageIdFieldValues.isEmpty()) {
                        // client did not include a MessageId in the article being posted, so create one.
                        messageId = c.identityService.createMessageID(protoArticle.getHeadersLowerCase());

                        // include this in the headers (headers will not be valid without this required field).
                        protoArticle.addFieldIfNotPresent(Specification.NNTP_Standard_Article_Headers.MessageID, messageId.getValue());
                    } else {
                        // client provided a MessageId in the article being posted., so validate it.
                        if (messageIdFieldValues.size() > 1) {
                            // the messageId field can only appear once
                            logger.error("Error in article being Posted.  Multiple Message-Ids: {}", messageIdFieldValues.size());
                            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error in the submission
                            return;
                        } else {
                            // client specified one header field value for message-id
                            String messageIdStr = messageIdFieldValues.iterator().next();

                            // validate the supplied messageId
                            messageId = new Specification.MessageId(messageIdStr);

                            // check to see if an article with this message-id already exists in the persistence service, possibly even as a banned article.
                            if (c.persistenceService.isKnown(messageId)) {
                                logger.info("Post request rejected.  An Article with this Message-Id already exists: {}", messageIdStr);
                                c.connectedClient.sendResponse(NNTP_Response_Code.Code_441);    // transfer rejected
                                return;
                            }
                        }
                    }

                    // add the Date header field if not present
                    protoArticle.addFieldIfNotPresent(Specification.NNTP_Standard_Article_Headers.Date, Utilities.DateAndTime.formatRFC3977(Instant.now()));

                    // add Path header field if not present
                    protoArticle.addFieldIfNotPresent(Specification.NNTP_Standard_Article_Headers.Path, c.identityService.getHostIdentifier());

                    // convert header field keys to lowercase (our internal convention)
                    Specification.Article.ArticleHeaders articleHeaders = new Specification.Article.ArticleHeaders(protoArticle.getHeadersLowerCase());

                    // read in the article from the client (into a proto article), validate it, and assign it to streamedArticle
                    Specification.Article streamedArticle = new Specification.Article(messageId, articleHeaders, protoArticle.getBodyText());

                    // tell the client we've received the article
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_240, String.valueOf(messageId));

                    // have the PolicyService do a review of whether to allow this article into the datastore and subsequent processing or not
                    switch (c.policyService.reviewArticle(streamedArticle, Specification.ArticleSource.Posting, submitter)) {
                        case Allow -> // save the article for a later review by PolicyService.reviewPosting()
                                c.persistenceService.addArticle(streamedArticle, Specification.ArticleSource.Posting, submitter);
                        case Ignore -> {
                            // do nothing
                        }
                        case Ban -> c.persistenceService.ban(messageId);
                    }
                } catch (Specification.MessageId.InvalidMessageIdException |
                         Specification.Article.InvalidArticleFormatException |
                        Specification.Article.ArticleHeaders.InvalidArticleHeaderException e) {
                    logger.error("Error reading article: {}", e.getMessage(), e);
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501); // syntax error
                }
            } else {
                logger.error("Client is not authorized to post to this newsgroup");
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_440); // posting is not allowed
            }
        } else {
            // the POST command should have exactly zero arguments
            logger.error("Invalid POST command format: {}", Arrays.toString(args));
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error
        }
    }

    protected void handleQuit(ClientContext c) {
        // save changes to the persistence store
        c.persistenceService.commit();

        // return response acknowledging the end of this NNTP session with this client, which also closes the connection
        c.connectedClient.sendResponse(NNTP_Response_Code.Code_205);
    }

    /**
     * Handles the STAT command in the NNTP protocol. This command is used to check
     * the existence of an article without transmitting its contents. The method
     * uses the {@code articleRequest} method internally with specific flags
     * indicating no headers or body are transmitted.
     *
     * @param c the client context, which includes necessary services and streams
     *          required to process the STAT command
     */
    protected void handleStat(ClientContext c) {
        articleRequest(c, false, false);
    }

    /* RFC 3977: XOVER is retained for backward compatibility as an alias of OVER
     * Delegate to the OVER/Overview handler.
     */
    protected void handleXOver(ClientContext c) {
        handleOverview(c);
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
    private static void writeOverviewLine(ClientContext c, int articleNumber, Specification.Article article) {
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

        // replace null strings with "" and strip CR/LF and TAB characters per Overview constraints
        subject = sanitizeOverviewValue(subject);
        from = sanitizeOverviewValue(from);
        date = sanitizeOverviewValue(date);
        messageId = sanitizeOverviewValue(messageId);
        references = sanitizeOverviewValue(references);
        bytes = sanitizeOverviewValue(bytes);
        lines = sanitizeOverviewValue(lines);

        c.connectedClient.printf("%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\r\n", articleNumber, subject, from, date, messageId, references, bytes, lines);
    }

    private static String headerFirst(Specification.Article.ArticleHeaders headers, String name) {
        if (headers != null) {
            Set<String> v = headers.getHeaderValue(name);
            if (v != null && !v.isEmpty()) {
                // take deterministic first
                return v.iterator().next();
            }
        }
        return "";
    }

    /**
     * Returns the multivalued header field as a single string delimited by commas, or empty string if not present.
     */
    private static String headerJoined(Specification.Article.ArticleHeaders headers, String name) {
        final char separator = ',';
        if (headers != null) {
            Set<String> v = headers.getHeaderValue(name);
            if (v != null && !v.isEmpty()) {
                return Strings.join(v, separator);
            }
        }
        return "";
    }

    /**
     * Removes CR and LF, and replaces tabs with spaces to keep tab-separated output compatible with XOVER.
     * Replaces null string with empty string.
     */
    private static String sanitizeOverviewValue(String s) {
        if (s != null && !s.isEmpty()) {
            return s.replaceAll("\r", " ").replaceAll("\n", " ").replaceAll("\t", " ");
        }
        return "";
    }

    /**
     * This method handles the ARTICLE, HEAD, BODY and STAT commands as they are all very similar.  It returns, as
     * directed, those parts of an article to the client.  If the article requested is banned, then a 404 response
     * code is returned because there is no article to return.  This messageId should not have been provided to the
     * client by the library, or it was valid but has subsequently been banned.
     * ARTICLE -> (sendHeaders, sendBody) == true, true
     * HEAD -> (sendHeaders, sendBody) == true, false
     * BODY -> (sendHeaders, sendBody) == false, true
     * STAT -> (sendHeaders, sendBody) == false, false
     * When the ARTICLE/HEAD/BODY/STAT request specifies an article number, then the current article pointer is updated
     * to that article within the selected (current) group.
     */
    private void articleRequest(ClientContext c, boolean sendHeaders, boolean sendBody) {

        String[] args = c.requestParts;

        Specification.Article article;
        int articleNumberInReply = 0;

        // get the article requested by the client
        if (args.length == 1) {
            // RFC's Third Form.  No argument provided - use the current article

            // Check if a newsgroup is currently selected
            if (c.currentArticleAndGroup.getCurrentGroup() != null) {
                // Check if the current newsgroup has a current article
                if (c.currentArticleAndGroup.getCurrentArticle() != null) {
                    PersistenceService.PublishedArticle a = c.currentArticleAndGroup.getCurrentArticle();
                    article = a.getArticle();
                    articleNumberInReply = a.getArticleNumber().getValue();
                } else {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_420); // the current article number is invalid
                    return;
                }
            } else {
                c.connectedClient.sendResponse(NNTP_Response_Code.Code_412);  // no newsgroup selected
                return;
            }
        } else if (args.length == 2) {
            String argument = args[1];

            // Check if the argument is a message-id (starts with '<' and ends with '>')
            if (argument.startsWith("<") && argument.endsWith(">")) {
                // RFC's First Form.  Argument is a message-id
                try {
                    // validate supplied argument as a messageId
                    Specification.MessageId messageId = new Specification.MessageId(argument);

                    // fetch the corresponding article, if any
                    PersistenceService.StoredArticle sa = c.persistenceService.getArticle(messageId);
                    if (sa != null && sa.isPublished()) {
                        // found the article, and it's been Published in at least one Newsgroup, so we can share it
                        article = sa;

                        // as a convenience, if this article appears as published in the current group (if there is one),
                        // then find its corresponding article number and return it.
                        if (c.currentArticleAndGroup.getCurrentGroup() != null) {
                            // look up this article (as a PublishedArticle) in the CurrentGroup
                            PersistenceService.PublishedArticle pa = c.currentArticleAndGroup.getCurrentGroup().getPublishedArticle(messageId);
                            if (pa != null) {
                                // as per RFC, the server MUST NOT alter the currently selected newsgroup or current article number in this case
                                articleNumberInReply = pa.getArticleNumber().getValue();
                            }
                        }
                    } else {
                        // referencing an unsharable article (i.e. not published in even one newsgroup, or banned, or never seen)
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_430);    // no article with that message-id
                        return;
                    }
                } catch (Specification.MessageId.InvalidMessageIdException e) {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501, "messageId format");  // syntax error: invalid message-id format
                    return;
                }
            } else {
                // RFC's Second Form.  Argument should be an article number
                try {
                    int articleNumberArgument = Integer.parseInt(argument);

                    // syntactically validate the supplied argument
                    Specification.ArticleNumber articleNum = new Specification.ArticleNumber(articleNumberArgument);

                    // This mode presumes a current group is selected. Check if a newsgroup is currently selected
                    if (c.currentArticleAndGroup.getCurrentGroup() != null) {
                        // According to the RFC: If there is an article with that number in the currently selected
                        // newsgroup, the server MUST set the current article number to that number.
                        PersistenceService.PublishedArticle numberedArticle = c.currentArticleAndGroup.getCurrentGroup().getArticleNumbered(articleNum);
                        if (numberedArticle != null) {
                            // change the current article to this one
                            c.currentArticleAndGroup.setCurrentArticle(numberedArticle);
                            // save reference to the article
                            article = numberedArticle.getArticle();
                            // article number is to be included in the response
                            articleNumberInReply = articleNum.getValue();
                        } else {
                            c.connectedClient.sendResponse(NNTP_Response_Code.Code_423);  // no article with that number
                            return;
                        }
                    } else {
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_412);  // no newsgroup selected
                        return;
                    }
                } catch (NumberFormatException |
                         Specification.ArticleNumber.InvalidArticleNumberException e) {
                    c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error
                    return;
                }
            }
        } else {
            // Too many arguments
            c.connectedClient.sendResponse(NNTP_Response_Code.Code_501);  // syntax error
            return;
        }

        if (article != null) {
            // send the response line: 220/221/222/223 <messageId>
            c.connectedClient.sendResponse(
                    (sendHeaders)
                            // ARTICLE -> response (220).  HEAD -> response (221)
                            ? ((sendBody) ? NNTP_Response_Code.Code_220 : NNTP_Response_Code.Code_221)
                            // BODY -> response (222). STAT -> response (223)
                            : ((sendBody) ? NNTP_Response_Code.Code_222 : NNTP_Response_Code.Code_223),
                    Integer.toString(articleNumberInReply),
                    article.getMessageId().getValue());

            try {
                if (sendHeaders) {
                    // Send headers
                    Specification.Article.ArticleHeaders headers = article.getAllHeaders();
                    if (headers != null) {
                        c.connectedClient.printf("%s", headers);
                    } else {
                        // headers must be present for an article to be sent
                        c.connectedClient.sendResponse(NNTP_Response_Code.Code_500);
                        return;
                    }
                }

                if (sendHeaders && sendBody) {
                    // When sending headers and a body, include a line separating headers from the Body
                    c.connectedClient.printf("\r\n");
                }

                if (sendBody) {
                    // Send body
                    String body = article.getBody();
                    // perform dot-stuffing on the body
                    if (body != null) {
                        // replace isolated dots at the start of a new line (i.e. a dot not followed by another dot) with two dots
                        body = body.replaceAll("(?m)^\\.(?!\\.)", "..");
                        // send the body to the client
                        c.connectedClient.printf("%s", body);
                    }
                    c.connectedClient.printf("\r\n");
                }

                if (sendHeaders || sendBody) {
                    // Send termination line (a single dot on a line by itself)
                    c.connectedClient.sendDotLine();
                }
            } catch (Exception e) {
                logger.error("Error sending article: {}", e.getMessage(), e);
            }
        }
    }
}
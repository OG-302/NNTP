package org.anarplex.lib.nntp;

import org.anarplex.lib.nntp.Spec.NNTP_Response_Code;
import org.anarplex.lib.nntp.ext.IdentityService;
import org.anarplex.lib.nntp.ext.PersistenceService;
import org.anarplex.lib.nntp.ext.PersistenceService.Newsgroup;
import org.anarplex.lib.nntp.ext.PersistenceService.NewsgroupArticle;
import org.anarplex.lib.nntp.ext.PolicyService;
import org.anarplex.lib.nntp.utils.DateAndTime;
import org.anarplex.lib.nntp.utils.WildMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.*;
import java.util.*;
import java.util.function.Function;

/**
 * This class implements the NNTP Server protocol as defined in RFC 3977.  It is designed to be used in a single-threaded
 * manner.
 */
public class Server {

    public static final String NNTP_VERSION = "2";  // RFC 3977 - NNTP Version 2.0
    public static final String NNTP_SERVER = "Postus";
    public static final String NNTP_SERVER_VERSION = "0.7";

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final CommandDispatcher dispatcher;

    /**
     * Creates a new NNTP Server instance for use by a single NNTP Client.  This method is not thread-safe and
     * should only be called once per client connection, and not shared between multiple threads or clients.
     * @param persistenceService
     * @param identityService
     * @param policyService
     * @param inputStream
     * @param outputStream
     */
    public Server(PersistenceService persistenceService, IdentityService identityService, PolicyService policyService, InputStreamReader inputStream, OutputStreamWriter outputStream) {
        if (persistenceService != null && identityService != null && policyService != null && inputStream != null && outputStream != null) {

            // load the dispatcher with handlers for each NNTP Command
            dispatcher = new CommandDispatcher(
                    new ClientContext(
                            persistenceService,
                            identityService,
                            policyService,
                            new BufferedReader(inputStream),
                            new BufferedWriter(outputStream)));

            // configure the command handler with this dispatcher
            setCommandHandlers(dispatcher);

            // update local log newsgroup: local.nntp.postus.log
            try {
                persistenceService.init();
                Spec.NewsgroupName logGroupName = new Spec.NewsgroupName("local.nntp.postus.log");
                PersistenceService.Newsgroup logGroup = persistenceService.getGroupByName(logGroupName);
                if (logGroup == null) {
                    logGroup = persistenceService.addGroup(logGroupName, "Local group for logging server activity", Spec.PostingMode.Prohibited, new Date(), NNTP_SERVER, false);
                }
                logGroup.setPostingMode(Spec.PostingMode.Allowed);
                Map<String, Set<String>> articleHeaders = new HashMap<>();
                articleHeaders.put(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue(), Set.of(logGroupName.getValue()));
                articleHeaders.put(Spec.NNTP_Standard_Article_Headers.Subject.getValue(), Set.of("Server activity log"));
                articleHeaders.put(Spec.NNTP_Standard_Article_Headers.From.getValue(), Set.of(NNTP_SERVER));
                articleHeaders.put(Spec.NNTP_Standard_Article_Headers.Date.getValue(), Set.of(DateAndTime.now()));
                articleHeaders.put(Spec.NNTP_Standard_Article_Headers.Path.getValue(), Set.of(identityService.getHostIdentifier() + "!not-for-email"));
                Spec.MessageId msgID = identityService.createMessageID(articleHeaders);
                articleHeaders.put(Spec.NNTP_Standard_Article_Headers.MessageID.getValue(), Set.of(msgID.getValue()));
                Reader body = new StringReader("Server started at " + articleHeaders.get(Spec.NNTP_Standard_Article_Headers.Date.getValue()).iterator().next() + '\n');
                Spec.Article.ArticleHeaders articleHeadersObj = new Spec.Article.ArticleHeaders(articleHeaders);
                logGroup.addArticle(msgID, articleHeadersObj, body,false);
                logGroup.setPostingMode(Spec.PostingMode.Prohibited);

            } catch (Spec.NewsgroupName.InvalidNewsgroupNameException | PersistenceService.ExistingNewsgroupException |
                     Spec.Article.ArticleHeaders.InvalidArticleHeaderException | Newsgroup.ExistingArticleException e) {
                throw new RuntimeException(e);
            }

        } else {
            throw new NullPointerException("persistenceService, identityService, policyService, inputStream and outputStream must not be null");
        }
    }

    /**
     * Consumes a stream of NNTP Requests from a client and replies with a stream of corresponding NNTP Responses.
     * Before returning, this method will close the supplied request and response streams.
     * If the client ends the dialog gracefully (via the QUIT command), then true is returned.  But if an unrecoverable
     * error is encountered, false is returned.
     *
     * @return true if the client dialog ended without errors, false otherwise
     */
    public boolean process() {

        try {
            if (dispatcher.clientContext.policyService.isPostingAllowed(null)) {
                sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_200, NNTP_SERVER, NNTP_SERVER_VERSION);
            } else {
                sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_201, NNTP_SERVER, NNTP_SERVER_VERSION);
            }

            do {
                dispatcher.clientContext.responseStream.flush();
                dispatcher.clientContext.persistenceService.commit();

                String line = dispatcher.clientContext.requestStream.readLine();
                if (line != null) {
                    String[] requestArgs = line.trim().split("\\s+");   // split the command line on whitespace
                    if (0 < requestArgs.length) {
                        boolean result = dispatcher.applyHandler(requestArgs);  // submit the request to the dispatcher
                        if (requestArgs[0].equalsIgnoreCase("QUIT")) {
                            return result;
                        } else {
                            if (result) {
                                continue; // continue to process the next request
                            } else {
                                return false; // request processing resulted in an error.  exit processing
                            }
                        }
                    } else {
                        return false; // no command name was found
                    }
                }
                return true;    // no more input to process
            } while (true);
        } catch (Exception e) {
            sendResponse(dispatcher.clientContext.responseStream, NNTP_Response_Code.Code_500);
            logger.error(e.getMessage(), e);
            return false;
        } finally {
            dispatcher.clientContext.persistenceService.commit();
            try {
                dispatcher.clientContext.responseStream.flush();
                dispatcher.clientContext.requestStream.close();
                dispatcher.clientContext.responseStream.close();
            } catch (IOException e) {}
        }
    }


    //
    // Command Handlers
    //


    /**
     * Handles the ARTICLE command in the NNTP protocol, which retrieves both the
     * headers and the body of a specified article. This method delegates the
     * request processing to the {@code articleRequest} method with appropriate
     * parameters to ensure that both headers and body are sent.
     *
     * @param c the client context containing services and I/O streams needed to
     *          process the ARTICLE command
     * @return true if the article is successfully retrieved and the response is
     *         properly sent, false otherwise
     */
    protected static Boolean handleArticle(ClientContext c) {
        return articleRequest(c, true, true);
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
    protected static Boolean handleHead(ClientContext c) {
        return articleRequest(c, true, false);
    }

    /**
     * Handles the BODY command in the NNTP protocol to retrieve the body of an article
     * without including the headers. It uses the {@code articleRequest} method
     * with predefined parameters to process the request.
     *
     * @param c the client context, which includes services and streams
     *          required to handle the command
     * @return true if the body of the article is successfully retrieved,
     *         false if an error occurs
     */
    protected static Boolean handleBody(ClientContext c) {
        return articleRequest(c, false, true);
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
     *         false otherwise
     */
    protected static Boolean handleStat(ClientContext c) {
        return articleRequest(c, false, false);
    }

    /**
     * This implementation is NOT a mode-switching server (as defined in RFC 3977).
     *
     * @param c
     * @return
     */
    protected static Boolean handleMode(ClientContext c) {
        if (c.policyService.isPostingAllowed(null)) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_200, "MODE READER");
        } else {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_201, "MODE READER");
        }
    }

    protected static Boolean handleNewgroups(ClientContext c) {
        String[] args = c.requestArgs;

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

        Date since;
        try {
            java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
            df.setLenient(false);
            df.setTimeZone(TimeZone.getTimeZone("UTC")); // treat times as UTC regardless; GMT means same here
            since = df.parse(dateStr + timeStr);
        } catch (Exception e) {
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
                        (metrics.getHighestArticleNumber() != null ? metrics.getHighestArticleNumber().getValue() : Spec.NoArticlesHighestNumber),
                        (metrics.getHighestArticleNumber() != null ? metrics.getLowestArticleNumber().getValue() : Spec.NoArticlesLowestNumber),
                        status));
            }

            // Terminate
            c.responseStream.write(".\r\n");
            c.responseStream.flush();
            return true;
        } catch (IOException e) {
            logger.error("Error writing NEWSGROUPS response: " + e.getMessage(), e);
            return false;
        } catch (Exception e) {
            logger.error("Error handling NEWSGROUPS: " + e.getMessage(), e);
            return false;
        }
    }

    protected static Boolean handleNewNews(ClientContext c) {
        String[] args = c.requestArgs;

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
        // Any remaining args are distributions; RFC allows them but this implementation ignores them.

        // Validate date/time formats: yyyymmdd and hhmmss
        if (dateStr == null || timeStr == null || dateStr.length() != 8 || timeStr.length() != 6) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
        }

        Date since;
        try {
            java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
            df.setLenient(false);
            df.setTimeZone(TimeZone.getTimeZone("UTC"));
            since = df.parse(dateStr + timeStr);
        } catch (Exception e) {
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
            c.responseStream.write(".\r\n");
            c.responseStream.flush();
            return true;
        } catch (IOException ioe) {
            logger.error("Error writing NEWNEWS response: " + ioe.getMessage(), ioe);
            return false;
        } catch (Exception e) {
            logger.error("Error handling NEWNEWS: " + e.getMessage(), e);
            return false;
        }
    }

    protected Boolean handleCapabilities(ClientContext c) {
        sendResponse(c.responseStream, NNTP_Response_Code.Code_101, "Capability list:");
        // dynamically determine the capabilities supported by the server
        String[] commands = dispatcher.getHandlerNames();
        try {
            c.responseStream.write("VERSION " + NNTP_VERSION + '\n');
            for (String command : commands) {
                c.responseStream.write(command + '\n');
            }
            c.responseStream.flush();
        } catch (IOException e) {
            logger.error("Error writing capabilities: {}", e.getMessage(), e);
            return false;
        }
        return true;
    }

    protected static Boolean handleGroup(ClientContext c) {
        String[] args = c.requestArgs;
        
        // GROUP command requires exactly one argument: the newsgroup name
        if (args.length != 2) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }
        
        String groupNameStr = args[1];
        
        try {
            // Validate and create newsgroup name
            Spec.NewsgroupName groupName = new Spec.NewsgroupName(groupNameStr);
            
            // Retrieve the newsgroup from the persistence service
            PersistenceService.Newsgroup newsgroup = c.persistenceService.getGroupByName(groupName);
            
            // Check if the newsgroup exists
            if (newsgroup == null || newsgroup.isIgnored()) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_411);  // no such newsgroup
            }
            
            // Set as current newsgroup
            c.currentGroup = newsgroup;
            
            // Get newsgroup metrics
            PersistenceService.NewsgroupMetrics metrics = newsgroup.getMetrics();
            
            // Send response: 211 count low high group
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_211,
                    metrics.getNumberOfArticles(),
                    metrics.getLowestArticleNumber(),
                    metrics.getHighestArticleNumber(),
                    groupName.getValue());
            
        } catch (Spec.NewsgroupName.InvalidNewsgroupNameException e) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error - invalid newsgroup name
        } catch (Exception e) {
            logger.error("Error in GROUP command: " + e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_403);  // internal fault
        }
    }

    protected static Boolean handleOverview(ClientContext c) {
        String[] args = c.requestArgs;

        try {
            // OVER with no arguments → use current article in current group
            if (args.length == 1) {
                if (c.currentGroup == null) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_412); // no newsgroup selected
                }
                if (c.currentGroup.getCurrentArticle() == null) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_420); // current article number invalid
                }

                // Send 224 then exactly one overview line and terminator
                if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_224)) {
                    return false;
                }

                PersistenceService.NewsgroupArticle a = c.currentGroup.getCurrentArticle();
                writeOverviewLine(c, a.getArticleNumber().getValue(), a.getArticle());
                c.responseStream.write(".\r\n");
                c.responseStream.flush();
                return true;

            } else if (args.length == 2) {
                String argument = args[1];

                // Message-ID form
                if (argument.startsWith("<") && argument.endsWith(">")) {
                    Spec.MessageId messageId;
                    try {
                        messageId = new Spec.MessageId(argument);
                    } catch (IllegalArgumentException | Spec.MessageId.InvalidMessageIdException e) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_430); // invalid message-id
                    }

                    PersistenceService.Article article = c.persistenceService.getArticle(messageId);
                    if (article == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_430); // no article with that message-id
                    }

                    // Determine article number within current group, if any
                    int numberForLine = 0;
                    if (c.currentGroup != null) {
                        Spec.ArticleNumber n = c.currentGroup.getArticle(messageId);
                        if (n != null) numberForLine = n.getValue();
                    }

                    if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_224)) {
                        return false;
                    }
                    writeOverviewLine(c, numberForLine, article);
                    c.responseStream.write(".\r\n");
                    c.responseStream.flush();
                    return true;

                } else {
                    // Range form requires a selected group
                    if (c.currentGroup == null) {
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
                            // Single number means that single article
                            low = Integer.parseInt(argument);
                            high = low;
                        }
                    } catch (NumberFormatException e) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
                    }

                    // Determine numeric bounds
                    Spec.ArticleNumber lowerBound;
                    Spec.ArticleNumber upperBound;
                    try {
                        if (low == null) {
                            // "-m" not supported: treat as syntax error per conservative approach
                            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
                        }
                        lowerBound = new Spec.ArticleNumber(low);

                        if (high == null) {
                            // n- → up to current group's highest
                            PersistenceService.NewsgroupMetrics metrics = c.currentGroup.getMetrics();
                            Spec.ArticleNumber highest = metrics.getHighestArticleNumber();
                            if (highest == null) {
                                return sendResponse(c.responseStream, NNTP_Response_Code.Code_423);
                            }
                            upperBound = highest;
                        } else {
                            upperBound = new Spec.ArticleNumber(high);
                        }
                    } catch (Spec.ArticleNumber.InvalidArticleNumberException e) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
                    }

                    Iterator<PersistenceService.NewsgroupArticle> it = c.currentGroup.getArticlesNumbered(lowerBound, upperBound);
                    if (it == null || !it.hasNext()) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_423); // no article in that range
                    }

                    if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_224)) {
                        return false;
                    }
                    while (it.hasNext()) {
                        PersistenceService.NewsgroupArticle nga = it.next();
                        writeOverviewLine(c, nga.getArticleNumber().getValue(), nga.getArticle());
                    }
                    c.responseStream.write(".\r\n");
                    c.responseStream.flush();
                    return true;
                }
            } else {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
            }

        } catch (IOException e) {
            logger.error("Error in OVER command: " + e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_403); // internal fault
        }
    }

    protected static Boolean handlePost(ClientContext c) {
        String[] args = c.requestArgs;

        // POST command requires exactly zero arguments
        if (args.length != 1) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        Subject submitter = null; // TODO obtain submitter's subject from the client

        if (!c.policyService.isPostingAllowed(submitter)) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_440); // posting is not allowed
        }

        try {
            // Article is wanted - request the client to send it
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_340)) {  // send article to be transferred
                return false;
            }

            // Read the article from the client (headers and body)
            StringBuilder articleText = new StringBuilder();
            String line;

            try {
                while ((line = c.requestStream.readLine()) != null) {
                    // Check for termination (single dot on a line)
                    if (".".equals(line)) {
                        break;
                    }

                    // Un-dot-stuff lines that start with two dots
                    if (line.startsWith("..")) {
                        line = line.substring(1);
                    }

                    articleText.append(line).append("\r\n");
                }
            } catch (IOException e) {
                logger.error("Error reading article: " + e.getMessage(), e);
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer failed
            }

            // Parse the article into headers and body
            String articleContent = articleText.toString();
            int headerBodySeparator = articleContent.indexOf("\r\n\r\n");

            if (headerBodySeparator == -1) {
                // Invalid article format - no separator between headers and body
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer rejected
            }

            String headersText = articleContent.substring(0, headerBodySeparator);
            String bodyText = articleContent.substring(headerBodySeparator + 4);

            // Validate the body text
            if (Spec.Article.isInvalidBody(bodyText)) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441); // transfer rejected
            }

            // Parse headers
            Map<String, Set<String>> headerMap = new HashMap<>();
            String[] headerLines = headersText.split("\r\n");

            for (String headerLine : headerLines) {
                if (headerLine.isEmpty()) continue;

                // Handle folded headers (continuation lines start with whitespace)
                if (headerLine.startsWith(" ") || headerLine.startsWith("\t")) {
                    // This is a continuation of the previous header - skip for simplicity
                    continue;
                }

                int colonIndex = headerLine.indexOf(':');
                if (colonIndex == -1) {
                    // Invalid header line
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer rejected
                }

                String headerName = headerLine.substring(0, colonIndex).trim();
                if (headerName.isEmpty() ||
                    headerName.equalsIgnoreCase(Spec.NNTP_Standard_Article_Headers.Lines.getValue()) ||
                    headerName.equalsIgnoreCase(Spec.NNTP_Standard_Article_Headers.Bytes.getValue())) {
                    // not interested in hearing about these headers
                    continue;
                }
                String headerValue = headerLine.substring(colonIndex + 1).trim();

                // header values may contain multiple comma-separated values
                String[] headerValues = headerValue.split(",");
                Set<String> headerValuesSet = new HashSet<>();
                for (String headerValueItem : headerValues) {
                    headerValuesSet.add(headerValueItem.trim());
                }
                if (!headerMap.containsKey(headerName)) {
                    headerMap.put(headerName, headerValuesSet);
                } else {
                    headerMap.get(headerName).addAll(headerValuesSet);
                }
            }

            // Create ArticleHeaders object
            Spec.Article.ArticleHeaders headers;
            try {
                headers = new Spec.Article.ArticleHeaders(headerMap);
            } catch (Spec.Article.ArticleHeaders.InvalidArticleHeaderException e) {
                logger.error("Invalid article headers: " + e.getMessage(), e);
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer rejected
            }

            // obtain messageID
            Spec.MessageId messageId = new Spec.MessageId(headers.getHeaderValue(Spec.NNTP_Standard_Article_Headers.MessageID.getValue()).iterator().next());

            // check to see if an article with this message-id already exists in the persistence service
            if (c.persistenceService.hasArticle(messageId)) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);    // transfer rejected
            }

            // Extract newsgroups from headers
            Set<String> newsgroupsHeader = headerMap.get(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue());
            if (newsgroupsHeader == null || newsgroupsHeader.isEmpty()) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer rejected
            }

            // Check policy for each newsgroup and add article to allowed newsgroups
            boolean addedToAnyGroup = false;

            for (String newsgroupName : newsgroupsHeader) {
                newsgroupName = newsgroupName.trim();

                try {
                    Spec.NewsgroupName groupName = new Spec.NewsgroupName(newsgroupName);

                    // Check if the newsgroup exists
                    PersistenceService.Newsgroup newsgroup = c.persistenceService.getGroupByName(groupName);
                    if (newsgroup == null || newsgroup.isIgnored()) {
                        // Newsgroup doesn't exist, or we don't track it anymore - skip it
                        continue;
                    }

                    // Check if the article is approved to be added to the newsgroup
                    if (newsgroup.getPostingMode().getValue() != Spec.PostingMode.Prohibited.getValue()) {
                        // Create a StringReader for the body
                        StringReader bodyReader = new StringReader(bodyText);

                        // Check policy if article is allowed in this newsgroup
                        // Note: For IHAVE, we use the peer policies (assuming the client is a peer)
                        // In a real implementation, you would get the actual peer identity
                        // For now, we'll check if policy service allows it
                        // (This is a simplified check - you may need to enhance this)
                        boolean isApproved = c.policyService.isArticleAllowed(messageId, headerMap, bodyReader, newsgroup.getName(), newsgroup.getPostingMode(), submitter);

                        // Add article to the newsgroup
                        try {
                            newsgroup.addArticle(messageId, headers, bodyReader, !isApproved);
                            addedToAnyGroup = true;
                        } catch (PersistenceService.Newsgroup.ExistingArticleException e) {
                            // Article already exists in this newsgroup - that's ok, skip it
                            logger.debug("Article already exists in newsgroup " + newsgroupName);
                        }
                    }
                } catch (Spec.NewsgroupName.InvalidNewsgroupNameException e) {
                    logger.warn("Invalid newsgroup name in article: " + newsgroupName);
                    // Continue with other newsgroups
                }
            }

            if (!addedToAnyGroup) {
                // Article wasn't added to any newsgroup
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer rejected
            }

            // Article successfully transferred
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_240);  // article transferred OK

        } catch (Spec.MessageId.InvalidMessageIdException e) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        } catch (Exception e) {
            logger.error("Error in IHAVE command: " + e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_441);  // transfer failed
        }
    }

    /**
     * Write one OVER/OVERVIEW line per RFC 3977 / XOVER compatibility.
     * Format (tab-separated):
     *  number TAB subject TAB from TAB date TAB message-id TAB references TAB bytes TAB lines
     * Missing optional fields are emitted as empty.
     */
    private static void writeOverviewLine(ClientContext c, int articleNumber, PersistenceService.Article article) throws IOException {
        Spec.Article.ArticleHeaders headers = article.getAllHeaders();

        String subject = headerFirst(headers, Spec.NNTP_Standard_Article_Headers.Subject.getValue());
        String from = headerFirst(headers, Spec.NNTP_Standard_Article_Headers.From.getValue());
        String date = headerFirst(headers, Spec.NNTP_Standard_Article_Headers.Date.getValue());
        String messageId = article.getMessageId() != null ? article.getMessageId().toString() :
                headerFirst(headers, Spec.NNTP_Standard_Article_Headers.MessageID.getValue());
        String references = headerJoined(headers, Spec.NNTP_Standard_Article_Headers.References.getValue());
        String bytes = headerFirst(headers, Spec.NNTP_Standard_Article_Headers.Bytes.getValue());
        String lines = headerFirst(headers, Spec.NNTP_Standard_Article_Headers.Lines.getValue());

        // Ensure non-null strings and strip CR/LF per overview constraints
        subject = sanitizeOverviewValue(subject);
        from = sanitizeOverviewValue(from);
        date = sanitizeOverviewValue(date);
        messageId = sanitizeOverviewValue(messageId);
        references = sanitizeOverviewValue(references);
        bytes = sanitizeOverviewValue(bytes);
        lines = sanitizeOverviewValue(lines);

        StringBuilder sb = new StringBuilder();
        sb.append(articleNumber).append('\t')
          .append(subject).append('\t')
          .append(from).append('\t')
          .append(date).append('\t')
          .append(messageId).append('\t')
          .append(references).append('\t')
          .append(bytes).append('\t')
          .append(lines)
          .append("\r\n");

        c.responseStream.write(sb.toString());
    }

    private static String headerFirst(Spec.Article.ArticleHeaders headers, String name) {
        if (headers == null) return "";
        Set<String> v = headers.getHeaderValue(name);
        if (v == null || v.isEmpty()) return "";
        // take deterministic first
        return v.iterator().next();
    }

    private static String headerJoined(Spec.Article.ArticleHeaders headers, String name) {
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

    private static String sanitizeOverviewValue(String s) {
        if (s == null) return "";
        // Remove CR and LF, and replace tabs with spaces to keep tab-separated format intact
        return s.replace("\r", " ").replace("\n", " ").replace('\t', ' ');
    }

    protected static Boolean handleXOver(ClientContext c) {
        // RFC 3977: XOVER is retained for backward compatibility as an alias of OVER
        // Delegate to the OVER/Overview handler
        return handleOverview(c);
    }

    /**
     * articleRequest handles the ARTICLE, HEAD, BODY and STAT command.
     * ARTICLE -> (sendHeaders, sendBody) == true, true
     * HEAD -> (sendHeaders, sendBody) == true, false
     * BODY -> (sendHeaders, sendBody) == false, true
     * STAT -> (sendHeaders, sendBody) == false, false
     *
     * @param c
     * @param sendHeaders
     * @param sendBody
     * @return
     */
    private static Boolean articleRequest(ClientContext c, boolean sendHeaders, boolean sendBody) {

        String[] args = c.requestArgs;

        PersistenceService.Article article = null;
        int articleNumberReply = 0;

        // obtain the article requested by the client
        if (args.length == 1) {
            // No argument provided - use the current article

            // Check if a newsgroup is currently selected
            if (c.currentGroup == null) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_412);  // no newsgroup selected
            }

            // Check if the current newsgroup has a current article
            if (c.currentGroup.getCurrentArticle() == null) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_420); // the current article number is invalid
            }

            article = c.currentGroup.getCurrentArticle().getArticle();
            articleNumberReply = c.currentGroup.getCurrentArticle().getArticleNumber().getValue();

        } else if (args.length == 2) {
            String argument = args[1];

            // Check if argument is a message-id (starts with '<' and ends with '>')
            if (argument.startsWith("<") && argument.endsWith(">")) {
                // Argument is a message-id
                try {
                    Spec.MessageId messageId = new Spec.MessageId(argument);

                    // Check if an article exists in the persistence service
                    article = c.persistenceService.getArticle(messageId);
                    if (article == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_430);  // no article with that message-id
                    }

                    // if there is a current newsgroup, see if this article belongs to it.  A side-effect of this will
                    // be to move the current article cursor of the newsgroup to the article.
                    if (c.currentGroup != null) {
                        // Get the article number corresponding to this article in the current newsgroup (if exists)
                        Spec.ArticleNumber n = c.currentGroup.getArticle(messageId);
                        if (n != null) {
                            articleNumberReply = n.getValue();
                        }
                    }
                } catch (IllegalArgumentException | Spec.MessageId.InvalidMessageIdException e) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_430);  // invalid message-id format
                }
            } else {
                // Argument is an article number
                try {
                    int articleNumber = Integer.parseInt(argument);
                    Spec.ArticleNumber articleNum = new Spec.ArticleNumber(articleNumber);

                    NewsgroupArticle numberedArticle = c.currentGroup.getArticleNumbered(articleNum);
                    if (numberedArticle == null) {
                        return sendResponse(c.responseStream, NNTP_Response_Code.Code_423);  // no article with that number
                    }
                    article = numberedArticle.getArticle();
                    articleNumberReply = articleNum.getValue();
                } catch (NumberFormatException | Spec.ArticleNumber.InvalidArticleNumberException e) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
                }
            }
        } else {
            // Too many arguments
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        if (article == null) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_423);  // no such article
        }

        // send the response line
        if (!sendResponse(c.responseStream,
                (sendHeaders)
                        // ARTICLE response (220).  HEAD response (221)
                        ? ((sendBody) ? NNTP_Response_Code.Code_220 : NNTP_Response_Code.Code_221)
                        // BODY response (222). STAT response (223)
                        : ((sendBody) ? NNTP_Response_Code.Code_222 : NNTP_Response_Code.Code_223),
                articleNumberReply,
                article.getMessageId())) {
            return false;
        }

        try {
            if (sendHeaders) {
                // Send headers
                Spec.Article.ArticleHeaders headers = article.getAllHeaders();
                if (headers != null) {
                    for (Map.Entry<String, Set<String>> header : headers.entrySet()) {
                        String headerName = header.getKey();
                        if (headerName == null) continue;
                        if (headerName.equalsIgnoreCase(Spec.NNTP_Standard_Article_Headers.Lines.getValue()) ||
                            headerName.equalsIgnoreCase(Spec.NNTP_Standard_Article_Headers.Bytes.getValue())) {
                            // TODO.  Skip.  Experimental
                            continue;
                        }
                        for (String headerValue : header.getValue()) {
                            c.responseStream.write(headerName + ": " + headerValue + "\r\n");
                        }
                    }
                } else {
                    // headers must be present for an article to be sent
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_500);
                }
            }

            if (sendHeaders && sendBody) {
                // Empty line separating headers from the Body
                c.responseStream.write("\r\n");
            }

            if (sendBody) {
                // Send body
                Reader bodyReader = article.getBody();
                if (bodyReader != null) {
                    char[] buffer = new char[8192];
                    int charsRead;
                /*
                 * Body is stored in database in transmission form
                while ((charsRead = bodyReader.read(buffer)) != -1) {
                    String chunk = new String(buffer, 0, charsRead);
                    // Dot-stuff lines that start with a dot (per RFC 3977)
                    chunk = chunk.replaceAll("(?m)^\\.", "..");
                    c.responseStream.write(chunk);
                }
                 */
                    while ((charsRead = bodyReader.read(buffer)) != -1) {
                        c.responseStream.write(buffer, 0, charsRead);
                    }
                    bodyReader.close();
                } else {
                    // a body must be present for an article to be sent
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_500);
                }
            }

            if (sendHeaders || sendBody) {
                // Send termination line (a single dot on a line by itself)
                c.responseStream.write("\r\n.\r\n");
                c.responseStream.flush();
            }
            return true;

        } catch (Exception e) {
            logger.error("Error sending article: {}", e.getMessage(), e);
            return false;
        }
    }


    protected static Boolean handleDate(ClientContext c) {
        // DATE command takes no arguments
        if (c.requestArgs.length > 1) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }
        
        // Get current date/time in UTC
        java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String dateTimeString = dateFormat.format(new Date());
        
        // Send response: 111 yyyyMMddHHmmss
        return sendResponse(c.responseStream, NNTP_Response_Code.Code_111, dateTimeString);
    }


    protected static Boolean handleHelp(ClientContext c) {
        // HELP command takes no arguments
        if (c.requestArgs.length > 1) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }

        try {
            // Send initial response: 100 help text follows
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_100)) {
                return false;
            }

            // Send help text describing available commands
            c.responseStream.write("The following commands are supported:\r\n");
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
            c.responseStream.write("Server: " + NNTP_SERVER + " " + NNTP_SERVER_VERSION + "\r\n");
            c.responseStream.write("NNTP Version: " + NNTP_VERSION + "\r\n");

            // Send termination line
            c.responseStream.write(".\r\n");
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
     * either marked as rejected or not depending on the policy service's decision.  Articles can subsequently be
     * marked as not rejected by the moderator at a later time.
     *
     * RFC-3977 on page 60 notes an example -
     *    "Note that the message-id in the IHAVE command is not the same as the one
     *    in the article headers; while this is bad practice and SHOULD NOT be
     *    done, it is not forbidden."
     *    In this implementation, the message-id in the IHAVE command MUST be the same as the one in the article headers
     *    because it makes no sense when they are different.
     * @param c
     * @return
     */
    protected static Boolean handleIHave(ClientContext c) {
        String[] args = c.requestArgs;
        
        // IHAVE command requires exactly one argument: the message-id
        if (args.length != 2) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        }
        
        String messageIdStr = args[1];

        Subject submitter = null; // TODO obtain submitter's subject from the client

        if (!c.policyService.isIHaveTransferAllowed(submitter)) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_437); // transfer rejected
        }

        try {
            // Validate message-id format
            Spec.MessageId messageId = new Spec.MessageId(messageIdStr);
            
            // Check if the article already exists in the persistence service
            if (c.persistenceService.hasArticle(messageId)) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_435);  // the article isn't wanted
            }
            
            // Check if the article was previously rejected
            Boolean isRejected = c.persistenceService.isRejectedArticle(messageId);
            if (isRejected != null && isRejected) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_435);  // the article isn't wanted
            }
            
            // Article is wanted - request the client to send it
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_335)) {  // send article to be transferred
                return false;
            }
            
            // Read the article from the client (headers and body)
            StringBuilder articleText = new StringBuilder();
            String line;
            
            try {
                while ((line = c.requestStream.readLine()) != null) {
                    // Check for termination (single dot on a line)
                    if (".".equals(line)) {
                        break;
                    }
                    
                    // Un-dot-stuff lines that start with two dots
                    if (line.startsWith("..")) {
                        line = line.substring(1);
                    }
                    
                    articleText.append(line).append("\r\n");
                }
            } catch (IOException e) {
                logger.error("Error reading article: {}", e.getMessage(), e);
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_436);  // transfer failed
            }
            
            // Parse the article into headers and body
            String articleContent = articleText.toString();
            int headerBodySeparator = articleContent.indexOf("\r\n\r\n");
            
            if (headerBodySeparator == -1) {
                // Invalid article format - no separator between headers and body
                c.persistenceService.rejectArticle(messageId);
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_437);  // transfer rejected
            }
            
            String headersText = articleContent.substring(0, headerBodySeparator);
            String bodyText = articleContent.substring(headerBodySeparator + 4);

            // Validate the body text
            if (Spec.Article.isInvalidBody(bodyText)) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_437); // transfer rejected
            }

            // Parse headers
            Map<String, Set<String>> headerMap = new HashMap<>();
            String[] headerLines = headersText.split("\r\n");
            
            for (String headerLine : headerLines) {
                if (headerLine.isEmpty()) continue;
                
                // Handle folded headers (continuation lines start with whitespace)
                if (headerLine.startsWith(" ") || headerLine.startsWith("\t")) {
                    // This is a continuation of the previous header - skip for simplicity
                    continue;
                }
                
                int colonIndex = headerLine.indexOf(':');
                if (colonIndex == -1) {
                    // Invalid header line
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_437);  // transfer rejected
                }
                
                String headerName = headerLine.substring(0, colonIndex).trim();
                String headerValue = headerLine.substring(colonIndex + 1).trim();

                // header values may contain multiple comma-separated values
                String[] headerValues = headerValue.split(",");
                Set<String> headerValuesSet = new HashSet<>();
                for (String headerValueItem : headerValues) {
                    headerValuesSet.add(headerValueItem.trim());
                }
                if (!headerMap.containsKey(headerName)) {
                    headerMap.put(headerName, headerValuesSet);
                } else {
                    headerMap.get(headerName).addAll(headerValuesSet);
                }
            }

            // Validate that the Message-ID header matches the message-id in the IHAVE command
            Set<String> messageIdHeaders = headerMap.get(Spec.NNTP_Standard_Article_Headers.MessageID.getValue());
            if (messageIdHeaders == null || messageIdHeaders.size() != 1 || 
                !messageIdHeaders.iterator().next().equals(messageIdStr)) {
                // Message-ID mismatch or missing
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_437);  // transfer rejected
            }
            
            // Create ArticleHeaders object
            Spec.Article.ArticleHeaders headers;
            try {
                headers = new Spec.Article.ArticleHeaders(headerMap);
            } catch (Spec.Article.ArticleHeaders.InvalidArticleHeaderException e) {
                logger.error("Invalid article headers: " + e.getMessage(), e);
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_437);  // transfer rejected
            }
            
            // Extract newsgroups from headers
            Set<String> newsgroupsHeader = headerMap.get(Spec.NNTP_Standard_Article_Headers.Newsgroups.getValue());
            if (newsgroupsHeader == null || newsgroupsHeader.isEmpty()) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_437);  // transfer rejected
            }

            // Check policy for each newsgroup and add article to allowed newsgroups
            boolean addedToAnyGroup = false;
            
            for (String newsgroupName : newsgroupsHeader) {
                newsgroupName = newsgroupName.trim();
                
                try {
                    Spec.NewsgroupName groupName = new Spec.NewsgroupName(newsgroupName);
                    
                    // Check if the newsgroup exists
                    PersistenceService.Newsgroup newsgroup = c.persistenceService.getGroupByName(groupName);
                    if (newsgroup == null || newsgroup.isIgnored()) {
                        // Newsgroup doesn't exist, or we don't track it anymore - skip it
                        continue;
                    }


                    // Check if the article is approved to be added to the newsgroup
                    if (newsgroup.getPostingMode().getValue() != Spec.PostingMode.Prohibited.getValue()) {
                        // Create a StringReader for the body
                        StringReader bodyReader = new StringReader(bodyText);

                        // Check policy if article is allowed in this newsgroup
                        // Note: For IHAVE, we use the peer policies (assuming the client is a peer)
                        // In a real implementation, you would get the actual peer identity
                        // For now, we'll check if policy service allows it
                        // (This is a simplified check - you may need to enhance this)
                        boolean isApproved = c.policyService.isArticleAllowed(messageId, headerMap, bodyReader, newsgroup.getName(), newsgroup.getPostingMode(), submitter);

                        // Add article to the newsgroup
                        try {
                            newsgroup.addArticle(messageId, headers, bodyReader, !isApproved);
                            addedToAnyGroup = true;
                        } catch (PersistenceService.Newsgroup.ExistingArticleException e) {
                            // Article already exists in this newsgroup - that's ok, skip it
                            logger.debug("Article already exists in newsgroup " + newsgroupName);
                        }
                    }
                } catch (Spec.NewsgroupName.InvalidNewsgroupNameException e) {
                    logger.warn("Invalid newsgroup name in article: " + newsgroupName);
                    // Continue with other newsgroups
                }
            }
            
            if (!addedToAnyGroup) {
                // Article wasn't added to any newsgroup
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_437);  // transfer rejected
            }
            
            // Article successfully transferred
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_235);  // article transferred OK
            
        } catch (Spec.MessageId.InvalidMessageIdException e) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
        } catch (Exception e) {
            logger.error("Error in IHAVE command: " + e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_436);  // transfer failed
        }
    }

    protected static Boolean handleLast(ClientContext c) {
        if (c.currentGroup != null) {
            if (c.currentGroup.getCurrentArticle() != null) {
                NewsgroupArticle a = c.currentGroup.gotoPreviousArticle();  // go to the previous newsgroup article
                if (a != null) {
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

    protected static Boolean handleList(ClientContext c) {
        String[] args = c.requestArgs;

        // LIST command can have optional arguments (LIST ACTIVE, LIST NEWSGROUPS, etc.)

        // the default LIST command is LIST ACTIVE
        String subCommand = (args.length == 1) ? "ACTIVE" : args[1].toUpperCase();

        // Handle LIST ACTIVE (or just LIST with no arguments)
        if ("ACTIVE".equals(subCommand)) {
            if (args.length > 2) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);  // syntax error
            }

            try {
                // Get all newsgroups
                Iterator<Newsgroup> groups = c.persistenceService.listAllGroups(false, false);

                // Send initial response
                if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_215)) {
                    return false;
                }

                // Send each newsgroup in the format: group high low status
                while (groups.hasNext()) {
                    PersistenceService.Newsgroup group = groups.next();
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
                            (metrics.getHighestArticleNumber() != null ? metrics.getHighestArticleNumber().getValue() : -1),
                            (metrics.getHighestArticleNumber() != null ? metrics.getLowestArticleNumber().getValue() : 0),
                            status));
                }

                // Send termination line
                c.responseStream.write(".\r\n");
                c.responseStream.flush();
                return true;

            } catch (Exception e) {
                logger.error("Error in LIST command: " + e.getMessage(), e);
                return false;
            }
        } else if ("NEWSGROUPS".equals(subCommand)) {
            // check to see if NEWSGROUPS include a wildmat qualifier in args[2]: LIST NEWSGROUPS *.*.java
            WildMatcher matcher = null;
            if (args.length >= 3) {
                // extract wildmat expression
                String wildmat = String.join(",", Arrays.copyOfRange(args, 2, args.length));
                try {
                    matcher = new WildMatcher(wildmat);
                } catch (IllegalArgumentException e) {
                    logger.error("Invalid wildmat expression: " + wildmat);
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_501);
                }
            }

            // Get all newsgroups
            Iterator<Newsgroup> groups = c.persistenceService.listAllGroups(false, false);

            try {
                // Send initial response
                if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_215)) {
                    return false;
                }

                // Send each newsgroup in the format: group high low status
                while (groups.hasNext()) {
                    PersistenceService.Newsgroup group = groups.next();

                    if (matcher == null || matcher.matches(group.getName().getValue())) {

                        PersistenceService.NewsgroupMetrics metrics = group.getMetrics();

                        // Format: groupname description
                        c.responseStream.write(String.format("%s\t%s\r\n",
                                group.getName().getValue(),
                                group.getDescription()));
                    }
                }

                // Send termination line
                c.responseStream.write(".\r\n");
                c.responseStream.flush();
                return true;
            } catch(IOException e){
                logger.error("Error in LIST command: " + e.getMessage(), e);
                throw new RuntimeException(e);
            }
        } else {
            // Unsupported LIST variant
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_503);  // feature not supported
        }
    }

    protected static Boolean handleListGroup(ClientContext c) {
        String[] args = c.requestArgs;

        // LISTGROUP [newsgroup]
        if (args.length > 2) {
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
        }

        PersistenceService.Newsgroup targetGroup = null;

        if (args.length == 2) {
            // Use the provided newsgroup argument (do not change currentGroup per RFC)
            try {
                Spec.NewsgroupName groupName = new Spec.NewsgroupName(args[1]);
                targetGroup = c.persistenceService.getGroupByName(groupName);
                if (targetGroup == null || targetGroup.isIgnored()) {
                    return sendResponse(c.responseStream, NNTP_Response_Code.Code_411); // no such newsgroup
                }
            } catch (Spec.NewsgroupName.InvalidNewsgroupNameException e) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_501); // syntax error
            }
        } else {
            // No argument: must have a currently selected group
            if (c.currentGroup == null) {
                return sendResponse(c.responseStream, NNTP_Response_Code.Code_412); // no newsgroup selected
            }
            targetGroup = c.currentGroup;
        }

        try {
            PersistenceService.NewsgroupMetrics metrics = targetGroup.getMetrics();

            // Initial response: 211 count low high group
            if (!sendResponse(c.responseStream, NNTP_Response_Code.Code_211,
                    metrics.getNumberOfArticles(),
                    metrics.getLowestArticleNumber(),
                    metrics.getHighestArticleNumber(),
                    targetGroup.getName().getValue())) {
                return false;
            }

            // Now list article numbers, one per line, then terminating dot
            Spec.ArticleNumber low = metrics.getLowestArticleNumber();
            Spec.ArticleNumber high = metrics.getHighestArticleNumber();

            if (low != null && high != null && metrics.getNumberOfArticles() > 0) {
                Iterator<PersistenceService.NewsgroupArticle> it =
                        targetGroup.getArticlesNumbered(low, high);
                while (it.hasNext()) {
                    PersistenceService.NewsgroupArticle a = it.next();
                    c.responseStream.write(Integer.toString(a.getArticleNumber().getValue()));
                    c.responseStream.write("\r\n");
                }
            }

            c.responseStream.write(".\r\n");
            c.responseStream.flush();
            return true;
        } catch (IOException e) {
            logger.error("Error writing LISTGROUP response: {}", e.getMessage(), e);
            return false;
        } catch (Exception e) {
            logger.error("Error handling LISTGROUP: {}", e.getMessage(), e);
            return sendResponse(c.responseStream, NNTP_Response_Code.Code_403);
        }
    }

    protected static Boolean handleNext(ClientContext c) {
        if (c.currentGroup != null) {
            if (c.currentGroup.getCurrentArticle() != null) {
                NewsgroupArticle a = c.currentGroup.gotoNextArticle();  // proceed to the next newsgroup article
                if (a != null) {
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

    protected static Boolean handleQuit(ClientContext c) {
        return sendResponse(c.responseStream, NNTP_Response_Code.Code_205);    // end of NNTP session
    }


    // every NNTP Client connection is given its own context object.  None of these services are shared between clients.
    protected static class ClientContext {
        private final PersistenceService persistenceService;
        private final IdentityService identityService;
        private final PolicyService policyService;

        private final BufferedReader requestStream;
        BufferedWriter responseStream;

        protected Newsgroup currentGroup;

        protected Long authenticationToken;
        String[] requestArgs;  // Store the parsed request arguments

        ClientContext(PersistenceService persistenceService, IdentityService identityService,
                      PolicyService policyService, BufferedReader requestStream,
                      BufferedWriter responseStream) {
            this.persistenceService = persistenceService;
            this.identityService = identityService;
            this.policyService = policyService;
            this.requestStream = requestStream;
            this.responseStream = responseStream;
        }
    }

    private void setCommandHandlers(CommandDispatcher dispatcher) {
        dispatcher.clearHandlers();

        dispatcher.addHandler(Spec.NNTP_Request_Commands.CAPABILITIES, this::handleCapabilities);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.ARTICLE, Server::handleArticle);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.BODY, Server::handleBody);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.DATE, Server::handleDate);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.GROUP, Server::handleGroup);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.HEAD, Server::handleHead);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.HELP, Server::handleHelp);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.IHAVE, Server::handleIHave);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.LAST, Server::handleLast);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.LIST, Server::handleList);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.LISTGROUP, Server::handleListGroup);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.MODE, Server::handleMode);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.NEWGROUPS, Server::handleNewgroups);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.NEWNEWS, Server::handleNewNews);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.NEXT, Server::handleNext);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.OVERVIEW, Server::handleOverview);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.POST, Server::handlePost);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.QUIT, Server::handleQuit);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.STAT, Server::handleStat);
        dispatcher.addHandler(Spec.NNTP_Request_Commands.XOVER, Server::handleXOver);
    }

    static private class CommandDispatcher {
        private static final Logger logger = LoggerFactory.getLogger(CommandDispatcher.class);

        // nntpCommandHandlers is a map of NNTP command names to their corresponding handlers.  The handlers read an
        // input stream and write an output stream.  The handlers return true if the operation was successful, implying
        // that further stream processing should be done by other handlers; false otherwise (e.g. due to errors, etc).
        private final EnumMap<Spec.NNTP_Request_Commands, Function<ClientContext, Boolean>> nntpCommandHandlers = new EnumMap<>(Spec.NNTP_Request_Commands.class);

        private final ClientContext clientContext;
        static final Spec.NNTP_Request_Commands DEFAULT_HANDLER = null;

        CommandDispatcher(ClientContext clientContext) {
            this.clientContext = clientContext;
        }

        void clearHandlers() {
            nntpCommandHandlers.clear();
        }

        /**
         * Returns the names of the handlers that were added to this dispatcher.
         * @return
         */
        String[] getHandlerNames() {
            return nntpCommandHandlers.keySet()
                    .stream()
                    .map(Enum::name)
                    .toArray(String[]::new);
        }

        private void addHandler(Spec.NNTP_Request_Commands name, Function<ClientContext, Boolean> func) {
            nntpCommandHandlers.put(name, func);
        }

        /**
         * ApplyHandler dispatches a request to the appropriate handler.  The handler is identified by the first
         * argument in the request.  If no matching handler is found for the command, the default handler is used.
         *
         * @param args request (first word) and its arguments (subsequent words in the request line)
         * @return true if the request was processed successfully, false otherwise
         */
        private boolean applyHandler(String[] args) {
            String name = (args != null && 1 <= args.length) ? args[0].toUpperCase() : null;

            try {
                Spec.NNTP_Request_Commands command = Spec.NNTP_Request_Commands.valueOf(name);
                clientContext.requestArgs = args;  // Store arguments for handlers to access
                return nntpCommandHandlers.get(command).apply(clientContext);
            } catch (IllegalArgumentException e) {
                sendResponse(clientContext.responseStream, NNTP_Response_Code.Code_500); // unrecognized command
                return false;
            }
        }
    }

    /**
     * SendResponse writes a response to the response stream.
     *
     * @param responseStream the stream to write to
     * @param response       the response code to write (e.g. NNTP_Response_Code.Code_200)
     * @param args           and additional arguments to write (e.g. article number) on the response line
     * @return true if the response was written successfully, false otherwise
     */
    protected static boolean sendResponse(BufferedWriter responseStream, NNTP_Response_Code response, Object... args) {
        try {
            responseStream.write(String.valueOf(response)); // write the NNTP response code
            for (Object arg : args) {
                responseStream.write(" " + arg.toString()); // write out each of the variable arguments (if any)
            }
            responseStream.write("\r\n");   // terminate the response line
            responseStream.flush(); // send it
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }
}

package org.anarplex.lib.nntp;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.util.*;
import java.util.stream.Collectors;

import static org.anarplex.lib.nntp.utils.DateAndTime.parseRFC3977Date;

public class Specification {

    private Specification() {
        super();
    }

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Specification.class);

    public static final String HeaderNameFieldDelimiter = ": ";
    public static final String HeaderValueFieldSeparator = ", ";
    public static final String CRLF = "\r\n";
    public static final String DOT_CRLF = "."+CRLF;
    public static final String CRLF_DOT_CRLF = CRLF+DOT_CRLF;

    /*
     * NNTP Commands according to RFC3977
     * CAPABILITIES: Used to query the server for its supported features and capabilities.
     * QUIT: Terminates the session with the server.
     * ARTICLE: Requests an article by message ID or article number.
     * BODY: Requests the body of an article by message ID or article number.
     * HEAD: Requests the header of an article by message ID or article number.
     * HELP: Requests help information from the server.
     * LIST: Retrieves various lists such as active newsgroups, overview formats, headers, and more.
     * MODE READER: Switches the server to reader mode, enabling access to reading commands.
     * POST: Sends an article to the server for posting.
     * IHAVE: Offers an article to a server, used primarily in peer-to-peer news distribution.
     * GROUP: Selects a newsgroup for reading or posting.
     * STAT: Requests information about an article by message ID or article number.
     * DATE: Requests the current date and time from the server.
     * NEWNEWS: Requests a list of new articles in specified newsgroups since a given date.
     * OVER: Requests overview information for articles in a newsgroup.
     * HDR: Requests specific header fields for articles in a newsgroup.
     * XHDR: A variant of HDR that allows for more flexible header retrieval.
     * XOVER: A variant of OVER that provides overview data in a different format.
     * XGTITLE: Retrieves the title of a newsgroup.
     * XPOST: A variant of POST used for posting articles to a specific newsgroup.
     * XHDR: A command for retrieving specific header fields from articles.
     */
    public enum NNTP_Request_Commands {
        ARTICLE("ARTICLE"),
        AUTHINFO("AUTHINFO"),
        BODY("BODY"),
        CAPABILITIES("CAPABILITIES"),
        DATE("DATE"),
        GROUP("GROUP"),
        HEAD("HEAD"),
        HELP("HELP"),
        IHAVE("IHAVE"),
        LAST("LAST"),
        LIST("LIST"),
        LIST_ACTIVE("LIST ACTIVE"),
        LIST_ACTIVE_TIMES("LIST ACTIVE.TIMES"),
        LIST_DISTRIBUTION_PATS("LIST DISTRIBUTION.PATS"),
        LIST_NEWSGROUPS("LIST NEWSGROUPS"),
        LIST_OVERVIEW_FMT("LIST OVERVIEW.FMT"),
        HDR("HDR"),
        LIST_HEADERS("LIST HEADERS"),   // distinct from LIST because it belongs to a different capability - HDR
        LIST_GROUP("LISTGROUP"),
        MODE_READER("MODE READER"),
        NEW_GROUPS("NEWGROUPS"),
        NEW_NEWS("NEWNEWS"),
        NEXT("NEXT"),
        OVERVIEW("OVER"),
        POST("POST"),
        QUIT("QUIT"),
        STAT("STAT"),
        XOVER("XOVER");

        NNTP_Request_Commands(String value) { this.value = normalize(value); }

        /**
         * Return the Command whose value matches the supplied value, or null if no match is found.
         * The algorithm chooses the Command whose value is the longest match. e.g., LIST.ACTIVE.TIME over LIST ACTIVE
         */
        static public NNTP_Request_Commands getCommand(String value) {
            // normalize input value
            String normalizedValue = normalize(value);
            for (NNTP_Request_Commands command : sortedValues) {
                if (normalizedValue.startsWith(command.getValue())) {
                    return command;
                }
            }
            return null;
        }

        public String getValue() { return value; }
        public String toString() { return getValue(); }

        /**
         * Normalize the supplied string to upper-case and trims all leading, trailing, and in-between whitespace to
         * one space character.
         * For comparison purposes, values need to be in normal form.
         */
        private static String normalize(String value) {
            String[] words = value.split("\\s+"); // split string on whitespace boundaries
            return String.join(" ", words).toUpperCase();  // rejoin with one space character between words and convert to uppercase
        }

        // calculate the sorted values array once, at class load time, as it never changes during program execution
        private static final NNTP_Request_Commands[] sortedValues;
        static {
            sortedValues = Arrays.stream(NNTP_Request_Commands.values())
                    .sorted(Comparator.comparingInt((NNTP_Request_Commands c) -> c.getValue().length()).reversed())
                    .toArray(NNTP_Request_Commands[]::new);
        }

        private final String value;
    }

    /**
     * NNTP Capabilities and their required Commands according to RFC-3977.
     * Note that the commands: CAPABILITIES, HEAD, HELP, QUIT, and STAT are all mandatory and thus don't have a
     * corresponding (explicit) capability.
     * The string value of each capability is its formal expression, according to RFC-3977.  The set of NNTP Commands
     * for each capability are those commands that MUST be available to the client in order for the server to claim
     * current support for that capability.
     */
    public enum NNTP_Server_Capabilities {
        _MANDATORY(null, // not an explicit capability, but a conceptual one
                Set.of(NNTP_Request_Commands.CAPABILITIES,
                        NNTP_Request_Commands.HEAD,
                        NNTP_Request_Commands.HELP,
                        NNTP_Request_Commands.QUIT,
                        NNTP_Request_Commands.STAT)),
        HDR("HDR",
                Set.of(NNTP_Request_Commands.HDR,
                        NNTP_Request_Commands.LIST_HEADERS)),
        I_HAVE("IHAVE",
                Set.of(NNTP_Request_Commands.IHAVE)),
        LIST("LIST",
                Set.of(NNTP_Request_Commands.LIST,
                        NNTP_Request_Commands.LIST_ACTIVE,
                        NNTP_Request_Commands.LIST_ACTIVE_TIMES,
                        NNTP_Request_Commands.LIST_DISTRIBUTION_PATS,
                        NNTP_Request_Commands.LIST_NEWSGROUPS)),
        MODE_READER("MODE-READER",
                Set.of(NNTP_Request_Commands.MODE_READER)),
        NEW_NEWS("NEWNEWS",
                Set.of(NNTP_Request_Commands.NEW_NEWS)),
        OVER("OVER",
                Set.of(NNTP_Request_Commands.OVERVIEW,
                        NNTP_Request_Commands.XOVER,
                        NNTP_Request_Commands.LIST_OVERVIEW_FMT)),
        POST("POST",
                Set.of(NNTP_Request_Commands.POST)),
        READER("READER",
                Set.of(NNTP_Request_Commands.ARTICLE,
                        NNTP_Request_Commands.BODY,
                        NNTP_Request_Commands.DATE,
                        NNTP_Request_Commands.GROUP,
                        NNTP_Request_Commands.LAST,
                        NNTP_Request_Commands.LIST_GROUP,
                        NNTP_Request_Commands.LIST_NEWSGROUPS,
                        NNTP_Request_Commands.NEW_GROUPS,
                        NNTP_Request_Commands.NEXT));

        private final String value;
        private final Set<NNTP_Request_Commands> requisiteCommands;
        NNTP_Server_Capabilities(String value, Set<NNTP_Request_Commands> requisiteCommands) { this.value = value; this.requisiteCommands = requisiteCommands; }
        public String getValue() { return value; }
        public String toString() { return getValue(); }
        public static boolean contains(String v) { return EnumSet.allOf(NNTP_Server_Capabilities.class).stream().anyMatch(e -> e.getValue().equalsIgnoreCase(v)); }
        /**
         * Returns true if all the commands associated with this capability are found in the supplied set of commands
         */
        public boolean isSufficientSet(Set<NNTP_Request_Commands> commandSet) { return commandSet.containsAll(this.requisiteCommands); }

        /**
         * Returns true if the supplied dispatcherCommand is required by this capability.
         */
        public boolean isRequiredCommand(NNTP_Request_Commands dispatcherCommand) { return this.requisiteCommands.contains(dispatcherCommand); }
    }



    public enum NNTP_Response_Code {
        Code_100(100), // Generated by: HELP.  Meaning: help text follows.
        Code_101(101), // Generated by: CAPABILITIES.  Meaning: capabilities list follows.
        Code_111(111), // Generated by: DATE.  articles.ddl argument: yyyymmddhhmmss.  Meaning: server date and time.
        Code_200(200), // Generated by: initial connection, MODE READER.  Meaning: service available, posting allowed.
        Code_201(201), // Generated by: initial connection, MODE READER.  Meaning: service available, posting prohibited.
        Code_205(205), // Generated by: QUIT.  Meaning: connection closing (the server immediately closes the connection).
        Code_211(211), // Generated either by: GROUP with 4 arguments (number low high group) meaning: group selected, or (multi-line) by: LISTGROUP with 4 arguments (number low high group) meaning: article numbers follow.
        Code_215(215), // Generated by: LIST.  Meaning: information follows.
        Code_220(220), // Generated by: ARTICLE msgs.ddl arguments: n message-id.  Meaning: article follows.
        Code_221(221), // Generated by: HEAD.  msgs.ddl arguments: n message-id.  Meaning: article headers follow.
        Code_222(222), // Generated by: BODY.  msgs.ddl arguments: n message-id.  Meaning: article body follows.
        Code_223(223), // Generated by: LAST, NEXT, STAT.  msgs.ddl arguments: n message-id.  Meaning: article exists and selected.
        Code_224(224), // Generated by: OVER.  Meaning: overview information follows.
        Code_225(225), // Generated by: HDR.  Meaning: headers follow.
        Code_230(230), // Generated by: NEWNEWS.  Meaning: list of new articles follows.
        Code_231(231), // Generated by: NEWGROUPS.  Meaning: list of new newsgroups follows.
        Code_235(235), // Generated by: IHAVE (second stage).  Meaning: article transferred OK.
        Code_240(240), // Generated by: POST (second stage).  Meaning: article received OK.
        Code_281(281), // Successful authentication using the AUTHINFO command extension.
        Code_335(335), // Generated by: IHAVE (first stage) Meaning: send article to be transferred
        Code_340(340), // Generated by: POST (first stage) Meaning: send article to be posted.
        Code_381(381), // RFC-2980: More authentication information required
        Code_400(400), // Generic response and generated by initial connection. Meaning: service not available or no longer available (the server immediately closes the connection).
        Code_403(403), // Generic response. Meaning: internal fault or problem preventing action being taken.
        Code_411(411), // Generated by: GROUP, LISTGROUP. Meaning: no such newsgroup.
        Code_412(412), // Generated by: ARTICLE, BODY, GROUP, HDR, HEAD, LAST, LISTGROUP, NEXT, OVER, STAT.  Meaning: no newsgroup selected.
        Code_420(420), // Generated by: ARTICLE, BODY, HDR, HEAD, LAST, NEXT, OVER, STAT.  Meaning: current article number is invalid.
        Code_421(421), // Generated by: NEXT.  Meaning: no next article in this group.
        Code_422(422), // Generated by: LAST.  Meaning: no previous article in this group.
        Code_423(423), // Generated by: ARTICLE, BODY, HDR, HEAD, OVER, STAT.  Meaning: no article with that number or in that range.
        Code_430(430), // Generated by: ARTICLE, BODY, HDR, HEAD, OVER, STAT.  Meaning: no article with that message-id.
        Code_435(435), // Generated by: IHAVE (first stage).  Meaning: article not wanted.
        Code_436(436), // Generated by: IHAVE (either stage).  Meaning: transfer not possible (first stage) or failed (second stage); try again later.
        Code_437(437), // Generated by: IHAVE (second stage).  Meaning: transfer rejected; do not retry.
        Code_440(440), // Generated by: POST (first stage).  Meaning: posting not permitted.
        Code_441(441), // Generated by: POST (second stage).  Meaning: posting failed.
        Code_480(480), // Generic response.  Meaning: command unavailable until the client has authenticated itself.
        Code_481(481), // RFC-4643: Authentication failed/rejected
        Code_482(482), // RFC-2980: Authentication rejected.  RFC-4643: Authentication commands issued out of sequence
        Code_483(483), // Generic response.  Meaning: command unavailable until suitable privacy has been arranged.
        Code_500(500), // Generic response.  Meaning: unknown command.
        Code_501(501), // Generic response.  Meaning: syntax error in command.
        Code_502(502), // Generic response and generated by initial connection.  Meaning for the initial connection and the MODE READER command: service permanently unavailable (the server immediately closes the connection).  Meaning for all other commands: command not permitted (and there is no way for the client to change this).
        Code_503(503), // Generic response.  Meaning: feature not supported.
        Code_504(504);  // Generic response.  Meaning: error in base64-encoding [RFC4648] of an argument

        private final int code;

        NNTP_Response_Code(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public String toString() {
            return Integer.toString(code);
        }

        public static NNTP_Response_Code findByCode(int code) {
            for (NNTP_Response_Code s : NNTP_Response_Code.values()) {
                if (s.code == code) {
                    return s;
                }
            }
            logger.warn("NNTP_Response_Code.findByCode: unknown code: {}", code);
            return null;
        }
    }

    public enum NNTP_Standard_Article_Headers {
        // mandatory headers for every article, in the order prescribed by RFC-3977 Sec 8.3 - Over Command.
        Subject("Subject"),
        From("From"),
        Date("Date"),
        MessageID("Message-Id"),
        References("References"),
        Bytes(":bytes"),
        Lines(":lines"),
        Newsgroups("Newsgroups"),
        Path("Path");

        private final String value;

        NNTP_Standard_Article_Headers(String value) {
            this.value = value.toLowerCase();
        }

        public String toString() {
            return value;
        }

        public String getValue() {
            return value;
        }

        public boolean isMandatory() {
            return this != NNTP_Standard_Article_Headers.Bytes && this != NNTP_Standard_Article_Headers.Lines && this != NNTP_Standard_Article_Headers.References;
        }

        public static boolean contains(String v) {
            return EnumSet.allOf(NNTP_Standard_Article_Headers.class).stream().anyMatch(e -> e.getValue().equalsIgnoreCase(v));
        }
    }

    /**
     * Newsgroup name.
     * All newsgroup names are case-insensitive and are converted to lower case.
     */
    public static class NewsgroupName implements Serializable {
        private final static int NewsgroupNameMaxLen = 1024;  // (practical limit.  not from spec)
        private final String name;

        public static class InvalidNewsgroupNameException extends Exception {
            public InvalidNewsgroupNameException(String message) {
                super(message);
            }
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof NewsgroupName that)) return false;

            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        /**
         * Create a new NewsgroupName.  Throws an IllegalArgumentException if the name is invalid.
         */
        public NewsgroupName(String name) throws InvalidNewsgroupNameException {
            if (isValid(name)) {
                this.name = name.toLowerCase(); // convert to lower case
            } else {
                throw new InvalidNewsgroupNameException("Invalid newsgroup name: " + name);
            }
        }

        /**
         * Always returns lowercase variant of the name.
         */
        public String getValue() {
            return name;
        }

        /**
         * isLocal returns true if the newsgroup name is a local newsgroup name: i.e. the compound name begins
         * with "local."
         * NOTE.  This IS NOT part of the RFC-3977 specification.  It is used to create newsgroups whose articles will
         * never be shared with other NNTP Peers, although NNTP Clients will be allowed access.
         */
        public static boolean isLocal(NewsgroupName name) {
            return (name != null && name.getValue().startsWith("local."));
        }

        @Override
        public String toString() { return getValue();}

        /**
         * Check if the name is valid.
         * According to RFC 3977, a newsgroup name must consist of dot-separated components,
         * where each component contains one or more letters, digits, hyphens (-), plus signs (+), or underscores (_).
         * Also, there must be at least one component, and the maximum length of the name is 1024 characters.
         */
        public static boolean isValid(String name) {
            return name != null
                    && !name.isEmpty()
                    && name.length() <= NewsgroupNameMaxLen
                    && name.charAt(0) != '.'                    // does not start with a dot
                    && name.charAt(name.length() - 1) != '.'    // does not end with a dot
                    && !name.contains("..")                     // does not contain two consecutive dots
                    && name.matches("^[a-zA-Z0-9\\-+_.]+$");
        }


    }

    public enum PostingMode {
        Prohibited(0),
        Allowed(1),
        Moderated(2);

        private final int value;

        PostingMode(final int value) {
            this.value = value;
        }

        public static PostingMode valueOf(int anInt) {
            return Arrays.stream(PostingMode.values())
                    .filter(v -> v.getValue() == anInt)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid PostingMode: " + anInt));
        }

        public int getValue() {
            return value;
        }

        public String toString() {
            return  Integer.toString(getValue());
        }
    }

    public static class MessageId implements Comparable<MessageId>, Serializable {
        private final static int MessageIdMaxLen = 250;
        private final String messageId;

        @Override
        public int compareTo(MessageId o) {
            return this.messageId.compareTo(o.messageId);
        }

        static public class InvalidMessageIdException extends Exception {
            public InvalidMessageIdException(String message) {
                super(message);
            }
        }

        public MessageId(String messageId) throws InvalidMessageIdException {
            if (isValid(messageId)) {
                this.messageId = messageId;
            } else {
                throw new InvalidMessageIdException("Invalid message id: " + messageId);
            }
        }

        public String getValue() {
            return messageId;
        }

        public String toString() {
            return getValue();
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof MessageId messageId1)) return false;

            return messageId.equals(messageId1.messageId);
        }

        @Override
        public int hashCode() {
            return messageId.hashCode();
        }

        public static boolean isValid(String id) {
            return id != null &&
                    id.length() >= 3 &&
                    id.length() <= MessageIdMaxLen &&
                    id.indexOf('<') == 0 && // the first character is the opening delimiter
                    id.indexOf('>') == id.length() - 1 &&  // the closing delimiter is found only at the end of the id
                    StringUtils.isAsciiPrintable(id);   // all characters must be printable ASCII
        }
    }

    public static class ArticleNumber implements Serializable {
        final int number;

        static public class InvalidArticleNumberException extends Exception {
            public InvalidArticleNumberException(String message) {
                super(message);
            }
        }

        public ArticleNumber(int number) throws InvalidArticleNumberException {
            this.number = number;
            if (!isValid(number)) {
                throw new InvalidArticleNumberException("Invalid article number: " + number);
            }
        }

        public int getValue() {
            return this.number;
        }

        public static boolean isValid(int number) {
            return 1 <= number || number == NoArticlesLowestNumber || number == NoArticlesHighestNumber;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof ArticleNumber that)) return false;

            return number == that.number;
        }

        @Override
        public int hashCode() {
            return number;
        }

        @Override
        public String toString() { return Integer.toString(getValue()); }
    }

    // The lowest number to be returned when a newsgroup does not have any articles.  MUST be less than 1.
    final static int NoArticlesLowestNumber = 0;    // by NNTP convention this value is 0 but that choice not mandatory
    // The highest number to be returned when a newsgroup does not have any articles.  MUST be less than 0.
    final static int NoArticlesHighestNumber = -1;  // by NNTP convention this value is -1 but that choice not mandatory

    public static class NoArticlesLowestNumber extends ArticleNumber {
        public NoArticlesLowestNumber() throws InvalidArticleNumberException {
            super(NoArticlesLowestNumber);
        }
        public static NoArticlesLowestNumber getInstance() {
            NoArticlesLowestNumber result = null;
            try {
                result = new NoArticlesLowestNumber();
            } catch (InvalidArticleNumberException e) {
                // its not wrong when we do it :)
            }
            return result;
        }
    }

    public static class NoArticlesHighestNumber extends ArticleNumber {
        public NoArticlesHighestNumber() throws InvalidArticleNumberException {
            super(NoArticlesHighestNumber);
        }
        public static NoArticlesHighestNumber getInstance() {
            NoArticlesHighestNumber result = null;
            try {
                result = new NoArticlesHighestNumber();
            } catch (InvalidArticleNumberException e) {
                // its not wrong when we do it :)
            }
            return result;
        }
    }

    public static class Article implements Serializable {
        protected final MessageId messageId;
        protected final ArticleHeaders headers;
        protected final String body;

        protected Article(MessageId messageId, ArticleHeaders headers, String body) {
            this.messageId = messageId;
            this.headers = headers;
            this.body = body;

            if (headers == null || body == null) {
                throw new IllegalArgumentException("Article must have headers and body");
            }

            // add :lines header field if not present
            headers.headerFields.putIfAbsent(NNTP_Standard_Article_Headers.Lines.getValue(),
                    Set.of(String.valueOf(body.lines().count())));

            // add :bytes header field if not present
            headers.headerFields.putIfAbsent(Specification.NNTP_Standard_Article_Headers.Bytes.getValue(),
                    Set.of(String.valueOf(body.getBytes(StandardCharsets.UTF_8).length)));
        }

        public ArticleHeaders getAllHeaders() {
            return headers;
        }

        /**
         * Returns the body in a stream suitable for sending directly to the client.  This is the same format as
         * was read-in from a POST or IHAVE command. DOES NOT have a terminating dot-line.
         */
        public String getBody() {
            return body;
        }

        /**
         * When the stream-representation of the Article is invalid.
         */
        static public class InvalidArticleFormatException extends Exception {
            public InvalidArticleFormatException(String message) {
                super(message);
            }
        }

        public static boolean isInvalidBody(String body) {
            return body == null || body.endsWith(Specification.CRLF_DOT_CRLF);
        }

        public MessageId getMessageId() {
                return messageId;
        }

        /**
         * Writes the entire Article as a stream following the XOVER/OVER prescription for ordering of header fields,
         * and terminates the stream with a single dot-line.
         */
        public String toString() {
            // write all header fields
            return getAllHeaders().toString() +
                    // write a separator line
                    Specification.CRLF +
                    // write the body
                    getBody() +
                    // write the end of the article
                    Specification.DOT_CRLF;
        }

        @Override
        public int hashCode() {
            return getMessageId().hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Article article)) return false;
            return getMessageId().equals(article.getMessageId());
        }

        public static class ArticleHeaders implements Serializable {
            private final Map<String, Set<String>> headerFields;

            /**
             * Returns all the headers as one String following RFC 5322 / RFC 3977 format.
             * Each header is followed by CRLF.
             */
            public String toString() {
                StringBuilder sb = new StringBuilder();

                // first, write out the standard headers is their enum order
                for (NNTP_Standard_Article_Headers header : NNTP_Standard_Article_Headers.values()) {
                    Set<String> values = headerFields.get(header.getValue());
                    if (values != null) {
                        if (!values.isEmpty()) {
                            // RFC 3977/5536: Newsgroups header is a single line with comma-separated values
                            if (NNTP_Standard_Article_Headers.Newsgroups.getValue().equalsIgnoreCase(header.getValue())) {
                                sb.append(header);
                                sb.append(Specification.HeaderNameFieldDelimiter);
                                sb.append(String.join(Specification.HeaderValueFieldSeparator, values));
                                sb.append(Specification.CRLF);
                            } else {
                                // For other headers, output each value on a new line
                                for (String value : values) {
                                    sb.append(header);
                                    sb.append(Specification.HeaderNameFieldDelimiter);
                                    sb.append(value);
                                    sb.append(Specification.CRLF);
                                }
                            }
                        } else {
                            sb.append(header);
                            sb.append(Specification.HeaderNameFieldDelimiter);
                            sb.append(Specification.CRLF);
                        }
                    }
                }
                // now write out the custom headers in alphabetical order
                for (Map.Entry<String, Set<String>> headerField : headerFields.entrySet()) {
                    if (headerField.getValue().isEmpty()) {
                        if (!NNTP_Standard_Article_Headers.contains(headerField.getKey())) {
                            Set<String> values = headerField.getValue();
                            for (String value : values) {
                                sb.append(headerField.getKey());
                                sb.append(Specification.HeaderNameFieldDelimiter);
                                sb.append(value);
                                sb.append(Specification.CRLF);
                            }
                        } else {
                            sb.append(headerField.getKey());
                            sb.append(Specification.HeaderNameFieldDelimiter);
                            sb.append(Specification.CRLF);
                        }
                    }
                }
                return sb.toString();
            }

            public static class InvalidArticleHeaderException extends Exception {
                public InvalidArticleHeaderException(String message) {
                    super(message);
                }
            }

            public ArticleHeaders(Map<String, Set<String>> headerFields) throws InvalidArticleHeaderException {
                Map<String, Set<String>> caseInsensitiveHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                caseInsensitiveHeaders.putAll(headerFields);
                this.headerFields = validateHeaderFields(caseInsensitiveHeaders);
            }

            /**
             * Return a deep-copy of the header fields
             */
            public Map<String, Set<String>> getHeaderFields() {
                Map<String, Set<String>> deepCopy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (Map.Entry<String, Set<String>> entry : headerFields.entrySet()) {
                    deepCopy.put(entry.getKey(), new HashSet<>(entry.getValue()));
                }
                return deepCopy;
            }

            public Iterable<? extends Map.Entry<String, Set<String>>> entrySet() {
                return Set.copyOf(headerFields.entrySet());
            }

            public Set<String> getHeaderValue(String headerName) {
                return (headerFields.containsKey(headerName) ? Set.copyOf(headerFields.get(headerName)) : null);
            }

            public MessageId getMessageId() {
                Set<String> messageIdHeaders = headerFields.get(Specification.NNTP_Standard_Article_Headers.MessageID.getValue());
                try {
                    return new MessageId(messageIdHeaders.iterator().next());
                } catch (MessageId.InvalidMessageIdException e) {
                    // not possible.  headerFields has already been validated for MessageId
                    throw new RuntimeException(e);
                }
            }

            /**
             * Gets the set of Newsgroups mentioned in this article's header: Newsgroups
             */
            public Set<NewsgroupName> getNewsgroups() {
                String newsgroupsHeader = Specification.NNTP_Standard_Article_Headers.Newsgroups.getValue();
                Set<String> groups = headerFields.get(newsgroupsHeader);
                return groups.stream()
                        .map(name -> {
                            try {
                                return new NewsgroupName(name);
                            } catch (NewsgroupName.InvalidNewsgroupNameException e) {
                                // This shouldn't happen as headerFields are validated on construction
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.toSet());
            }

            /**
             * Check if the header fields are valid.  A valid header field set MUST contain all the Standard Article Headers,
             * and those fields MUST be valid, according to the various methods defined below.  As for non-standard headers,
             * those must conform to basic syntax and formatting rules (see also below).
             * This method will return a normalised version of the header fields. i.e. one where a multivalued
             * Newgroup header value (such as "group1, group2") is converted to a set of single valued entries.
             *
             * @return normalized header fields where each value is properly normalised according to the field type
             */
            public static Map<String, Set<String>> validateHeaderFields(Map<String, Set<String>> headerFields)
                throws InvalidArticleHeaderException {

                if (headerFields != null) {
                    // check that all required headers are present
                    for (NNTP_Standard_Article_Headers standardizedHeader : EnumSet.allOf(NNTP_Standard_Article_Headers.class)) {
                        if (standardizedHeader.isMandatory() && !headerFields.containsKey(standardizedHeader.getValue())) {
                            throw new InvalidArticleHeaderException("Missing required header field: " + standardizedHeader.getValue());
                        }
                    }

                    // The result set is each header with its field(s) split up into a set of individual values.
                    Map<String, Set<String>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

                    for (Map.Entry<String, Set<String>> e : headerFields.entrySet()) {
                        String headerName = e.getKey();

                        if (isValidHeaderName(headerName)) {

                            for (String headerValue : e.getValue()) {

                                // check the specific cases of the standardized headers.
                                if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.MessageID.getValue())) {
                                    if (result.containsKey(headerName)) {
                                        throw new InvalidArticleHeaderException("Duplicate header field: " + headerName);
                                    } else if (!MessageId.isValid(headerValue)) {
                                        throw new InvalidArticleHeaderException("Invalid Message-ID header value: " + headerValue);
                                    } else {
                                        result.put(headerName, Set.of(headerValue));
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.Newsgroups.getValue())) {
                                    for (String v : headerValue.split(",")) {   // split newsgroup field parts on comma
                                        if (!NewsgroupName.isValid(v)) {
                                            throw new InvalidArticleHeaderException("Invalid newsgroup name: " + v);
                                        } else {
                                            // add to the result set
                                            result.computeIfAbsent(headerName, k -> new HashSet<>()).add(v);
                                        }
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.Subject.getValue())) {
                                    if (result.containsKey(headerName)) {
                                        throw new InvalidArticleHeaderException("Duplicate header field: " + headerName);
                                    } else if (isInvalidUnstructuredValue(headerValue)) {
                                        throw new InvalidArticleHeaderException("Invalid Subject header value: " + headerValue);
                                    } else {
                                        result.put(headerName, Set.of(headerValue));
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.Date.getValue())) {
                                    if (result.containsKey(headerName)) {
                                        throw new InvalidArticleHeaderException("Duplicate header field: " + headerName);
                                    } else if (!isValidDate(headerValue)) {
                                        throw new InvalidArticleHeaderException("Invalid Date header value: " + headerValue);
                                    } else {
                                        result.put(headerName, Set.of(headerValue));
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.From.getValue())) {
                                    if (result.containsKey(NNTP_Standard_Article_Headers.From.getValue())) {
                                        throw new InvalidArticleHeaderException("Duplicate header field: " + headerName);
                                    } else if (isInvalidUnstructuredValue(headerValue)) {
                                        throw new InvalidArticleHeaderException("Invalid From header value: " + headerValue);
                                    } else {
                                        result.put(headerName, Set.of(headerValue));
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.References.getValue())) {
                                    if (!isValidHeaderName(headerValue)) {
                                        throw new InvalidArticleHeaderException("Invalid References header value: " + headerValue);
                                    } else {
                                        result.computeIfAbsent(headerName, k -> new HashSet<>()).add(headerValue);
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.Path.getValue())) {
                                    if (result.containsKey(NNTP_Standard_Article_Headers.Path.getValue())) {
                                        throw new InvalidArticleHeaderException("Duplicate header field: " + headerName);
                                    } else if (isInvalidUnstructuredValue(headerValue)) {
                                        throw new InvalidArticleHeaderException("Invalid Path header value: " + headerValue);
                                    } else {
                                        result.put(headerName, Set.of(headerValue));
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.Bytes.getValue())) {
                                    if (result.containsKey(NNTP_Standard_Article_Headers.Bytes.getValue())) {
                                        throw new InvalidArticleHeaderException("Duplicate header field: " + headerName);
                                    } else {
                                        try {
                                            if (Integer.parseInt(headerValue) < 0) {   // Bytes header value must be a valid positive number
                                                throw new InvalidArticleHeaderException("Invalid Bytes header value: " + headerValue);
                                            } else {
                                                result.put(headerName, Set.of(headerValue));
                                            }
                                        } catch (NumberFormatException _) {
                                            throw new InvalidArticleHeaderException("Invalid Bytes header value: " + headerValue);
                                        }
                                    }
                                } else if (headerName.equalsIgnoreCase(NNTP_Standard_Article_Headers.Lines.getValue())) {
                                    if (result.containsKey(NNTP_Standard_Article_Headers.Lines.getValue())) {
                                        throw new InvalidArticleHeaderException("Duplicate header field: " + headerName);
                                    } else {
                                        try {
                                            if (Integer.parseInt(headerValue) < 0) {   // Lines header value must be a valid positive number
                                                throw new InvalidArticleHeaderException("Invalid Lines header value: " + headerValue);
                                            } else {
                                                result.put(headerName, Set.of(headerValue));
                                            }
                                        } catch (NumberFormatException _) {
                                            throw new InvalidArticleHeaderException("Invalid Lines header value: " + headerValue);
                                        }
                                    }
                                } else {    // not a standard header name
                                    // just check that the header value is ok
                                    if (!isValidHeaderValue(headerValue)) {
                                        throw new InvalidArticleHeaderException("Invalid header value: " + headerValue);
                                    } else {
                                        result.computeIfAbsent(headerName, k -> new HashSet<>()).add(headerValue);
                                    }
                                }
                            }
                        } else {
                            throw new InvalidArticleHeaderException("Invalid header name: " + headerName);
                        }
                    }
                    return result;
                }
                throw new InvalidArticleHeaderException("no Header fields present");
            }



            /**
             * Check if the header name is valid.
             * <p>
             * Header names must consist of one or more printable US-ASCII characters, excluding the colon character.
             */
            public static boolean isValidHeaderName(String name) {
                return name != null                             // not null
                        && !name.isEmpty()                      // not empty
                        && (!name.contains(" ") || name.length() == name.trim().length())    // no leading or trailing spaces (because they're of no use and insidiously deceptive)
                        && (!name.contains(":")                 // must not contain a colon (except for :bytes and :lines)
                        || name.contains(NNTP_Standard_Article_Headers.Bytes.getValue())
                        || name.contains(NNTP_Standard_Article_Headers.Lines.getValue()))
                        && StringUtils.isAsciiPrintable(name);  // all characters must be printable ASCII
            }

            public static boolean isValidHeaderValue(String value) {
                return value != null
                        && !value.isEmpty()
                        && !value.contains("\t")    // no tabs
                        && !value.contains("\r\n");   // no CRLFs
            }

            /**
             * Check if the string is unstructured.
             * According to RFC 5536 and RFC 5322, Unstructured is defined as a sequence of
             * - printable ASCII characters, spaces, and tabs, optionally including folded lines
             * (where long headers are split using CRLF followed by whitespace).
             * However, despite being called "unstructured" it still follows specific formatting rules:
             * It must contain at least one non-whitespace character.
             * It may include folding (line breaks) using CRLF followed by whitespace (FWS), but this is typically used for long headers.
             * It excludes control characters except for whitespace used in folding.
             */
            public static boolean isInvalidUnstructuredValue(String s) {
                return s == null
                        || s.isEmpty()
                        || !s.matches(".*\\p{Print}.*")   // contains at least one non-whitespace character
                        || !s.matches("\\p{Print}+(\\r\\n\\t\\s)*[\\p{Print}\\t\\r\\n]*");
            }

            /**
             * The Date header value must be in the format "Wdy, DD Mon YYYY HH:MM TIMEZONE" and while the use of "GMT" as a time zone is
             * deprecated, it is still widely accepted and must be supported by agents.
             */
            public static boolean isValidDate(String date) {
                try {
                    if (date != null && !date.isEmpty()) {
                        parseRFC3977Date(date);
                        return true;
                    }
                } catch (DateTimeException e) {
                    logger.debug("Invalid Date header value: " + date, e);
                }
                return false;
            }

            /**
             * Checks if the specified path contains the given identity as one of its components.
             * A path is a sequence of components separated by the "!" character, and the method
             * verifies whether any component of the path matches the given identity.
             *
             * @param path the Path header value to search within; can be null or empty but must be valid if non-null
             * @param identity the identity to search for within the components of the path; can be null or empty
             * @return true if the identity is found as a component of the path, false otherwise
             */
            public static boolean pathContainsIdentity(String path, String identity) {
                if (path != null && identity != null && !identity.isEmpty() && !path.isEmpty() && isValidPath(path)) {
                    String[] pathComponents = path.split("!", -1);
                    for (String pathComponent : pathComponents) {
                        if (pathComponent.equals(identity)) {
                            return true;
                        }
                    }
                }
                return false;
            }

            /**
             * Validates a Path header value according to RFC 5536 Section 3.1.4.
             * <p>
             * According to RFC 5536, the Path header field records the route that the article
             * took to reach the current system. The format is:
             * <p>
             * path = "Path:" SP *WSP path-list tail-entry *WSP CRLF
             * path-list = *( path-identity [FWS] [path-diagnostic] "!" )
             * path-identity = dot-atom-text
             * path-diagnostic = diag-match / diag-other
             * tail-entry = path-identity / path-diagnostic
             * <p>
             * Where:
             * - path-identity: A dot-atom-text representing the system identifier (e.g., "news.example.com")
             * - path-diagnostic: Optional diagnostic information (UTF-8 text without "!")
             * - tail-entry: The final component, either a path-identity or path-diagnostic
             * <p>
             * The path consists of a series of system identifiers separated by "!" exclamation marks.
             * Each system that processes the article prepends its identifier to the path.
             * <p>
             * Examples of valid paths according to RFC 5536:
             * - "not-for-mail" (common tail-entry for posted articles)
             * - "news.example.com!not-for-mail"
             * - "reader.example.net!news.example.com!not-for-mail"
             * - "site1!site2!site3" (UUCP-style)
             *
             * @param path the Path header value to validate (without the "Path:" prefix)
             * @return true if the path is valid according to RFC 5536, false otherwise
             */
            public static boolean isValidPath(String path) {
                if (path == null || path.isEmpty()) {
                    return false;
                }

                // Trim leading and trailing whitespace (allowed by *WSP in ABNF)
                path = path.trim();

                // Path must not be empty after trimming
                if (path.isEmpty()) {
                    return false;
                }

                // Split by "!" to get path components
                // According to RFC 5536, each component before "!" is a path-identity (optionally with path-diagnostic)
                // The last component is the tail-entry
                String[] components = path.split("!", -1); // -1 to preserve trailing empty strings

                // Must have at least one component (tail-entry)
                if (components.length == 0) {
                    return false;
                }

                // Validate each component
                for (int i = 0; i < components.length; i++) {
                    String component = components[i];
                    boolean isTailEntry = (i == components.length - 1 && 0 < i);

                    // Trim whitespace from component (FWS is allowed between components)
                    component = component.trim();

                    // Empty components are not allowed (indicates consecutive "!!" or leading/trailing "!")
                    if (component.isEmpty()) {
                        return false;
                    }

                    // For components in the path-list (all except tail-entry), they should be path-identity
                    // The tail-entry can be either path-identity or path-diagnostic
                    if (!isTailEntry) {
                        // Must be a valid path-identity (dot-atom-text)
                        if (!isValidPathIdentity(component)) {
                            return false;
                        }
                    } else {
                        // Tail-entry: can be path-identity or path-diagnostic
                        // Try path-identity first (more restrictive)
                        if (isValidPathIdentity(component)) {
                            // Valid path-identity
                            continue;
                        }

                        // If not a valid path-identity, check if it's a valid path-diagnostic
                        // path-diagnostic is more permissive - allows UTF-8 characters except "!"
                        // Since we split by "!", we already know there's no "!" inside
                        if (!isValidPathDiagnostic(component)) {
                            return false;
                        }
                    }
                }

                return true;
            }

            /**
             * Checks if a string is a valid path-identity according to RFC 5536.
             * <p>
             * A path-identity is defined as dot-atom-text in RFC 5536, which means:
             * - One or more atext characters separated by dots
             * - atext = ALPHA / DIGIT / "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" /
             *           "-" / "/" / "=" / "?" / "^" / "_" / "`" / "{" / "|" / "}" / "~"
             * <p>
             * For practical purposes and compatibility with existing implementations,
             * we use a more restrictive pattern that matches typical system identifiers:
             * letters, digits, dots, hyphens, and underscores.
             * <p>
             * According to RFC 5322 (referenced by RFC 5536):
             * - Cannot start or end with a dot
             * - Cannot have consecutive dots
             *
             * @param component the component to validate as a path-identity
             * @return true if the component is a valid path-identity
             */
            private static boolean isValidPathIdentity(String component) {
                if (component == null || component.isEmpty()) {
                    return false;
                }

                // dot-atom-text constraints from RFC 5322:
                // Cannot start or end with a dot
                if (component.startsWith(".") || component.endsWith(".")) {
                    return false;
                }

                // Cannot have consecutive dots
                if (component.contains("..")) {
                    return false;
                }

                // Must consist of valid atext characters and dots
                // For practical implementation, we limit to: letters, digits, dots, hyphens, underscores
                // This matches typical system identifiers and FQDN formats
                // Pattern: one or more of [a-zA-Z0-9._-]
                if (!component.matches("^[a-zA-Z0-9._-]+$")) {
                    return false;
                }

                // Additional validation: ensure it's not just dots/hyphens/underscores
                // Must contain at least one alphanumeric character
                if (!component.matches(".*[a-zA-Z0-9].*")) {
                    return false;
                }

                return true;
            }

            /**
             * Checks if a string is a valid path-diagnostic according to RFC 5536.
             * <p>
             * A path-diagnostic is optional diagnostic information that can appear in the path.
             * According to RFC 5536:
             * - path-diagnostic = diag-match / diag-other
             * - diag-match = "(" dot-atom-text ")"
             * - diag-other = <any UTF-8 character except "!">
             * <p>
             * Since we split by "!", we know there's no "!" inside the component.
             * We need to validate that it contains valid UTF-8 characters.
             * <p>
             * For the tail-entry, path-diagnostic allows more flexibility than path-identity,
             * including phrases like "not-for-mail" or diagnostic information.
             *
             * @param component the component to validate as a path-diagnostic
             * @return true if the component is a valid path-diagnostic
             */
            private static boolean isValidPathDiagnostic(String component) {
                if (component == null || component.isEmpty()) {
                    return false;
                }

                // path-diagnostic should not contain "!" (already ensured by split)
                // Should consist of printable characters (ASCII or UTF-8)
                // Allow letters, digits, spaces, and common punctuation (but not "!")

                // For practical implementation and security, we'll be somewhat restrictive
                // but allow common diagnostic strings like "not-for-mail"
                // Pattern: printable ASCII characters and basic punctuation, no control characters
                return component.matches("^[\\p{Print}\\p{L}\\p{N}._-]+$");
            }
        }
    }

    /**
     * A ProtoArticle is the raw article read in from a stream.
     * It is not validated (e.g. it may be missing required header fields, etc).
     * It is an internal representation of the streamed data alone, which may or may not be what we regard as a valid
     * Article object.
     * All header field names (map keys) are translated to lowercase.
     * The body text can be empty but is never null.
     */
    public static class ProtoArticle {
        private final Map<String, Set<String>> headersLowerCase;
        private final String bodyText;

        private ProtoArticle(Map<String, Set<String>> headersLowerCase, String bodyText) {
            this.headersLowerCase = headersLowerCase;
            this.bodyText = bodyText;
        }

        /**
         * Creates a ProtoArticle from the supplied streamed text version which includes the headers, body text, and terminating dot line.
         */
        static ProtoArticle fromString(String articleContent)
                throws Specification.Article.InvalidArticleFormatException {

            if (articleContent == null || articleContent.isEmpty()) {
                throw new Specification.Article.InvalidArticleFormatException("Invalid article format - no article content");
            }

            // Parse the article into headers and body
            int headerBodySeparator = articleContent.indexOf("\r\n\r\n");   // empty line that separates headers from the body

            if (headerBodySeparator == -1) {
                // Invalid article format - no separator between headers and body
                throw new Specification.Article.InvalidArticleFormatException("Invalid article format - no separator between headers and body");
            }

            String headersText = articleContent.substring(0, headerBodySeparator);
            String bodyText = articleContent.substring(headerBodySeparator + 4);    // 4 == '\r\n\r\n'.length()'

            // Validate the body text
            if (Specification.Article.isInvalidBody(bodyText)) {
                throw new Specification.Article.InvalidArticleFormatException("Invalid article format - invalid body");
            }

            // merge continuation lines back into one line
            headersText = headersText.replaceAll("\r\n +|\t", " ");

            // split headers into lines
            String[] headerLines = headersText.split("\r\n");

            // Parse headers, one line at a time
            Map<String, Set<String>> headerMap = new HashMap<>();

            for (String headerLine : headerLines) {
                // remove leading and trailing whitespace
                headerLine = headerLine.trim();

                // ignore empty lines
                if (headerLine.isEmpty()) continue;

                // split the header line into name and value.  Name must be non-empty, so start colon search from pos 1 not 0.
                int colonIndex = headerLine.substring(1).indexOf(':');
                if (colonIndex == -1) {
                    throw new Specification.Article.InvalidArticleFormatException("Invalid article format - invalid header line: " + headerLine);
                }
                // all headers are converted to Lowercase for ease of comparison
                String headerName = headerLine.substring(0, colonIndex+1).trim().toLowerCase();  // +1 because colon search started at substring(1)
                String headerValue = headerLine.substring(colonIndex+2).trim();

                // add the header value to the map.  Even if null, the header key will always be associated with a Set of values (possibly an empty Set).
                headerMap.computeIfAbsent(headerName, k -> new HashSet<>()).add(headerValue);
            }

            // return a record with an immutable header map
            return new ProtoArticle(headerMap, bodyText);
        }

        public Map<String, Set<String>> getHeadersLowerCase() {
            return Map.copyOf(headersLowerCase);
        }

        public String getBodyText() {
            return bodyText;
        }

        public void addFieldIfNotPresent(NNTP_Standard_Article_Headers nntpStandardArticleHeaders, String value) {
            if (!headersLowerCase.containsKey(nntpStandardArticleHeaders.getValue())) {
                headersLowerCase.put(nntpStandardArticleHeaders.getValue(), Set.of(value));
            } else if (headersLowerCase.get(nntpStandardArticleHeaders.getValue()).isEmpty()) {
                headersLowerCase.get(nntpStandardArticleHeaders.getValue()).add(value);
            }
        }
    }
}

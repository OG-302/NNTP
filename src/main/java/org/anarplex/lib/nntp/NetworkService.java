package org.anarplex.lib.nntp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * The NetworkService class abstracts underlying network-related operations. Concrete implementations of this class
 * define specific functionalities based on the underlying environment, platform, network transport mechanism, and
 * libraries being used.
 * <p/>
 * This class is designed to facilitate isolating network communication concerns from higher-level protocol
 * logic, allowing the ProtocolEngine and similar components to function independently of specific network
 * implementations.
 * <p/>
 * Key Responsibilities:
 * - Handling callbacks when new connections from clients are established, and
 * - Establishing connections to peer servers for synchronization purposes.
 */
public abstract class NetworkService {

    private static final Logger logger = LoggerFactory.getLogger(NetworkService.class);

    /**
     * ProtocolStreams represents a communication pathway between:
     * a) Clients and the NNTP Protocol Engine, and
     * b) A Peer Synchronizer and other NNTP Peers.
     * This class is a base class for both ConnectedClient and ConnectedPeer (see below).
     */
    public static abstract class ProtocolStreams {
        BufferedWriter writer;
        BufferedReader reader;


        ProtocolStreams(BufferedReader reader, BufferedWriter writer) {
            this.reader = reader;
            this.writer = writer;
        }

        public boolean isConnected() {
            return (reader != null && writer != null);
        }

        /**
         * Closes the resources associated with this instance, including the reader and writer streams.
         * If an error occurs while closing the resources, it is logged.
         * After this method is called, both reader and writer references are set to null.
         * Resources:
         * - Reader: The input stream used by this instance.
         * - Writer: The output stream used by this instance.
         * Notes:
         * - This method ensures that both streams are properly closed, regardless of any exceptions encountered during closure.
         * - The final(ly) block ensures that resources are reset to null even in the event of an error.
         */
        public void close() {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                logger.error("Error closing reader", e);
            } finally {
                reader = null;
                writer = null;
            }
        }

        /**
         * Returns the next line of text from the input stream or null if an error occurs.  Upon error, the
         * connection is closed.
         */
        public String readNextLine() {
            if (reader != null) {
                try {
                    return currentLine = reader.readLine();
                } catch (IOException e) {
                    close();
                }
            }
            return null;
        }

        /**
         * Returns the entire current line being read.
         */
        public String getCurrentLine() {
            return currentLine;
        }

        // the current line being read
        private String currentLine;

        /**
         * Writes a formatted string to the writer using the specified format string and arguments.
         * The behavior is similar to {@link String#format(String, Object...)}.
         * If an I/O error occurs during writing, the error is logged, and the resources are closed.
         *
         * @param format the format string, which specifies the structure of the output
         * @param args   the arguments referenced by the format specifiers in the format string
         */
        public void printf(String format, Object... args) {
            if (writer != null) {
                try {
                    writer.write(String.format(format, args));
                } catch (IOException e) {
                    logger.error("Error writing response to client", e);
                    close();
                }
            }
        }

        /**
         * Reads lines from the input stream until a single dot-line is encountered (CRLF+DOT+CRLF), then returns the
         * entire content as a list of text lines.
         * Does not unstuff dot-stuffed lines.
         * If an I/O error occurs during reading, the error is logged, the connection is closed, and an empty list
         * is returned.
         */
        public List<String> readList() {
            List<String> result = new ArrayList<>();
            if (reader != null) {
                String line;

                while ((line = readNextLine()) != null) {
                    if (line.length() == 1 && line.charAt(0) == Specification.DOT) {
                        // dot-line encountered so end the reading
                        break;
                    }
                    result.add(line.trim());
                }
            }
            return result;
        }

        /**
         * Reads lines from the input stream until a single dot-line is encountered (CRLF+DOT+CRLF), then returns the
         * entire content as a single string.
         * Does not unstuff dot-stuffed lines.
         * If an I/O error occurs during reading, the error is logged, the connection is closed, and an empty list
         * is returned.
         */
        public String readStream() {
            List<String> lines = readList();
            return String.join(Specification.CRLF, lines) + Specification.CRLF;
        }

        /**
         * Sends a dot-line (.\r\n) to the output stream.
         * If an I/O error occurs during writing, the error is logged, and the connection is closed.
         */
        public void sendDotLine() {
            if (writer != null) {
                try {
                    writer.write(Specification.DOT_CRLF);
                    writer.flush();
                } catch (IOException e) {
                    logger.error("Error writing response to client", e);
                    close();
                }
            }
        }

        /**
         * Flushes the output stream.
         * If an I/O error occurs during flushing, the error is logged, and the connection is closed.
         */
        public void flush() {
            if (writer != null) {
                try {
                    writer.flush();
                } catch (IOException e) {
                    logger.error("Error flushing writer", e);
                    close();
                }
            }
        }
    }

    /*
     * -----------------------------------------------------------------------------------------------------------------
     * The Protocol Engine uses the interfaces below to set up a listener for incoming connections.
     * -----------------------------------------------------------------------------------------------------------------
     */

    /**
     * This method registers a callback for servicing incoming connections and returns an interface to manage the
     * callback's lifecycle.
     * The callback listener's onConnection() method is to be called whenever a new connection from a client is
     * established with the NNTP Server.
     */
    public abstract ConnectionListener registerService(ServiceProvider serviceProvider);

    /**
     * The ServiceProvider interface defines a callback method which is invoked when a new connection is established with the NNTP service.
     */
    public interface ServiceProvider {
        /**
         * Invoked by a NetworkService implementation when a client establishes a new connection.
         * The implementation of this method (ultimately the ProtocolEngine) will process the stream of NNTP requests
         * arriving on the input stream and send responses to the output stream, according to the NNTP Protocol.
         * The method does not return until the entire NNTP dialog session between client and server has been completed.
         * Thus, the calling thread will be occupied for the duration of the session, and therefore the implementation
         * should assign a worker thread for this call.
         * This method returns when either the connection with the client has been terminated by the client, a network
         * error occurred, or a server-side error was encountered.
         */
        void onConnection(ConnectedClient client);
    }

    /**
     * The ConnectionListener interface provides a standardized and abstract way to manage the lifecycle of the connection
     * listener.  These methods must be implemented by NetworkService implementations.
     */
    public interface ConnectionListener {
        /**
         * Indicates to the NetworkService implementation that the service provider is ready to process incoming
         * requests from NNTP clients.
         */
        void start();

        /**
         * Indicates to the NetworkService implementation that new connection requests should be rejected but to allow
         * existing connections to continue uninterrupted.
         * @return the number of connections currently open
         */
        int shutdown();

        /**
         * Blocks until all connections has been closed.
         * This method is typically called after invoking the {@link #shutdown()} or {@link #stop()} methods
         * to ensure that all ongoing processes, such as handling of existing connections, have been completed
         * prior to termination.
         * Implementations should ensure that this method does not return until the shutdown process is fully
         * complete and all resources associated with the listener have been released.
         */
        void awaitShutdownCompletion();

        /**
         * Indicates that connection listening should be terminated and all existing connections closed immediately.
         */
        void stop();

    }

    /**
     * A convenience class which extends the notion of an NNTP Client connected via a bidirectional network stream to
     * the NNTP Protocol Engine.
     * This class provides convenience methods for sending and receiving NNTP responses.
     * If a network read or write error occurs, close() is invoked which prevents further communication with the (once)
     * connected client.  The status of the connection can be checked using the isConnected() method.
     */
    public static class ConnectedClient extends ProtocolStreams {
        private final IdentityService.Subject subject;    // defines who we are connected to

        /**
         * Constructs a new ConnectedClient instance with the specified input and output streams.
         */
        ConnectedClient(BufferedReader reader, BufferedWriter writer, IdentityService.Subject subject) {
            super(reader, writer);
            this.subject = subject;
        }

        IdentityService.Subject getSubject() {
            return subject;
        }

        /**
         * SendResponse writes a response to the response stream.
         * If sending a 205 response, then the connection is immediately closed.
         */
        public void sendResponse(Specification.NNTP_Response_Code code, String... args) {
            if (writer != null) {
                logger.info("Sending response: {} {}", code, Arrays.toString(args));
                printf("%s %s\r\n", code.getCode(), (args != null ? String.join(" ", args) : ""));
                flush();
                if (Specification.NNTP_Response_Code.Code_205.equals(code)) {
                    // after sending a 205 response, the server is to close the connection immediately
                    close();
                }
            }
        }
    }


    /*
     * -----------------------------------------------------------------------------------------------------------------
     * The NewsgroupSynchronizer uses the interfaces below to connect to NNTP Peers.
     * -----------------------------------------------------------------------------------------------------------------
     */

    /**
     * Connects to the specified Peer and returns a ProtocolStream for communication or null if connection failed.
     * This is used by the NewsgroupSynchronizer to establish a connection to a peer.
     */
    abstract ConnectedPeer connectToPeer(PersistenceService.Peer peer);

    /**
     * A convenience class which extends the notion of an NNTP Peer as a persisted entity represented by
     * {@code PersistenceService.Peer} to a Peer which is also connected via bidirectional network protocol streams to
     * that entity.
     * This class provides methods to communicate with the peer, exchange commands, and process responses.
     * It also implements mechanisms to handle and cache the NNTP capabilities being offered by the connected peer.
     * This class extends {@code ProtocolStreams} and implements {@code PersistenceService.Peer}.
     * It is designed to be subclassed by users of this library.
     */
    public static class ConnectedPeer extends ProtocolStreams {
        private final PersistenceService.Peer peer;
        private Set<String> capabilities = null;

        protected ConnectedPeer(BufferedReader reader, BufferedWriter writer, PersistenceService.Peer peer) {
            super(reader, writer);
            this.peer = peer;

            // check that the Peer is properly initialized
            // expecting a 200 welcome code to be waiting in the response buffer
            Specification.NNTP_Response_Code responseCode = getResponseCode();
            switch(responseCode) {
                case Code_200:  // Service available, posting allowed
                case Code_201:  // Service available, posting prohibited
                    // successfully connected to peer
                    logger.debug("Successfully connected to peer {}", peer.getLabel());
                    break;
                case Code_400: // Service temporarily unavailable
                    logger.warn("Peer {} returned 400 response code.  Connection temporarily unavailable.", peer.getLabel());
                    close();
                    break;
                case Code_502: // Service permanently unavailable
                    logger.error("Peer {} returned 502 response code.  Connection permanently unavailable.  Marking peer as disabled", peer.getLabel());
                    // mark the peer as disabled
                    setDisabledStatus(true);
                    close();
                    break;
                default:
                    logger.error("Error connecting to peer {}.  Got response code {}.  Closing connection.", getLabel(), responseCode);
                    close();
            }

            if (isConnected()) {
                // once connected, check for current capabilities offered by the Peer
                getCapabilities(true);
            }
        }

        PersistenceService.Peer getPeer() {
            return peer;
        }

        /**
         * Close the network connection with the peer, gracefully if possible.
         */
        public void close() {
            if (isConnected()) {
                // gracefully terminate connection with peer
                sendCommand(Specification.NNTP_Request_Commands.QUIT);
                // check on peer's response to QUIT command
                Specification.NNTP_Response_Code responseCode = getResponseCode();
                if (Specification.NNTP_Response_Code.Code_205.equals(responseCode)) {
                    // 205 response codes indicate that the server is closing the connection
                    logger.debug("Successfully ended session with peer {}", getLabel());
                } else {
                    logger.warn("Got response code {} while ending session with peer {}.", responseCode, getLabel());
                    if (Specification.NNTP_Response_Code.Code_502.equals(responseCode)) {
                        setDisabledStatus(true);
                    }

                }
            }
            super.close();
        }

        /**
         * Send the supplied NNTP command to this peer, possibly including optional arguments.
         */
        public void sendCommand(Specification.NNTP_Request_Commands nntpRequestCommand, String... args) {
            logger.debug("Sending command: {} {}", nntpRequestCommand, Arrays.toString(args));
            printf("%s %s\r\n", nntpRequestCommand.getValue(), (args != null ? String.join(" ", args) : ""));
            flush();
        }

        /**
         * Fetch the next line of text from the peer and interpret it as an NNTP response code.
         * If a 400 or 205 response code is encountered, the connection will be closed.
         * If a 502 response code is encountered, the peer will be marked as disabled and the connection closed.
         * If the response code is not recognized, then null is returned.
         */
        public Specification.NNTP_Response_Code getResponseCode() {
            String responseLine = readNextLine();
            if (responseLine != null) {
                String[] parts = responseLine.trim().split("\\s+");
                int part0 = Integer.parseInt(parts[0]);
                Specification.NNTP_Response_Code responseCode = Specification.NNTP_Response_Code.findByCode(part0);
                // analyze the response code for generic situations
                if (Specification.NNTP_Response_Code.Code_400.equals(responseCode)) {
                    super.close();    // close the connection (without sending QUIT) if a 400 response code is encountered as per RFC.
                } else if (Specification.NNTP_Response_Code.Code_205.equals(responseCode)) {
                    // 205 response codes indicate that the server is closing the connection.  we should do the same.
                    super.close();
                } else if (Specification.NNTP_Response_Code.Code_502.equals(responseCode)) {
                    // 502 response codes indicate that the server is permanently unavailable.  mark peer as disabled.
                    setDisabledStatus(true);
                    super.close();
                }
                return responseCode;
            }
            return null;
        }

        /**
         * Returns true if the peer supports the specified capability.
         * Comparison is case-insensitive and ignores hyphens and underscores.
         */
        public boolean hasCapability(Specification.NNTP_Server_Capabilities capability) {
            getCapabilities(false);
            if (!isConnected() || capabilities == null) return false;

            // Normalize token by removing hyphens/underscores and lowercasing for robust comparison
            String required = capability.toString().replace("-", "").replace("_", "").toLowerCase();
            for (String c : capabilities) {
                String normalized = (c == null ? "" : c).replace("-", "").replace("_", "").toLowerCase();
                if (normalized.equals(required)) return true;
            }
            return false;
        }

        /**
         * Ask the peer for its capabilities and cache the result.
         * Don't assume that all returned capabilities are defined by the NNTP specification. The Peer may return private extensions.
         */
        void getCapabilities(boolean forceRefresh) {
            if (reader != null && writer != null) {
                // peer is only contacted once during a connection session as capabilities shouldn't change
                if (capabilities == null || forceRefresh) {
                    capabilities = new HashSet<>();

                    sendCommand(Specification.NNTP_Request_Commands.CAPABILITIES);
                    if (Specification.NNTP_Response_Code.Code_101.equals(getResponseCode())) {
                        // peer responded with a 101 code indicating that it supports the CAPABILITIES command
                        List<String> response = readList();
                        capabilities = Set.copyOf(response);
                    }
                }
            }
        }

        public int getID() {
            return peer.getID();
        }

        public String getAddress() {
            return peer.getAddress();
        }

        public String getLabel() {
            return peer.getLabel();
        }

        public void setLabel(String label) {
            peer.setLabel(label);
        }

        public boolean isDisabled() {
            return peer.isDisabled();
        }

        public void setDisabledStatus(boolean disabled) {
            peer.setDisabledStatus(disabled);
        }

        public Instant getListLastFetched() {
            return peer.getListLastFetched();
        }

        public void setListLastFetched(Instant lastFetched) {
            peer.setListLastFetched(lastFetched);
        }

        public String getPrincipal() {
            return peer.getPrincipal();
        }
    }

    /**
     * A cache for managing connections to peers of the NNTP (Network News Transfer Protocol).
     * This class handles creating, caching, and removing peer connections, ensuring that a connection
     * to a peer is reused if it is still valid and properly cleaned up when no longer needed.
     * The class interacts with {@code NetworkService} to establish connections and determine the
     * availability of a peer. It also respects the NNTP protocol's response codes to handle
     * connectivity states appropriately.
     */
    public static class PeerConnectionCache {
        private final NetworkService networkService;
        private final Map<Integer, ConnectedPeer> peers = new HashMap<>();

        public PeerConnectionCache(NetworkService networkService) {
            this.networkService = networkService;
        }

        public NetworkService.ConnectedPeer getConnectedPeer(PersistenceService.Peer peer) {
            // look up peer in cache
            ConnectedPeer p = peers.get(peer.getID());

            if (p != null && !p.isConnected()) {
                // found a cached instance but no longer connected.  Remove it from the cache
                removeConnectedPeer(p);
                p = null;
            }
            if (p == null) {
                // establish a new connection to Peer and save in cache
                p = networkService.connectToPeer(peer);

                if (p != null && p.isConnected()) {
                    addConnectedPeer(p);
                }
            }
            return p;
        }

        public void closeAllConnections() {
            for (ConnectedPeer peer : peers.values()) {
                if (peer != null && peer.isConnected()) {
                    peer.close();
                    removeConnectedPeer(peer);
                }
            }
        }

        private void addConnectedPeer(NetworkService.ConnectedPeer peer) {
            peers.put(peer.getID(), peer);
        }

        private void removeConnectedPeer(NetworkService.ConnectedPeer peer) {
            peers.remove(peer.getID());
        }
    }
}

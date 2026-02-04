package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public abstract class NetworkUtilities {

    private static final Logger logger = LoggerFactory.getLogger(NetworkUtilities.class);

    /**
     * ProtocolStreams represents a communication pathway either between:
     * a) Clients and the NNTP Protocol Engine, or
     * b) A Peer Synchronizer and other NNTP Peers.
     * This class is a base class for both ConnectedClient and ConnectedPeer (see further below).
     */
    public static abstract class ProtocolStreams {
        protected BufferedWriter writer;
        protected BufferedReader reader;

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
         * Returns the next line of text from the input stream or null if an error occurs.
         */
        public String readLine() {
            if (reader != null) {
                try {
                    return reader.readLine();
                } catch (IOException e) {
                    close();
                }
            }
            return null;
        }

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
         * Read from the input stream until a single dot-line is encountered, then return the entire content with CRLFs
         * as a string except the final dot-line.
         * This function will unstuff dot-stuffed lines and include them in the result.
         * Returns null if an error occurs.
         */
        public String readUntilDotLine() {
            if (reader != null) {
                StringBuilder result = new StringBuilder();
                String line;
                try {
                    while ((line = reader.readLine()) != null) {
                        // Check if the line begins with a dot
                        if (line.startsWith(".")) {
                            if (line.length() > 1) {
                                if (line.charAt(1) == '.') {
                                    // dot-stuffed line.  remove the first (escaping) dot and keep the rest
                                    result.append(line.substring(1)).append(Specification.CRLF);
                                } else {
                                    logger.error("Protocol error.  Unparsable line encountered: {}.  Closing connection.", line);
                                    close();
                                }
                            } else {
                                // dot-line encountered so end the reading
                                return result.toString();
                            }
                        } else {
                            result.append(line).append(Specification.CRLF);
                        }
                    }
                } catch (IOException e) {
                    logger.error("Error reading from client", e);
                    close();
                }
            }
            return null;
        }

        /**
         * Sends a dot-line (.\r\n) to the output stream.
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
     * The interfaces etc. below are for use by the Protocol Engine (i.e., as a service) to listen for incoming connections.
     * -----------------------------------------------------------------------------------------------------------------
     */

    /**
     * This method is to be implemented by concrete implementations of the NetworkUtilities class.  Those implementations
     * will vary depending on the underlying network transport mechanism and network libraries used.  This method allows
     * such details to be isolated from the ProtocolEngine.
     * This method registers a callback listener (a ServiceProvider) which SHOULD BE called whenever a new connection
     * from a client is established with the NNTP Server.
     */
    public abstract ConnectionListener registerService(ServiceProvider serviceProvider);

    /**
     * The ServiceProvider interface defines a callback method which is invoked when a new connection is established.
     */
    public interface ServiceProvider {
        /**
         * Invoked by a NetworkUtilities implementation when a client establishes a new connection.
         * The implementation of this method is to process the input stream according to the NNTP Protocol and send
         * responses to the output stream.  This processing will be done using the calling thread.  Hence, the
         * NetworkUtilities should assign a worker thread for this call.
         * When the method returns, it is because either the connection with the client has been
         * terminated by the client, a network error occurred, or a server-side error was encountered.
         */
        void onConnection(ConnectedClient client);
    }

    /**
     * The ConnectionManager interface provides a standardized and abstract way to manage the network listener.
     */
    public interface ConnectionListener {
        /**
         * Signals that the listener should begin listening for incoming connections and dispatch them to the registered
         * ServiceProvider in their own worker thread.
         */
        void start();

        /**
         * Prevents new connections from being accepted but allows existing connections to continue to be processed.
         * Returns the number of connections currently open.
         */
        int shutdown();

        /**
         * Blocks the current thread and waits until the listener has been properly instructed to shut down.
         * This method is typically called after invoking the {@link #shutdown()} or {@link #stop()} methods
         * to ensure that all ongoing processes, such as handling of existing connections, have been completed
         * prior to termination.
         * Implementations should ensure that this method does not return until the shutdown process is fully
         * complete and all resources associated with the listener have been released.
         */
        void awaitShutdownCompletion();

        /**
         * Prevents new connections from being accepted and immediately closes all existing connections.
         */
        void stop();

    }

    /**
     * A convenience class which extends the notion of an NNTP Client connected via a bidirectional network stream to
     * the NNTP Protocol Engine.
     * This class provides convenience methods for sending and receiving NNTP responses.
     * If a network read or write error occurs, the object invokes close() which prevents further communication with the
     * client.  The status of the object can be checked using the isConnected() method.
     */
    public static class ConnectedClient extends ProtocolStreams {

        /**
         * Constructs a new ConnectedClient instance with the specified input and output streams.
         */
        public ConnectedClient(BufferedReader reader, BufferedWriter writer) {
            super(reader, writer);
        }

        /**
         * SendResponse writes a response to the response stream.
         * If sending a 205, then the connection is immediately closed.
         */
        public void sendResponse(Specification.NNTP_Response_Code code, String... args) {
            if (writer != null) {
                if (code.getCode() < 400) {
                    logger.debug("Sending response: {} {}", code, Arrays.toString(args));
                } else  {
                    logger.warn("Sending response: {} {}", code, Arrays.toString(args));
                }
                printf("%s %s\r\n", code.getCode(), (args != null ? String.join(" ", args) : ""));
                flush();
                if (Specification.NNTP_Response_Code.Code_205.equals(code)) {
                    // when sending a 205 response, close the connection immediately
                    close();
                }
            }
        }
    }


    /*
     * -----------------------------------------------------------------------------------------------------------------
     * The interfaces etc. below are for use by the PeerSynchronizer to connect to NNTP Peers.
     * -----------------------------------------------------------------------------------------------------------------
     */

    /**
     * Connects to the specified Peer using supplied properties and returns a ProtocolStream for communication or null if connection failed.
     * This is used by the PeerSynchronizer to establish a connection to a peer.
     */
    abstract ConnectedPeer connectToPeer(PersistenceService.Peer peer);

    /**
     * A convenience class which extends the notion of an NNTP Peer connected via a bidirectional network stream typically
     * from a Peer Synchronizer, and which is also represented by a {@code PersistenceService.Peer}.
     * This class provides methods to communicate with the peer, exchange commands, and process responses.
     * It also implements mechanisms to handle and cache server capabilities.
     * This class extends {@code ProtocolStreams} and implements {@code PersistenceService.Peer}.
     * It is designed to be subclassed for concrete implementations.
     */
    public abstract static class ConnectedPeer extends ProtocolStreams implements PersistenceService.Peer {

        protected ConnectedPeer(BufferedReader reader, BufferedWriter writer, PersistenceService.Peer peer) {
            super(reader, writer);
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
                    peer.setDisabledStatus(true);
                    close();
                    break;
                default:
                    logger.error("Error connecting to peer {}.  Closing connection.", getLabel());
                    close();
            }

            if (isConnected()) {
                // once connected, check for current capabilities offered by the Peer
                getCapabilities(true);
            }
        }

        public void close() {
            if (isConnected()) {
                // gracefully terminate connection with peer
                sendCommand(Specification.NNTP_Request_Commands.QUIT);
            }
            super.close();
        }

        /**
         * Send the supplied NNTP command to this peer, possibly including optional arguments.
         */
        public void sendCommand(Specification.NNTP_Request_Commands nntpRequestCommand, String... args) {
            printf("%s %s\r\n", nntpRequestCommand.getValue(), (args != null ? String.join(" ", args) : ""));
            flush();
        }

        /**
         * Fetch the next line of text from the peer and interpret it as an NNTP response code.
         * If a 400 response is encountered, the connection will be closed.
         * If the response code is not recognized, then null is returned.
         */
        public Specification.NNTP_Response_Code getResponseCode() {
            String responseLine = readLine();
            if (responseLine != null) {
                String[] parts = responseLine.trim().split("\\s+");
                int part0 = Integer.parseInt(parts[0]);
                Specification.NNTP_Response_Code responseCode = Specification.NNTP_Response_Code.findByCode(part0);
                if (Specification.NNTP_Response_Code.Code_400.equals(responseCode)) {
                    super.close();    // close the connection (without sending QUIT) if a 400 response code is encountered as per RFC.
                } else if (Specification.NNTP_Response_Code.Code_205.equals(responseCode)) {
                    // 205 response codes indicate that the server is closing the connection.  we should do the same.
                    super.close();
                }
                return responseCode;
            }
            return null;
        }

        /**
         * Returns true if the peer supports the specified capability.
         */
        public boolean hasCapability(Specification.NNTP_Server_Capabilities capability) {
            getCapabilities(false);
            return isConnected() && capabilities.contains(capability.toString().toLowerCase());
        }

        /**
         * Ask the peer for its capabilities and cache the result.
         */
        void getCapabilities(boolean forceRefresh) {
            if (reader != null && writer != null) {
                // peer is only contacted once during a connection session as capabilities shouldn't change
                if (capabilities == null || forceRefresh) {
                    capabilities = new HashSet<>();

                    sendCommand(Specification.NNTP_Request_Commands.CAPABILITIES);

                    if (Specification.NNTP_Response_Code.Code_101.equals(getResponseCode())) {
                        // peer responded with a 101 code indicating that it supports the CAPABILITIES command
                        String line = readUntilDotLine();

                        for (String c : line.split(Specification.CRLF)) {
                            capabilities.add(c.trim().toLowerCase());
                        }
                    }
                }
            }
        }

        private Set<String> capabilities = null;
    }

    /**
     * A cache for managing connections to peers of the NNTP (Network News Transfer Protocol).
     * This class handles creating, caching, and removing peer connections, ensuring that a connection
     * to a peer is reused if it is still valid and properly cleaned up when no longer needed.
     * The class interacts with {@code NetworkUtilities} to establish connections and determine the
     * availability of a peer. It also respects the NNTP protocol's response codes to handle
     * connectivity states appropriately.
     */
    public static class PeerConnectionCache {
        private final NetworkUtilities networkUtilities;
        private final Map<Integer, ConnectedPeer> peers = new HashMap<>();

        public PeerConnectionCache(NetworkUtilities networkUtilities) {
            this.networkUtilities = networkUtilities;
        }

        public NetworkUtilities.ConnectedPeer getConnectedPeer(PersistenceService.Peer peer) {
            // look up peer in cache
            ConnectedPeer p = peers.get(peer.getID());

            if (p != null && !p.isConnected()) {
                // found a cached instance but no longer connected.  Remove it from the cache
                removeConnectedPeer(p);
                p = null;
            }
            if (p == null) {
                // establish a new connection to Peer and save in cache
                p = networkUtilities.connectToPeer(peer);

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

        private void addConnectedPeer(NetworkUtilities.ConnectedPeer peer) {
            peers.put(peer.getID(), peer);
        }

        private void removeConnectedPeer(NetworkUtilities.ConnectedPeer peer) {
            peers.remove(peer.getID());
        }
    }
}

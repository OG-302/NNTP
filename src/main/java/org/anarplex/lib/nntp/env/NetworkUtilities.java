package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.ProtocolEngine;
import org.anarplex.lib.nntp.Specification;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public interface NetworkUtilities {

    /**
     * The communication pathway between clients and the NNTP Protocol Engine, and also between the Peer Synchronizer
     * and other Peers.
     */
    interface ProtocolStreams {
        BufferedReader getReader();
        PrintWriter getWriter();
        void closeConnection();
    }

    public abstract class ConnectedPeer implements PersistenceService.Peer, ProtocolStreams {
        protected Set<String> capabilities = null;

        /**
         * Contact the peer and ask for its capabilities.
         */
        private void getCapabilities() {
            // peer is only contacted once during a connection session as capabilities shouldn't change
            if (capabilities == null) {
                capabilities = new HashSet<>();
                PrintWriter writer = getWriter();
                writer.println(Specification.NNTP_Request_Commands.CAPABILITIES);
                writer.flush(

                );
                BufferedReader reader = getReader();
                try {
                    if (reader.readLine().stripLeading().startsWith(Specification.NNTP_Response_Code.Code_101.toString())) {
                        String line;
                        // keep reading lines until a single dot-line is encountered
                        while ((line = reader.readLine()) != null && ".".equals(line)) {
                            capabilities.add(line.trim().toLowerCase());
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public boolean hasCapability(Specification.NNTP_Server_Capabilities capability) {
            getCapabilities();
            return capabilities.contains(capability.toString().toLowerCase());
        }
    }

    /**
     * Connects to the specified Peer using supplied properties and returns a ProtocolStream for communication.
     */
    ConnectedPeer connectToPeer(PersistenceService.Peer peer) throws IOException;

    /**
     * Opens a port (specified by supplied properties) to listen for incoming connections and dispatches such
     * new connections to the supplied ConnectionListener.  Returns a ServiceManager that can be used to start and
     * terminate the Service.
     */
    ServiceManager registerService(ConnectionListener cl) throws IOException;

    interface ConnectionListener {
        /**
         * Invoked when a new connection is established with a client.  The supplied ProtocolStream is ready for communication.
         * Implementation should execute the NNTP Protocol Engine on the supplied stream.
         * Will not be invoked until start() is called on the ServiceManager.
         */
        void onConnection(ProtocolStreams newConnection);
    }

    interface ServiceManager {
        void start();
        void terminate();
    }

    class PeerConnectionCache {
        private final NetworkUtilities networkUtilities;
        private final Set<ConnectedPeer> peers = new HashSet<>();

        public PeerConnectionCache(NetworkUtilities networkUtilities) {
            this.networkUtilities = networkUtilities;
        }

        public NetworkUtilities.ConnectedPeer getConnectedPeer(PersistenceService.Peer peer) {
            NetworkUtilities.ConnectedPeer p = peers.stream()
                    .filter(cp -> cp.getAddress().equals(peer.getAddress()))
                    .findFirst()
                    .orElse(null);
            if (p != null && p.getWriter().checkError()) {
                // peer connection is broken.  Close it and remove it from the cache
                removeConnectedPeer(p);
                p = null;
            }
            if (p == null) {
                // establish a new connection to Peer and save in cache
                try {
                    p = networkUtilities.connectToPeer(peer);
                    addConnectedPeer(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return p;
        }

        void closeAllConnections() {
            peers.forEach(NetworkUtilities.ConnectedPeer::closeConnection);
        }

        private void addConnectedPeer(NetworkUtilities.ConnectedPeer peer) {
            peers.add(peer);
        }

        private void removeConnectedPeer(NetworkUtilities.ConnectedPeer peer) {
            peer.closeConnection();
            peers.remove(peer);
        }
    }
}

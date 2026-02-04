package org.anarplex.lib.nntp.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MockNetworkUtilities extends NetworkUtilities {
    private static final Logger logger = LoggerFactory.getLogger(MockNetworkUtilities.class);

    private static final String NNTP_PORT_LABEL = "nntp.port";
    private static final int DEFAULT_NNTP_PORT = 3119;

    public static Properties networkEnv = new Properties();

    /**
     * Constructor.
     * Set the server listening port from properties: nntp.port
     */
    private MockNetworkUtilities(Properties p) {
        networkEnv.putAll(p);
    }

    public static MockNetworkUtilities getInstance(Properties p) {
        return new MockNetworkUtilities(p);
    }

    @Override
    public NetworkUtilities.ConnectionListener registerService(ServiceProvider sp) {
        return new ConnectionListener(sp);
    }

    public static class ConnectedPeerImpl extends ConnectedPeer {
        final PersistenceService.Peer peer;

        protected ConnectedPeerImpl(PersistenceService.Peer peer, BufferedReader reader, BufferedWriter writer) {
            super(reader, writer, peer);
            this.peer = peer;
        }

        @Override
        public int getID() {
            return peer.getID();
        }

        @Override
        public String getAddress() {
            return peer.getAddress();
        }

        @Override
        public String getLabel() {
            return peer.getLabel();
        }

        @Override
        public void setLabel(String label) {
            peer.setLabel(label);
        }

        @Override
        public boolean getDisabledStatus() {
            return peer.getDisabledStatus();
        }

        @Override
        public void setDisabledStatus(boolean disabled) {
            peer.setDisabledStatus(disabled);
        }

        @Override
        public LocalDateTime getListLastFetched() {
            return peer.getListLastFetched();
        }

        @Override
        public void setListLastFetched(LocalDateTime lastFetched) {
            peer.setListLastFetched(lastFetched);
        }

        @Override
        public String getPrincipal() {
            return peer.getPrincipal();
        }
    }

    /**
     * An implementation of the connectToPeer method that uses the host and port to properties to open a socket connection.
     * Assumes the Address field of the Peer is in the format nntp://host:port.
     */
    @Override
    public ConnectedPeer connectToPeer(PersistenceService.Peer peer)  {
        try {
            String NNTP_PROTOCOL = "nntp://";
            String address = peer.getAddress();
            if (address != null && address.startsWith(NNTP_PROTOCOL)) {
                String[] parts = address.substring(NNTP_PROTOCOL.length()).split(":");
                Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));

                if (socket.isConnected()) {
                    return new ConnectedPeerImpl(
                            peer,
                            new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)),
                            new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)));
                }
            }
        } catch (IOException e) {
            logger.error("Error connecting to peer: {}", e.getMessage());
        }
        return null;
    }


    private static class ConnectionListener implements NetworkUtilities.ConnectionListener {

        private final ServiceProvider serviceProvider;
        private final int port;
        private volatile boolean accepting = false;
        private ServerSocket serverSocket;
        private final ExecutorService clientPool = Executors.newCachedThreadPool();
        private final java.util.Set<Socket> openSockets = java.util.Collections.synchronizedSet(new java.util.HashSet<>());
        private final java.util.concurrent.atomic.AtomicInteger openConnections = new java.util.concurrent.atomic.AtomicInteger(0);

        private ConnectionListener(ServiceProvider serviceProvider) {
            this.serviceProvider = serviceProvider;
            String portStr = MockNetworkUtilities.networkEnv.getProperty(NNTP_PORT_LABEL, String.valueOf(DEFAULT_NNTP_PORT));
            int p;
            try {
                p = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
               logger.error("Invalid port number: {}", portStr);
               p = 0;
            }
            this.port = p;
        }

        @Override
        public void start() {
            if (accepting) return;
            accepting = true;
            try {
                serverSocket = new ServerSocket(port);
            } catch (IOException e) {
                logger.error("Failed to start server socket on port {}", port, e);
                accepting = false;
                return;
            }

            Thread acceptThread = new Thread(() -> {
                logger.info("Listening on port {}", port);
                while (accepting) {
                    try {
                        Socket client = serverSocket.accept();
                        openSockets.add(client);
                        openConnections.incrementAndGet();
                        clientPool.submit(() -> {
                            try {
                                new ClientHandler(client, serviceProvider).run();
                            } finally {
                                openSockets.remove(client);
                                openConnections.decrementAndGet();
                            }
                        });
                    } catch (IOException e) {
                        if (accepting) {
                            logger.error("Error accepting connection", e);
                        } else {
                            // socket closed due to shutdown/stop
                            break;
                        }
                    }
                }
                logger.info("Listener stopped on port {}", port);
            }, "MockNNTP-Acceptor");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        @Override
        public void awaitShutdownCompletion() {
            // Wait until shutdown/stop has been requested and all active connections have drained
            while (true) {
                if (!accepting && openConnections.get() == 0) {
                    break;
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            // Ensure client worker pool is shut down gracefully
            clientPool.shutdown();
            try {
                clientPool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public int shutdown() {
            // stop accepting new connections, keep existing running
            accepting = false;
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException ignored) {
                }
            }
            return openConnections.get();
        }

        @Override
        public void stop() {
            // stop accepting and close existing connections immediately
            shutdown();
            synchronized (openSockets) {
                for (Socket s : openSockets) {
                    try {
                        s.close();
                    } catch (IOException ignored) {
                    }
                }
                openSockets.clear();
            }
            clientPool.shutdownNow();
        }
    }


    /**
     * Handles client connections for a network service. This class implements runnable to allow
     * execution in a separate thread for managing individual client connections.
     * <p>
     * The ClientHandler is responsible for initializing input and output streams to communicate
     * with the connected client, and invoking the provided ConnectionListener to handle communication
     * protocol logic.
     * <p>
     * The life cycle of the connection is managed within this class, which includes resource
     * cleanup upon disconnection.
     * <p>
     * Thread-safety: While the ClientHandler is designed to work in multithreaded environments,
     * it's not inherently thread-safe. External synchronization may be required if multiple threads
     * interact with the same ClientHandler instance.
     */
    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final ServiceProvider listener;
        private final BufferedReader reader;
        private final BufferedWriter writer;


        public ClientHandler(Socket clientSocket, ServiceProvider listener) {
            this.clientSocket = clientSocket;
            this.listener = listener;
            // wrap peer's outputStream with a PrintWriter
            try {
                this.writer = new BufferedWriter(
                        new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8) // RFC-3977 requires UTF-8 encoding
                );
                // wrap the input stream with BufferedReader to read responses line by line
                this.reader = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
                );
            } catch (IOException e) {
                logger.error("Error creating client handler: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            try {
                // call the registered service provider to handle the client connection
                listener.onConnection(new ConnectedClient(this.reader, this.writer) {

                    @Override
                    public void close() {
                        if (!clientSocket.isClosed()) {
                            logger.info("Client disconnected: {}", clientSocket.getInetAddress());
                            try {
                                writer.flush();
                                clientSocket.close();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                });

            } finally {
                try {
                    if (clientSocket != null && !clientSocket.isClosed()) {
                        logger.info("Client disconnected: {}", clientSocket.getInetAddress());
                        clientSocket.close();
                    }
                } catch (IOException e) {
                    logger.error("Error closing client connection: {}", e.getMessage());
                }
            }
        }
    }
}

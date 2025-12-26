package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;
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

public class MockNetworkUtilities implements NetworkUtilities {
    private static final Logger logger = LoggerFactory.getLogger(MockNetworkUtilities.class);

    public static Properties networkEnv = new Properties();
    private static final int THREAD_POOL_SIZE = 10;
    private static final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    static {
        networkEnv.put("port", "119");
    }

    private MockNetworkUtilities(Properties p) {
        networkEnv.putAll(p);
    }

    public static MockNetworkUtilities getInstance(Properties p) {
        return new MockNetworkUtilities(p);
    }

    @Override
    public NetworkUtilities.ServiceManager registerService(ConnectionListener cl) {
        return new ServiceManager(cl);
    }

    public static class ConnectedPeerImpl extends ConnectedPeer {
        private final Socket connection;
        private final PersistenceService.Peer peer;
        PrintWriter writer;
        BufferedReader reader;


        protected ConnectedPeerImpl(Socket socket, PersistenceService.Peer peer) throws IOException {
            this.connection = socket;
            this.peer = peer;
            // wrap peer's outputStream with a PrintWriter
            writer = new PrintWriter(
                    new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8), // RFC-3977 requires UTF-8 encoding
                    false    // autoflush
            );
            // wrap the input stream with BufferedReader to read responses line by line
            reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)
            );
        }

        @Override
        public BufferedReader getReader() {
            return reader;
        }

        @Override
        public PrintWriter getWriter() {
            return writer;
        }

        @Override
        public void closeConnection() {
            if (connection != null && !connection.isConnected()) {
                try {
                    logger.info("Client disconnected: {}", connection.getInetAddress());
                    writer.flush();
                    connection.close();
                } catch (IOException e) {
                    logger.error("Error closing connection: {}", e.getMessage());
                    throw new RuntimeException(e);
                }
            }
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
        public void setLabel(String label) { peer.setLabel(label); }

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
            return getAddress();
        }
    }

    @Override
    public ConnectedPeer connectToPeer(PersistenceService.Peer peer) throws IOException {
        ConnectedPeerImpl c = new ConnectedPeerImpl(new Socket(networkEnv.getProperty("host"), Integer.parseInt(networkEnv.getProperty("port"))), peer);
        System.out.println("Connected to " + networkEnv.getProperty("host") + ":" + networkEnv.getProperty("port"));
        return c;
    }


    private static class ServiceManager implements NetworkUtilities.ServiceManager, Runnable {
        private ConnectionListener listener;

        ServiceManager(ConnectionListener listener) {
            this.listener = listener;
        }

        @Override
        public void start() {
            Thread thread = new Thread(this);
            thread.start(); // Starts the new thread
        }

        @Override
        public void run() {
            // Create a thread pool to handle client connections
            try (ServerSocket serverSocket = new ServerSocket(Integer.parseInt(networkEnv.getProperty("port")))) {
                logger.info("Server is listening on port {}", serverSocket.getLocalPort());
                while (true) {
                    // Accept a new client connection
                    Socket clientSocket = serverSocket.accept();
                    logger.info("Client connected: {}", clientSocket.getInetAddress());

                    // Handle the client in a separate thread
                    executorService.execute(new ClientHandler(clientSocket, listener));
                }
            } catch (IOException e) {
                logger.error("Server error: {}", e.getMessage());
            } finally {
                executorService.shutdown();
            }
        }

        @Override
        public void terminate() {
            listener = null;
            executorService.shutdown();
        }
    }


    // Class to handle individual client connections
    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final ConnectionListener listener;
        private final BufferedReader reader;
        private final PrintWriter writer;


        public ClientHandler(Socket clientSocket, ConnectionListener listener) {
            this.clientSocket = clientSocket;
            this.listener = listener;
            // wrap peer's outputStream with a PrintWriter
            try {
                this.writer = new PrintWriter(
                        new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8), // RFC-3977 requires UTF-8 encoding
                        true    // autoflush
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
                listener.onConnection(new ProtocolStreams() {

                    @Override
                    public BufferedReader getReader() {
                        return reader;
                    }

                    @Override
                    public PrintWriter getWriter() {
                        return writer;
                    }

                    @Override
                    public void closeConnection() {
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

    public static class MockProtocolStreams implements NetworkUtilities.ProtocolStreams {
        BufferedReader reader;
        PrintWriter writer;

        public MockProtocolStreams(InputStream inputStream, ByteArrayOutputStream outputStream) {
            reader = new BufferedReader(new InputStreamReader(inputStream));
            writer = new PrintWriter(outputStream);
        }


        @Override
        public BufferedReader getReader() {
            return reader;
        }

        @Override
        public PrintWriter getWriter() {
            return writer;
        }

        @Override
        public void closeConnection() {
            try {
                reader.close();
                writer.flush();
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

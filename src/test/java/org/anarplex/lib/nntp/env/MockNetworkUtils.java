package org.anarplex.lib.nntp.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MockNetworkUtils implements NetworkUtils {
    private static final Logger logger = LoggerFactory.getLogger(MockNetworkUtils.class);

    public static Properties defaults = new Properties();
    private static final int THREAD_POOL_SIZE = 10;
    private static final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    static {
        defaults.put("port", "3119");   // for testing purposes.  not a privileged port.
    }

    @Override
    public NetworkUtils.ServiceManager registerService(ConnectionListener cl, Properties p) {
        return new ServiceManager(cl, p);
    }

    @Override
    public ProtocolStreams connectToPeer(PersistenceService.Peer peer, Properties p) throws IOException {
        return new ProtocolStreams() {
            Socket connection;
            void ProtocolStreams() {
                try {
                    connection = new Socket(p.getProperty("host"), Integer.parseInt(p.getProperty("port")));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public InputStream getInputStream() {
                try {
                    return connection.getInputStream();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public OutputStream getOutputStream() {
                try {
                    return connection.getOutputStream();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public void close() {
                try {
                    connection.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }


    private static class ServiceManager implements NetworkUtils.ServiceManager {
        private ConnectionListener listener;
        private final Properties p;

        ServiceManager(ConnectionListener listener, Properties p) {
            this.listener = listener;
            this.p = p;
        }

        @Override
        public void start() {
            // Create a thread pool to handle client connections
            try (ServerSocket serverSocket = new ServerSocket(Integer.parseInt(p.getProperty("port")))) {
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

        public ClientHandler(Socket clientSocket, ConnectionListener listener) {
            this.clientSocket = clientSocket;
            this.listener = listener;
        }

        @Override
        public void run() {
            try {

                listener.onConnection(new ProtocolStreams() {
                    @Override
                    public InputStream getInputStream() {
                        try {
                            return clientSocket.getInputStream();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public OutputStream getOutputStream() {
                        try {
                            return clientSocket.getOutputStream();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void close() {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    logger.error("Error closing client connection: {}", e.getMessage());
                }
            }
        }
    }

    public static class MockProtocolStreams implements NetworkUtils.ProtocolStreams {
        InputStream inputStream;
        OutputStream outputStream;

        public MockProtocolStreams() {
            inputStream = new ByteArrayInputStream(new byte[0]);
            outputStream = new ByteArrayOutputStream();
        }

        public MockProtocolStreams(String inputString, ByteArrayOutputStream outputStream) {
            inputStream = new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
            this.outputStream = outputStream;
        }

        public MockProtocolStreams(InputStream inputStream, ByteArrayOutputStream outputStream) {
            this.inputStream = inputStream;
            this.outputStream = outputStream;
        }

        public MockProtocolStreams(ByteArrayOutputStream outputStream) {
            inputStream = new ByteArrayInputStream(new byte[0]);
            this.outputStream = outputStream;
        }

        @Override
        public InputStream getInputStream() {
            return inputStream;
        }

        @Override
        public OutputStream getOutputStream() {
            return outputStream;
        }

        @Override
        public void close() {
            try {
                getInputStream().close();
                getOutputStream().close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

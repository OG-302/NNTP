package org.anarplex.lib.nntp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP/IP test implementation of {@link NetworkService}.
 *
 * <p>This class is intended for integration-style tests where a real socket-based
 * transport is useful, while still keeping the implementation lightweight and local.
 */
public class MockNetworkService extends NetworkService {
    private static final Logger logger = LoggerFactory.getLogger(MockNetworkService.class);

    private final int listenPort;

    public MockNetworkService(int listenPort) {
        this.listenPort = listenPort;
    }

    @Override
    public ConnectionListener registerService(ServiceProvider serviceProvider) {
        return new SocketConnectionListener(serviceProvider, listenPort);
    }

    @Override
    ConnectedPeer connectToPeer(PersistenceService.Peer peer) {
        if (peer == null || peer.getAddress() == null || peer.getAddress().isBlank()) {
            return null;
        }

        String[] hostPort = peer.getAddress().trim().split(":", 2);
        if (hostPort.length != 2) {
            logger.warn("Peer address must be in host:port format: {}", peer.getAddress());
            return null;
        }

        try {
            String host = hostPort[0].trim();
            int port = Integer.parseInt(hostPort[1].trim());

            Socket socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(
                    new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer = new BufferedWriter(
                    new java.io.OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

            return new SocketConnectedPeer(socket, reader, writer, peer);
        } catch (IOException | NumberFormatException e) {
            logger.warn("Failed to connect to peer {}", peer.getAddress(), e);
            return null;
        }
    }

    private static final class SocketConnectionListener implements ConnectionListener {
        private final ServiceProvider serviceProvider;
        private final int listenPort;
        private final AtomicBoolean accepting = new AtomicBoolean(false);
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private final Set<SocketConnectedClient> openClients = ConcurrentHashMap.newKeySet();
        private final CountDownLatch listenerStarted = new CountDownLatch(1);
        private final CountDownLatch listenerDone = new CountDownLatch(1);

        private volatile ServerSocket serverSocket;
        private volatile Thread acceptThread;

        private SocketConnectionListener(ServiceProvider serviceProvider, int listenPort) {
            this.serviceProvider = serviceProvider;
            this.listenPort = listenPort;
        }

        @Override
        public void start() {
            if (accepting.getAndSet(true)) {
                return;
            }

            acceptThread = Thread.ofPlatform().name("mock-network-listener-" + listenPort).start(() -> {
                try (ServerSocket ss = new ServerSocket(listenPort)) {
                    serverSocket = ss;
                    listenerStarted.countDown();

                    while (accepting.get()) {
                        try {
                            Socket socket = ss.accept();

                            if (!accepting.get()) {
                                socket.close();
                                break;
                            }

                            BufferedReader reader = new BufferedReader(
                                    new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                            BufferedWriter writer = new BufferedWriter(
                                    new java.io.OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

                            IdentityService.Subject subject = new SocketSubject(socket);
                            SocketConnectedClient client = new SocketConnectedClient(socket, reader, writer, subject);
                            openClients.add(client);

                            Thread.ofPlatform().name("mock-network-client-" + socket.getPort()).start(() -> {
                                try {
                                    serviceProvider.onConnection(client);
                                } finally {
                                    openClients.remove(client);
                                    client.close();
                                }
                            });
                        } catch (IOException e) {
                            if (accepting.get()) {
                                logger.warn("Error accepting client connection", e);
                            }
                            break;
                        }
                    }
                } catch (IOException e) {
                    logger.warn("Failed to start mock listener on port {}", listenPort, e);
                } finally {
                    listenerStarted.countDown();
                    accepting.set(false);
                    stopped.set(true);
                    listenerDone.countDown();
                }
            });

            try {
                listenerStarted.await();
                if (serverSocket == null || serverSocket.isClosed()) {
                    throw new IllegalStateException("Mock listener failed to start on port " + listenPort);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while starting mock listener", e);
            }
        }

        @Override
        public int shutdown() {
            accepting.set(false);
            closeServerSocket();
            return openClients.size();
        }

        @Override
        public void awaitShutdownCompletion() {
            try {
                listenerDone.await();
                while (!openClients.isEmpty()) {
                    Thread.sleep(10L);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void stop() {
            accepting.set(false);
            stopped.set(true);
            closeServerSocket();

            for (SocketConnectedClient client : openClients) {
                client.close();
            }
            openClients.clear();
        }

        private void closeServerSocket() {
            ServerSocket ss = serverSocket;
            if (ss != null && !ss.isClosed()) {
                try {
                    ss.close();
                } catch (IOException e) {
                    logger.debug("Error closing server socket", e);
                }
            }
        }
    }

    private static final class SocketConnectedClient extends ConnectedClient {
        private final Socket socket;

        private SocketConnectedClient(
                Socket socket,
                BufferedReader reader,
                BufferedWriter writer,
                IdentityService.Subject subject
        ) {
            super(reader, writer, subject);
            this.socket = socket;
        }

        @Override
        public void close() {
            super.close();
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static final class SocketConnectedPeer extends ConnectedPeer {
        private final Socket socket;

        private SocketConnectedPeer(
                Socket socket,
                BufferedReader reader,
                BufferedWriter writer,
                PersistenceService.Peer peer
        ) {
            super(reader, writer, peer);
            this.socket = socket;
        }

        @Override
        public void close() {
            super.close();
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static final class SocketSubject implements IdentityService.Subject {
        private final String principal;
        private final long identifier;

        private SocketSubject(Socket socket) {
            String host = socket.getInetAddress() != null
                    ? socket.getInetAddress().getHostAddress()
                    : "unknown";
            this.principal = host + ":" + socket.getPort();
            this.identifier = principal.hashCode();
        }

        @Override
        public String getPrincipal() {
            return principal;
        }

        @Override
        public long getIdentifier() {
            return identifier;
        }
    }
}
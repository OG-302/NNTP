package org.anarplex.lib.nntp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class MockNetworkServiceTest {

    @Test
    @DisplayName("MockNetworkService accepts a TCP client and serves ProtocolEngine responses")
    void acceptsTcpClientAndRunsProtocolEngine() throws Exception {
        int port = findFreePort();

        MockNetworkService networkService = new MockNetworkService(port);
        InMemoryPersistence persistence = new InMemoryPersistence();
        CountDownLatch connectionHandled = new CountDownLatch(1);

        NetworkService.ConnectionListener listener = networkService.registerService(client -> {
            try {
                ProtocolEngine engine = new ProtocolEngine(
                        persistence,
                        new MockIdentityService(),
                        new MockPolicyService(),
                        client
                );
                engine.start();
            } finally {
                connectionHandled.countDown();
            }
        });

        listener.start();

        try (
                Socket socket = new Socket("127.0.0.1", port);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))
        ) {
            String greeting = reader.readLine();
            assertNotNull(greeting, "Expected greeting from server");
            assertTrue(greeting.startsWith("201 "), "Expected 201 greeting but got: " + greeting);

            writer.write("QUIT\r\n");
            writer.flush();

            String quitReply = reader.readLine();
            assertNotNull(quitReply, "Expected QUIT response from server");
            assertTrue(quitReply.startsWith("205 "), "Expected 205 reply but got: " + quitReply);

            assertTrue(connectionHandled.await(2, TimeUnit.SECONDS), "Expected connection to be handled");
        } finally {
            listener.stop();
            listener.awaitShutdownCompletion();
        }
    }

    @Test
    @DisplayName("connectToPeer establishes socket connection and loads capabilities")
    void connectToPeerEstablishesConnectionAndLoadsCapabilities() throws Exception {
        int port = findFreePort();
        CountDownLatch peerSessionHandled = new CountDownLatch(1);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            Thread.ofPlatform().name("mock-peer-server").start(() -> {
                try (
                        Socket socket = serverSocket.accept();
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                        BufferedWriter writer = new BufferedWriter(
                                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))
                ) {
                    writer.write("200 mock-peer ready\r\n");
                    writer.flush();

                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("CAPABILITIES")) {
                            writer.write("101 Capability list follows\r\n");
                            writer.write("VERSION 2\r\n");
                            writer.write("LIST\r\n");
                            writer.write("READER\r\n");
                            writer.write(".\r\n");
                            writer.flush();
                        } else if (line.startsWith("QUIT")) {
                            writer.write("205 closing connection\r\n");
                            writer.flush();
                            break;
                        } else {
                            writer.write("500 unknown command\r\n");
                            writer.flush();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    peerSessionHandled.countDown();
                }
            });

            MockNetworkService networkService = new MockNetworkService(findFreePort());
            TestPeer peer = new TestPeer(7, "peer-under-test", "127.0.0.1:" + port);

            NetworkService.ConnectedPeer connectedPeer = networkService.connectToPeer(peer);

            assertNotNull(connectedPeer, "Expected a ConnectedPeer instance");
            assertTrue(connectedPeer.isConnected(), "Expected peer to be connected");
            assertEquals("127.0.0.1:" + port, connectedPeer.getAddress());
            assertEquals("peer-under-test", connectedPeer.getLabel());
            assertTrue(connectedPeer.hasCapability(Specification.NNTP_Server_Capabilities.LIST),
                    "Expected LIST capability");
            assertTrue(connectedPeer.hasCapability(Specification.NNTP_Server_Capabilities.READER),
                    "Expected READER capability");

            connectedPeer.close();

            assertTrue(peerSessionHandled.await(2, TimeUnit.SECONDS), "Expected peer session to complete");
            assertFalse(peer.isDisabled(), "Peer should remain enabled");
        }
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class TestPeer extends PersistenceService.Peer {
        private final int id;
        private final String address;
        private final long identifier;
        private String label;
        private boolean disabled;
        private Instant listLastFetched;

        private TestPeer(int id, String label, String address) {
            this.id = id;
            this.label = label;
            this.address = address;
            this.identifier = id;
        }

        @Override
        public int getID() {
            return id;
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public boolean isDisabled() {
            return disabled;
        }

        @Override
        public void setDisabledStatus(boolean disabled) {
            this.disabled = disabled;
        }

        @Override
        public String getLabel() {
            return label;
        }

        @Override
        public void setLabel(String label) {
            this.label = label;
        }

        @Override
        public Instant getListLastFetched() {
            return listLastFetched;
        }

        @Override
        protected void setListLastFetched(Instant lastFetched) {
            this.listLastFetched = lastFetched;
        }

        @Override
        public String getPrincipal() {
            return label;
        }

        @Override
        public long getIdentifier() {
            return identifier;
        }
    }
}

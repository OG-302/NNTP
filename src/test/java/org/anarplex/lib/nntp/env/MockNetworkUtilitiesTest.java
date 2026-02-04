package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.ProtocolEngine;
import org.anarplex.lib.nntp.Specification;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class MockNetworkUtilitiesTest {

    private final static String HOST = "127.0.0.1";
    private final static int PORT = 3119;

    @Test
    void testConnectToPeer() throws IOException, InterruptedException {
        Properties props = new Properties();
        props.setProperty("host", HOST);
        props.setProperty("port", String.valueOf(PORT));

        MockNetworkUtilities networkUtils = MockNetworkUtilities.getInstance(props);

        NetworkUtilities.ConnectionListener server =
        // register our protocol engine as a handler which will process incoming connections
        networkUtils.registerService(connectedClient -> {
            try (
                    PersistenceService persistenceService = new MockPersistenceService();
                    IdentityService identityService = new MockIdentityService();
                    PolicyService policyService = new MockPolicyService()
            ) {
                // Setup backend storage
                persistenceService.init();

                // wire-up protocol engine
                ProtocolEngine protocolEngine = new ProtocolEngine(persistenceService, identityService, policyService, connectedClient);

                // start the engine to process incoming commands
                if (!protocolEngine.start()) {
                    System.err.println("Error encountered during client communication.");
                }
            } catch (IOException e) {
                System.err.println("Error handling client: " + e.getMessage());
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                connectedClient.close();
            }
        });
        server.start();

        // connect client to server
        PersistenceService.Peer peer = new PersistenceService.Peer() {

            @Override
            public String getPrincipal() {
                return getAddress();
            }

            @Override
            public int getID() {
                return 1;
            }

            @Override
            public String getAddress() {
                return "nntp://"+props.getProperty("host")+":"+props.getProperty("port");
            }

            @Override
            public String getLabel() {
                return "test peer";
            }

            @Override
            public void setLabel(String label) {
            }

            @Override
            public boolean getDisabledStatus() {
                return false;
            }

            @Override
            public void setDisabledStatus(boolean disabled) {
            }

            @Override
            public LocalDateTime getListLastFetched() {
                return null;
            }

            @Override
            public void setListLastFetched(LocalDateTime lastFetched) {
            }
        };
        NetworkUtilities.ConnectedPeer connectedPeer = networkUtils.connectToPeer(peer);

        // expect to find a connected client
        assertNotNull(connectedPeer);

        // check that the server has READER capability
        assertTrue(connectedPeer.hasCapability(Specification.NNTP_Server_Capabilities.READER));

        // send the QUIT command
        connectedPeer.sendCommand(Specification.NNTP_Request_Commands.QUIT);

        // check that the QUIT command was successful and got the success code
        Specification.NNTP_Response_Code responseCode = connectedPeer.getResponseCode();
        assertEquals(Specification.NNTP_Response_Code.Code_205, responseCode);

        // check that QUIT actually closed the connection
        assertFalse(connectedPeer.isConnected());

        // kill the server thread
        server.stop();
    }

    @Test
    void testDefaults() {
        assertEquals(String.valueOf(PORT), MockNetworkUtilities.networkEnv.getProperty("port"));
    }
}

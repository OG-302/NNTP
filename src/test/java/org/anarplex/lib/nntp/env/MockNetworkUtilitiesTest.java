package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.ProtocolEngine;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class MockNetworkUtilitiesTest {

    @Test
    void testConnectToPeer() throws IOException, InterruptedException {
        Properties props = new Properties();
        props.setProperty("host", "localhost");
        props.setProperty("port", "3119");

        MockNetworkUtilities networkUtils = MockNetworkUtilities.getInstance(props);

        // start server
        NetworkUtilities.ServiceManager serviceManager = networkUtils.registerService(newConnection -> {
            try (
                    PersistenceService persistenceService = new MockPersistenceService();
                    IdentityService identityService = new MockIdentityService();
                    PolicyService policyService = new MockPolicyService()
            ) {
                // Setup backend storage
                persistenceService.init();

                // wire-up protocol engine
                ProtocolEngine protocolEngine = new ProtocolEngine(persistenceService, identityService, policyService, newConnection);

                // let 'er rip
                if (!protocolEngine.start()) {
                    System.err.println("Error encountered during client communication.");
                }
            } catch (IOException e) {
                System.err.println("Error handling client: " + e.getMessage());
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                newConnection.closeConnection();
            }
        });
        serviceManager.start();

        // connect client to server
        PersistenceService.Peer peer = new PersistenceService.Peer() {

            @Override
            public String getPrincipal() {
                return getAddress();
            }

            @Override
            public String getAddress() {
                return props.getProperty("host");
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
        Thread.sleep(2000); // wait for the server to start
        NetworkUtilities.ProtocolStreams clientSideStreams = networkUtils.connectToPeer(peer);

        // expect to find a welcome message
        BufferedReader reader = clientSideStreams.getReader();
        String initialResponse = new String(reader.readLine().getBytes(StandardCharsets.UTF_8));
        assertTrue(initialResponse.startsWith("200"));

        // send the QUIT command
        PrintWriter sw = clientSideStreams.getWriter();
        sw.print("QUIT\r\n");
        sw.flush();
        // and check response
        String finalResponse = reader.readLine();
        assertTrue(finalResponse.startsWith("205"));

        clientSideStreams.closeConnection();
        serviceManager.terminate();
    }

    @Test
    void testDefaults() {
        assertEquals("3119", MockNetworkUtilities.networkEnv.getProperty("port"));
    }
}

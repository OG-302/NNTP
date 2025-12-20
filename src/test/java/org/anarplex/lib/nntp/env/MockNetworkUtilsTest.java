package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.ProtocolEngine;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MockNetworkUtilsTest {

    @Test
    void testConnectToPeer() throws IOException, InterruptedException {
        MockNetworkUtils networkUtils = new MockNetworkUtils();
        Properties props = new Properties();
        props.setProperty("host", "localhost");
        props.setProperty("port", "3119");

        // start server
        NetworkUtils.ServiceManager serviceManager = networkUtils.registerService(new NetworkUtils.ConnectionListener() {
            @Override
            public void onConnection(NetworkUtils.ProtocolStreams newConnection) {
                try (
                        PersistenceService persistenceService = new MockPersistenceService();
                        IdentityService identityService = new MockIdentityService();
                        PolicyService policyService = new MockPolicyService();
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
                    newConnection.close();
                }
            }
        }, props);
        serviceManager.start();

        // connect client to server
        PersistenceService.Peer peer = new PersistenceService.Peer() {
            @Override
            public int getPk() {
                return 0;
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
            public boolean getDisabledStatus() {
                return false;
            }

            @Override
            public void setDisabledStatus(boolean disabled) {
            }

            @Override
            public Date getListLastFetched() {
                return null;
            }

            @Override
            public void setListLastFetched(Date lastFetched) {
            }
        };

        NetworkUtils.ProtocolStreams clientSideStreams = networkUtils.connectToPeer(peer, props);

        // expect to find a welcome message
        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSideStreams.getInputStream()));
        String initialResponse = new String(reader.readLine().getBytes(StandardCharsets.UTF_8));
        assertTrue(initialResponse.startsWith("200"));

        // send the QUIT command
        BufferedOutputStream sw = new BufferedOutputStream(clientSideStreams.getOutputStream());
        sw.write("QUIT\r\n".getBytes(StandardCharsets.UTF_8));
        sw.flush();
        // and check response
        String finalResponse = new String(reader.readLine().getBytes(StandardCharsets.UTF_8));
        assertTrue(finalResponse.startsWith("205"));

        clientSideStreams.close();
        serviceManager.terminate();
    }

    @Test
    void testDefaults() {
        assertEquals("3119", MockNetworkUtils.defaults.getProperty("port"));
    }
}

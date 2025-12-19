package org.anarplex.lib.nntp.utils;

import org.anarplex.lib.nntp.ProtocolEngine;
import org.anarplex.lib.nntp.env.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class NNTP_Service {

    /**
     * Create an NNTP Server on a specified listening port with specified backend store.
     * Service remains active until killed.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // Open port for listening
        NetworkUtils networkUtils = new MockNetworkUtils();

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
        }, MockNetworkUtils.defaults);

        serviceManager.start();
    }
}

package org.anarplex.lib.nntp.utils;

import org.anarplex.lib.nntp.ProtocolEngine;
import org.anarplex.lib.nntp.env.*;

import java.io.IOException;

public class NNTP_Service {

    /**
     * Create an NNTP Server on a specified listening port with specified backend store.
     * Service remains active until killed.
     */
    public static void main(String[] args) throws Exception {

        // Open port for listening
        NetworkUtilities networkUtilities = MockNetworkUtilities.getInstance(MockNetworkUtilities.networkEnv);

        NetworkUtilities.ServiceManager serviceManager = networkUtilities.registerService(new NetworkUtilities.ConnectionListener() {
            @Override
            public void onConnection(NetworkUtilities.ProtocolStreams newConnection) {
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
                    newConnection.closeConnection();
                }
            }
        });

        serviceManager.start();
    }
}

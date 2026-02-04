package org.anarplex.lib.nntp.utils;

import org.anarplex.lib.nntp.ProtocolEngine;
import org.anarplex.lib.nntp.Specification;
import org.anarplex.lib.nntp.env.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;

import static org.anarplex.lib.nntp.ProtocolEngine.NNTP_SERVER;

public class MockNntpService {

    private static final Logger logger = LoggerFactory.getLogger(MockNntpService.class);

    /**
     * Create an NNTP Server on a specified listening port.
     * Service remains active until killed.
     */
    public static void main(String[] args) {

        final int DEFAULT_NNTP_PORT = 119;
        // check for program arguments.  First arg is listening port for nntp service.

        int port = (args.length > 0) ? Integer.parseInt(args[0]) : DEFAULT_NNTP_PORT;
        Properties networkEnv = new Properties();
        networkEnv.setProperty("nntp.port", String.valueOf(port));

        // Open port for listening
        NetworkUtilities networkUtilities = MockNetworkUtilities.getInstance(networkEnv);

        NetworkUtilities.ConnectionListener connectionListener =
            networkUtilities.registerService(clientConnection -> {
                try (
                        // each new client connection gets its own services
                        PersistenceService persistenceService = new MockPersistenceService();
                        IdentityService identityService = new MockIdentityService();
                        PolicyService policyService = new MockPolicyService()
                ) {
                    // ready storage service
                    persistenceService.init();

                    // wire-up NNTP ProtocolEngine
                    ProtocolEngine protocolEngine = new ProtocolEngine(persistenceService, identityService, policyService, clientConnection);

                    // add a test newsgroup if not already present to the persistence store
                    Specification.NewsgroupName testGroupName = new Specification.NewsgroupName("test.local");
                    PersistenceService.Newsgroup testGroup = persistenceService.getGroupByName(testGroupName);
                    if (testGroup == null) {
                        persistenceService.addGroup(testGroupName, "Local group for testing server activity", Specification.PostingMode.Allowed, LocalDateTime.now(), NNTP_SERVER, false);
                    }
                    persistenceService.commit();

                    // begin processing client requests from the input stream and send results to the output stream
                    if (!protocolEngine.start()) {
                        logger.error("Error encountered during client communication.");
                    }
                } catch (IOException e) {
                    logger.error("Error handling client: {}", e.getMessage());
                } catch (Exception e) {
                    logger.error("Unexpected error: {}", e.getMessage());
                } finally {
                    // client connection is no longer being used
                    clientConnection.close();
                }
            });

        connectionListener.start();

        connectionListener.awaitShutdownCompletion();
    }
}

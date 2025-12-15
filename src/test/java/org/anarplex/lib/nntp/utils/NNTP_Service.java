package org.anarplex.lib.nntp.utils;

import org.anarplex.lib.nntp.Server;
import org.anarplex.lib.nntp.ext.IdentityService;
import org.anarplex.lib.nntp.ext.NetworkUtils;
import org.anarplex.lib.nntp.ext.PersistenceService;
import org.anarplex.lib.nntp.ext.PolicyService;
import org.anarplex.lib.nntp.ext.MockIdentityService;
import org.anarplex.lib.nntp.ext.MockPolicyService;
import org.anarplex.lib.nntp.ext.MockPersistenceService;

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
        NetworkUtils networkUtils = new NetworkUtilsTest();
        ServerSocket serverSocket = networkUtils.getServerSocket(NetworkUtils.PROTOCOL_TCP, NetworkUtils.DEFAULT_PORT);
        if (serverSocket == null) {
            System.err.println("Unable to open port for listening");
            System.exit(1);
        }

        System.out.println("Listening on port " + serverSocket.getLocalPort());

        // don't wait for the first client to connect in order to initialize the backend storage
        PersistenceService persistenceService = new MockPersistenceService();
        persistenceService.init();
        persistenceService.close();

        while (true) {
            // Accept a new client connection
            Socket clientSocket = serverSocket.accept();
            System.out.println("New client connected: " + clientSocket.getInetAddress());

            // Create a new thread to handle the client
            new Thread(new ClientHandler(clientSocket)).start();
        }
    }

    // Handler class for processing client communication
    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (
                    PersistenceService persistenceService = new MockPersistenceService();

                    IdentityService identityService = new MockIdentityService();
                    PolicyService policyService = new MockPolicyService();

                    InputStreamReader inputStream = new InputStreamReader(clientSocket.getInputStream());
                    OutputStreamWriter outputStream = new OutputStreamWriter(clientSocket.getOutputStream())
            ) {
                persistenceService.init();

                // Setup backend storage
                Server server = new Server(persistenceService, identityService, policyService, inputStream, outputStream);

                if (!server.process()) {
                    System.err.println("Error encountered during client communication.");
                }
            } catch (IOException e) {
                System.err.println("Error handling client: " + e.getMessage());
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing client socket: " + e.getMessage());
                }
            }
        }
    }
}

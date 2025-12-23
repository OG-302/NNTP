package org.anarplex.lib.nntp.env;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public interface NetworkUtils {

    /**
     * The communication pathway between clients and the NNTP Protocol Engine, and also between the Peer Synchronizer
     * and other Peers.
     */
    interface ProtocolStreams {
        InputStream getInputStream();
        OutputStream getOutputStream();
        void close();
    }

    /**
     * Connects to the specified Peer and returns a ProtocolStream for communication.
     */
    ProtocolStreams connectToPeer(PersistenceService.Peer peer, Properties p) throws IOException;

    /**
     * Opens a port (specified by supplied properties) for listening for incoming connections and dispatches such
     * new connections to the supplied ConnectionListener.  Returns a ServiceManager that can be used to stop the
     * Service.
     */
    ServiceManager registerService(ConnectionListener cl, Properties p) throws IOException;

    interface ConnectionListener {
        void onConnection(ProtocolStreams newConnection);
    }

    interface ServiceManager {
        void start();
        void terminate();
    }

}

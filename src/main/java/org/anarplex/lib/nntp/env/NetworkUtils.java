package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.ProtocolEngine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public interface NetworkUtils {

    interface ProtocolStreams {
        InputStream getInputStream();
        OutputStream getOutputStream();
        void close();
    }

    /**
     * Connects to the specified Peer and returns a ProtocolStream for communication.
     * @param peer
     * @return
     * @throws IOException
     */
    ProtocolStreams connectToPeer(PersistenceService.Peer peer, Properties p) throws IOException;

    ServiceManager registerService(ConnectionListener cl, Properties p) throws IOException;

    interface ConnectionListener {
        void onConnection(ProtocolStreams newConnection);
    }

    interface ServiceManager {
        void start();
        void terminate();
    }








}

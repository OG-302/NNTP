package org.anarplex.lib.nntp.ext;

import java.io.IOException;
import java.net.ServerSocket;

public interface NetworkUtils {

    String PROTOCOL_TCP = "tcp";
    String PROTOCOL_UDP = "udp";
    int DEFAULT_PORT = 119;

    /**
     * Open a server socket for listening.
     * @param protocol
     * @param port
     * @return
     * @throws IOException
     */
    ServerSocket getServerSocket(String protocol, int port) throws IOException;

}

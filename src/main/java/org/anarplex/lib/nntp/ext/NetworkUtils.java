package org.anarplex.lib.nntp.ext;

import java.io.IOException;
import java.net.ServerSocket;

public interface NetworkUtils {

    static String PROTOCOL_TCP = "tcp";
    static String PROTOCOL_UDP = "udp";
    static int DEFAULT_PORT = 119;

    /**
     * Open a server socket for listening.
     * @param protocol
     * @param port
     * @return
     * @throws IOException
     */
    ServerSocket getServerSocket(String protocol, int port) throws IOException;

}

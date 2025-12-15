package org.anarplex.lib.nntp.utils;

import org.anarplex.lib.nntp.ext.NetworkUtils;

import java.io.IOException;
import java.net.ServerSocket;

public class NetworkUtilsTest implements NetworkUtils {

    public NetworkUtilsTest() {
    }

    @Override
    public ServerSocket getServerSocket(String protocol, int port) throws IOException {
        if (protocol.equals(NetworkUtils.PROTOCOL_TCP)) {
            return new ServerSocket(port);
        }
        return null;
    }
}

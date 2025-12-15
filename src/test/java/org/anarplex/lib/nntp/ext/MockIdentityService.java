package org.anarplex.lib.nntp.ext;

import org.anarplex.lib.nntp.utils.RandomNumber;
import org.anarplex.lib.nntp.Spec;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;

public class MockIdentityService implements IdentityService {
    private boolean closed;

    @Override
    public Long authenticate(String identity, String credentials) {
        return 0L;
    }

    @Override
    public boolean isValid(long token) {
        return false;
    }

    @Override
    public boolean authorizedToPost(long token, PersistenceService.Newsgroup newsgroup) {
        return false;
    }

    @Override
    public Spec.MessageId createMessageID(Map<String, Set<String>> articleHeaders) {
        try {
            long rn = RandomNumber.generate10DigitNumber();
            return new Spec.MessageId( '<'+String.valueOf(rn)+'>' ) ;
        } catch (Spec.MessageId.InvalidMessageIdException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getHostIdentifier() {
        // obtain hostname
        String hostname = null;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) { }
        if (hostname == null || hostname.isBlank()) {
            hostname = System.getenv("HOSTNAME");
        }
        if (hostname == null || hostname.isBlank()) {
            hostname = System.getenv("COMPUTERNAME");
        }
        if (hostname == null || hostname.isBlank()) {
            hostname = "localhost";
        }

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] d = md.digest(hostname.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(d.length * 2); char[] HEX = "0123456789abcdef".toCharArray();
        for (byte b : d) { sb.append(HEX[(b >>> 4) & 0x0F]).append(HEX[b & 0x0F]); }
        return sb.toString();
    }

    @Override
    public void close() throws Exception {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}

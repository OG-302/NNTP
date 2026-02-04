package org.anarplex.lib.nntp.env;

import org.anarplex.lib.nntp.Specification;
import org.anarplex.lib.nntp.utils.RandomNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;

public class MockIdentityService implements IdentityService {
    private static final Logger logger = LoggerFactory.getLogger(MockIdentityService.class);

    private boolean closed;

    @Override
    public Boolean requiresPassword(Subject subject) {
        // TODO
        return true;
    }

    /**
     * Mock authentication.
     * @return 0 for invalid authentication, 1 for authenticated with limited privileges, 2 for authenticated with full privileges
     */
    @Override
    public Long authenticate(IdentityService.Subject identity, String credentials) {
        if (identity != null && identity.getPrincipal() != null && identity.getPrincipal().equals(credentials)) {
            // authenticated.
            return (identity.getPrincipal().toUpperCase().equals(identity.getPrincipal())) ? 2L : 1L;
        } else {
            return 0L;
        }
    }

    @Override
    public boolean isValid(long token) {
        return 0 < token;
    }

    @Override
    public Specification.MessageId createMessageID(Map<String, Set<String>> articleHeaders) {
        try {
            long rn = RandomNumber.generate10DigitNumber();
            return new Specification.MessageId( '<'+String.valueOf(rn)+'>' ) ;
        } catch (Specification.MessageId.InvalidMessageIdException e) {
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

        MessageDigest md;
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
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}

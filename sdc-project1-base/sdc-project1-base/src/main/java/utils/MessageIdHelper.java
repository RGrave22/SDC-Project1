package utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MessageIdHelper {

    public static String generateUniqueID(String payload, long timestamp) throws NoSuchAlgorithmException {
        String msg = payload + "-" + timestamp;

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(msg.getBytes(StandardCharsets.UTF_8));


        StringBuilder hexString = new StringBuilder();
        for (byte b : digest) {
            hexString.append(String.format("%02x", b));
        }

        System.out.println("MD5 hash: " + hexString.toString());

        return hexString.toString();
    }
}

package utils;

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;

public class SignaturesHelper {

    public static final String SignatureAlgorithm = "SHA256withRSA";

    public static byte[] generateSignature(byte[] value, PrivateKey key) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        Signature sig = Signature.getInstance(SignaturesHelper.SignatureAlgorithm);
        sig.initSign(key);
        sig.update(value);
        return sig.sign();
    }

    public static boolean checkSignature(byte[] value, byte[] signature, PublicKey key) throws SignatureException, NoSuchAlgorithmException, InvalidKeyException {
        Signature sig = Signature.getInstance(SignaturesHelper.SignatureAlgorithm);
        sig.initVerify(key);
        sig.update(value);
        return sig.verify(signature);
    }

    public static byte[] digest(byte[] value) {

        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Erro ao calcular digest SHA-256", e);
        }

    }

}

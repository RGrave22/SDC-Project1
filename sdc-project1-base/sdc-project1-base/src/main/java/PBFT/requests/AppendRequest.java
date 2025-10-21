package PBFT.requests;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.SignaturesHelper;

public class AppendRequest extends ProtoRequest {

    public static final short REQUEST_ID = 902;

    private final UUID requestID;
    private final String clientID;
    private Host source;
    private final byte[] clientRequest;
    private byte[] serverSignature;
    private final long seqNo;

    public AppendRequest(String clientID, Host source, byte[] bs, long seqNo) {
        super(REQUEST_ID);
        this.requestID = UUID.randomUUID();
        this.clientID = clientID;
        this.source = source;
        this.clientRequest = bs;
        this.serverSignature = null;
        this.seqNo = seqNo;
    }

    public AppendRequest(UUID requestID, String clientID, Host source, byte[] cr, byte[] ssign, long seqNo) {
        super(REQUEST_ID);
        this.requestID = requestID;
        this.clientID = clientID;
        this.source = source;
        this.clientRequest = cr;
        this.serverSignature = ssign;
        this.seqNo = seqNo;
    }

    public void sign(PrivateKey key) throws InvalidKeyException, NoSuchAlgorithmException, SignatureException, IOException {
        ByteBuf buf = Unpooled.buffer();
        buf.writeLong(requestID.getMostSignificantBits());
        buf.writeLong(requestID.getLeastSignificantBits());
        buf.writeBytes(clientID.getBytes());
        Host.serializer.serialize(source, buf);
        buf.writeBytes(clientRequest);
        buf.writeLong(seqNo);

        buf.resetReaderIndex();
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);

        serverSignature = SignaturesHelper.generateSignature(payload, key);
    }

    public boolean checkSignature(PublicKey key) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, IOException {
        ByteBuf buf = Unpooled.buffer();
        buf.writeLong(requestID.getMostSignificantBits());
        buf.writeLong(requestID.getLeastSignificantBits());
        buf.writeBytes(clientID.getBytes());
        Host.serializer.serialize(source, buf);
        buf.writeBytes(clientRequest);
        buf.writeLong(seqNo);

        buf.resetReaderIndex();
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);

        return SignaturesHelper.checkSignature(payload, serverSignature, key);
    }

    public byte[] getServerSignature() {
        return serverSignature;
    }

    public UUID getRequestID() {
        return requestID;
    }

    public String getClientID() {
        return clientID;
    }

    public byte[] getClientRequest() {
        return clientRequest;
    }

    public Host getSource() {
        return source;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public static void serialize(AppendRequest msg, ByteBuf out) throws IOException {

        out.writeLong(msg.getRequestID().getMostSignificantBits());
        out.writeLong(msg.getRequestID().getLeastSignificantBits());

        byte[] clientBytes = msg.getClientID().getBytes();
        out.writeInt(clientBytes.length);
        out.writeBytes(clientBytes);

        Host.serializer.serialize(msg.source, out);

        out.writeInt(msg.getClientRequest().length);
        out.writeBytes(msg.getClientRequest());

        if (msg.getServerSignature() != null) {
            out.writeInt(msg.getServerSignature().length);
            out.writeBytes(msg.getServerSignature());
        } else {
            out.writeInt(0);
        }

        out.writeLong(msg.getSeqNo());

    }

    public static byte[] serialize2(AppendRequest msg, ByteBuf out) throws IOException {

        out.writeLong(msg.getRequestID().getMostSignificantBits());
        out.writeLong(msg.getRequestID().getLeastSignificantBits());

        byte[] clientBytes = msg.getClientID().getBytes();
        out.writeInt(clientBytes.length);
        out.writeBytes(clientBytes);

        Host.serializer.serialize(msg.source, out);

        out.writeInt(msg.getClientRequest().length);
        out.writeBytes(msg.getClientRequest());

        if (msg.getServerSignature() != null) {
            out.writeInt(msg.getServerSignature().length);
            out.writeBytes(msg.getServerSignature());
        } else {
            out.writeInt(0);
        }

        out.writeLong(msg.getSeqNo());

        byte[] result = new byte[out.readableBytes()];
        out.readBytes(result);
        return result;
    }

    public static AppendRequest deserialize(ByteBuf in) throws IOException {

        UUID reqID = new UUID(in.readLong(), in.readLong());

        byte[] clientBytes = new byte[in.readInt()];
        in.readBytes(clientBytes);

        String clientID = new String(clientBytes);

        Host source = Host.serializer.deserialize(in);

        byte[] data = new byte[in.readInt()];
        in.readBytes(data);

        int sigLen = in.readInt();
        byte[] sig = sigLen > 0 ? new byte[sigLen] : null;
        if (sig != null) {
            in.readBytes(sig);
        }

        long seqno = in.readLong();

        return new AppendRequest(reqID, clientID, source, data, sig, seqno);
    }

}

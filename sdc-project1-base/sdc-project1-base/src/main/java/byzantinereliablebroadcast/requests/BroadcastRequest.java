package byzantinereliablebroadcast.requests;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.SignaturesHelper;

import java.io.IOException;
import java.security.*;
import java.util.UUID;

public class BroadcastRequest extends ProtoRequest {

    public static final short REQUEST_ID = 901;

    private final UUID requestID;
    private final String appendReqId;
    private final String clientID;
    private Host source;
    private final byte[] clientRequest;
    private byte[] serverSignature;


    public BroadcastRequest(String appendReqId, String  clientID, Host source, byte[] bs) {
        super(REQUEST_ID);
        this.requestID = UUID.randomUUID();
        this.appendReqId = appendReqId;
        this.clientID = clientID;
        this.source = source;
        this.clientRequest = bs;
        this.serverSignature = null;
    }

    public BroadcastRequest(UUID requestID,String appendReqId, String clientID, Host source, byte[] cr, byte[] ssign) {
        super(REQUEST_ID);
        this.requestID = requestID;
        this.appendReqId = appendReqId;
        this.clientID = clientID;
        this.source = source;
        this.clientRequest = cr;
        this.serverSignature = ssign;
    }

    public void sign(PrivateKey key) throws InvalidKeyException, NoSuchAlgorithmException, SignatureException, IOException {
        ByteBuf buf = Unpooled.buffer();
        buf.writeLong(requestID.getMostSignificantBits());
        buf.writeLong(requestID.getLeastSignificantBits());
        buf.writeBytes(clientID.getBytes());
        Host.serializer.serialize(source, buf);
        buf.writeBytes(clientRequest);

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

    public String getAppendReqId(){
        return appendReqId;
    }

    public Host getSource() {
        return source;
    }


    public byte[] serialize() throws IOException {
        ByteBuf buf = Unpooled.buffer();

        buf.writeLong(requestID.getMostSignificantBits());
        buf.writeLong(requestID.getLeastSignificantBits());

        byte[] appendReqIdBytes = appendReqId.getBytes();
        buf.writeInt(appendReqIdBytes.length);
        buf.writeBytes(appendReqIdBytes);

        byte[] clientIdBytes = clientID.getBytes();
        buf.writeInt(clientIdBytes.length);
        buf.writeBytes(clientIdBytes);

        Host.serializer.serialize(source, buf);

        buf.writeInt(clientRequest.length);
        buf.writeBytes(clientRequest);

        if (serverSignature != null) {
            buf.writeInt(serverSignature.length);
            buf.writeBytes(serverSignature);
        } else {
            buf.writeInt(0);
        }

        byte[] result = new byte[buf.readableBytes()];
        buf.readBytes(result);
        return result;
    }

    public static BroadcastRequest deserialize(byte[] data) throws IOException {
        ByteBuf buf = Unpooled.wrappedBuffer(data);

        long msb = buf.readLong();
        long lsb = buf.readLong();
        UUID requestID = new UUID(msb, lsb);

        int appendReqIdLength = buf.readInt();
        byte[] appendReqIdBytes = new byte[appendReqIdLength];
        buf.readBytes(appendReqIdBytes);
        String appendReqId = new String(appendReqIdBytes);

        int clientIdLength = buf.readInt();
        byte[] clientIdBytes = new byte[clientIdLength];
        buf.readBytes(clientIdBytes);
        String clientID = new String(clientIdBytes);

        Host source = Host.serializer.deserialize(buf);

        int clientRequestLength = buf.readInt();
        byte[] clientRequest = new byte[clientRequestLength];
        buf.readBytes(clientRequest);

        int signatureLength = buf.readInt();
        byte[] serverSignature = null;
        if (signatureLength > 0) {
            serverSignature = new byte[signatureLength];
            buf.readBytes(serverSignature);
        }

        return new BroadcastRequest(requestID, appendReqId, clientID, source, clientRequest, serverSignature);
    }
}

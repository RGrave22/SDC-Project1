package PBFT.messages;

import java.io.IOException;

import PBFT.requests.AppendRequest;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class FowardAppendMessage extends SignedProtoMessage {

    public static final short MESSAGE_ID = 1010;

    private final AppendRequest req;
    private final byte[] digest;
    private final Host sender;

    public FowardAppendMessage(AppendRequest req, byte[] digest, Host sender) {
        super(MESSAGE_ID);
        this.req = req;
        this.digest = digest;
        this.sender = sender;
    }

    public AppendRequest getRequest() {
        return req;
    }

    public byte[] getDigest() {
        return digest;
    }

    public Host getSender() {
        return sender;
    }

    public final static SignedMessageSerializer<FowardAppendMessage> serializer
            = new SignedMessageSerializer<FowardAppendMessage>() {

        @Override
        public void serializeBody(FowardAppendMessage m, ByteBuf out) throws IOException {

            out.writeInt(m.digest == null ? 0 : m.digest.length);
            if (m.digest != null) {
                out.writeBytes(m.digest);
            }

            Host.serializer.serialize(m.sender, out);

            AppendRequest.serialize(m.req, out);
        }

        @Override
        public FowardAppendMessage deserializeBody(ByteBuf in) throws IOException {

            int len = in.readInt();
            byte[] digest = new byte[len];
            if (len > 0) {
                in.readBytes(digest);
            }

            Host sender = Host.serializer.deserialize(in);

            AppendRequest req = AppendRequest.deserialize(in);

            return new FowardAppendMessage(req, digest, sender);
        }
    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return FowardAppendMessage.serializer;
    }

}

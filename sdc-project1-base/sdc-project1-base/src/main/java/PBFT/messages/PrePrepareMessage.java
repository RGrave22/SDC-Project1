package PBFT.messages;

import java.io.IOException;
import java.util.Arrays;

import PBFT.requests.AppendRequest;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class PrePrepareMessage extends SignedProtoMessage {

    public static final short MESSAGE_ID = 1001;

    private final long view;
    private final long seq;
    private final byte[] digest;
    private final Host sender;
    private final AppendRequest request;

    public PrePrepareMessage(Host sender, long view, long seq, byte[] digest, AppendRequest request) {
        super(MESSAGE_ID);
        this.sender = sender;
        this.view = view;
        this.seq = seq;
        this.digest = digest;
        this.request = request;
    }

    public long getView() {
        return view;
    }

    public long getSeq() {
        return seq;
    }

    public byte[] getDigest() {
        return digest;
    }

    public Host getSender() {
        return sender;
    }

    public AppendRequest getRequest() {
        return request;
    }

    public final static SignedMessageSerializer<PrePrepareMessage> serializer = new SignedMessageSerializer<PrePrepareMessage>() {

        @Override
        public void serializeBody(PrePrepareMessage m, ByteBuf out) throws IOException {

            Host.serializer.serialize(m.sender, out);

            out.writeLong(m.view);
            out.writeLong(m.seq);

            out.writeInt(m.digest.length);
            out.writeBytes(m.digest);

            AppendRequest.serialize(m.request, out);

        }

        @Override
        public PrePrepareMessage deserializeBody(ByteBuf in) throws IOException {

            Host h = Host.serializer.deserialize(in);

            long view = in.readLong();
            long seq = in.readLong();

            byte[] digest = new byte[in.readInt()];
            in.readBytes(digest);

            AppendRequest req = AppendRequest.deserialize(in);

            PrePrepareMessage msg = new PrePrepareMessage(h, view, seq, digest, req);

            if (!Arrays.equals(msg.getDigest(), digest)) {
                throw new IOException("A mensagem foi alterada.");
            }

            return msg;

        }

    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return PrePrepareMessage.serializer;
    }

}

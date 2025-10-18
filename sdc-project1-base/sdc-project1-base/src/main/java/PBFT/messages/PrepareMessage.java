package PBFT.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class PrepareMessage extends SignedProtoMessage {

    public static final short MESSAGE_ID = 1002;

    private final long view;
    private final long seq;
    private final byte[] digest;
    private final Host sender;

    public PrepareMessage(long view, long seq, byte[] digest, Host sender) {
        super(MESSAGE_ID);
        this.view = view;
        this.seq = seq;
        this.digest = digest;
        this.sender = sender;
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

    public final static SignedMessageSerializer<PrepareMessage> serializer = new SignedMessageSerializer<PrepareMessage>() {

        @Override
        public void serializeBody(PrepareMessage m, ByteBuf out) throws IOException {

            out.writeLong(m.view);
            out.writeLong(m.seq);

            out.writeInt(m.digest == null ? 0 : m.digest.length);
            if (m.digest != null) {
                out.writeBytes(m.digest);
            }

            Host.serializer.serialize(m.sender, out);

        }

        @Override
        public PrepareMessage deserializeBody(ByteBuf in) throws IOException {

            long view = in.readLong();
            long seq = in.readLong();

            int len = in.readInt();
            byte[] digest = new byte[len];
            if (len > 0) {
                in.readBytes(digest);
            }

            Host sender = Host.serializer.deserialize(in);

            return new PrepareMessage(view, seq, digest, sender);
        }

    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return PrepareMessage.serializer;
    }
}

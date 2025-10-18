package PBFT.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class ViewChangeMessage extends SignedProtoMessage {

    public static final short MESSAGE_ID = 1004;

    private final long novaView;
    private final byte[] sign;
    private final Host replica;

    public ViewChangeMessage(long view, Host replica, byte[] signature) {
        super(MESSAGE_ID);
        this.novaView = view;
        this.sign = signature;
        this.replica = replica;
    }

    public long getNovaView() {
        return novaView;
    }

    public byte[] getSign() {
        return sign;
    }

    public Host getReplica() {
        return replica;
    }

    public final static SignedMessageSerializer<ViewChangeMessage> serializer = new SignedMessageSerializer<ViewChangeMessage>() {

        @Override
        public void serializeBody(ViewChangeMessage m, ByteBuf out) throws IOException {

            out.writeLong(m.novaView);

            Host.serializer.serialize(m.replica, out);

            out.writeInt(m.sign == null ? 0 : m.sign.length);
            if (m.sign != null) {
                out.writeBytes(m.sign);
            }

        }

        @Override
        public ViewChangeMessage deserializeBody(ByteBuf in) throws IOException {

            long novaView = in.readLong();

            Host replica = Host.serializer.deserialize(in);

            int len = in.readInt();
            byte[] signature = new byte[len];
            if (len > 0) {
                in.readBytes(signature);
            }

            return new ViewChangeMessage(novaView, replica, signature);
        }

    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return ViewChangeMessage.serializer;
    }
}

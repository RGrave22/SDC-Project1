package blockchainBRB.messages;

import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;

public class BRBReadyMessage extends SignedProtoMessage {

    public final static short MESSAGE_ID = 1003;

    private final Host myself;
    private final String messageId;
    private final byte[] payload;

    public BRBReadyMessage(Host me, String messageId, byte[]  payload){
        super(MESSAGE_ID);
        this.myself = me;
        this.messageId = messageId;
        this.payload = payload;
    }

    public String getMessageId() {
        return messageId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Host getSender() {
        return myself;
    }


    public final static SignedMessageSerializer<BRBReadyMessage> serializer = new SignedMessageSerializer<BRBReadyMessage>() {

        @Override
        public void serializeBody(BRBReadyMessage m, ByteBuf out) throws IOException {
            Host.serializer.serialize(m.myself, out);


            byte[] idBytes = m.messageId.getBytes();
            out.writeInt(idBytes.length);
            out.writeBytes(idBytes);

            out.writeInt(m.getPayload().length);
            out.writeBytes(m.getPayload());
        }

        @Override
        public BRBReadyMessage deserializeBody(ByteBuf in) throws IOException {
            Host h = Host.serializer.deserialize(in);

            byte[] idBytes =  new byte[in.readInt()];
            in.readBytes(idBytes);
            String messageId = new String(idBytes);

            byte[] payload = new byte[in.readInt()];
            in.readBytes(payload);

            return new BRBReadyMessage(h, messageId, payload);
        }

    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return BRBReadyMessage.serializer;
    }



}



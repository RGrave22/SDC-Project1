package PBFT.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;

public class NewViewMessage extends SignedProtoMessage {

    public static final short MESSAGE_ID = 1005;

    private final long novaView;
    private final List<ViewChangeMessage> viewChanges;
    private final List<PrePrepareMessage> prePreparedChanges;
    private final byte[] sign;

    public NewViewMessage(long view, List<ViewChangeMessage> viewChanges, List<PrePrepareMessage> prePreparedChanges, byte[] sign) {
        super(MESSAGE_ID);
        this.novaView = view;
        this.viewChanges = viewChanges;
        this.prePreparedChanges = prePreparedChanges;
        this.sign = sign;
    }

    public long getNovaView() {
        return novaView;
    }

    public byte[] getSign() {
        return sign;
    }

    public List<ViewChangeMessage> getViewChanges() {
        return viewChanges;
    }

    public List<PrePrepareMessage> getPrePreparedChanges() {
        return prePreparedChanges;
    }

    public final static SignedMessageSerializer<NewViewMessage> serializer = new SignedMessageSerializer<NewViewMessage>() {

        @Override
        public void serializeBody(NewViewMessage m, ByteBuf out) throws IOException {

            out.writeLong(m.novaView);

            out.writeInt(m.sign == null ? 0 : m.sign.length);
            if (m.sign != null) {
                out.writeBytes(m.sign);
            }

            out.writeInt(m.viewChanges.size());

            for (ViewChangeMessage vcm : m.viewChanges) {
                ViewChangeMessage.serializer.serializeBody(vcm, out);
            }

            out.writeInt(m.prePreparedChanges.size());

            for (PrePrepareMessage ppm : m.prePreparedChanges) {
                PrePrepareMessage.serializer.serializeBody(ppm, out);
            }

        }

        @Override
        public NewViewMessage deserializeBody(ByteBuf in) throws IOException {

            long novaView = in.readLong();

            int len = in.readInt();
            byte[] signature = new byte[len];
            if (len > 0) {
                in.readBytes(signature);
            }

            int listSize = in.readInt();
            List<ViewChangeMessage> viewChanges = new ArrayList<>();
            for (int i = 0; i < listSize; i++) {
                viewChanges.add(ViewChangeMessage.serializer.deserializeBody(in));
            }

            int listSize2 = in.readInt();
            List<PrePrepareMessage> prePreparedChanges = new ArrayList<>();
            for (int i = 0; i < listSize2; i++) {
                prePreparedChanges.add(PrePrepareMessage.serializer.deserializeBody(in));
            }

            return new NewViewMessage(novaView, viewChanges, prePreparedChanges, signature);
        }

    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return NewViewMessage.serializer;
    }
}

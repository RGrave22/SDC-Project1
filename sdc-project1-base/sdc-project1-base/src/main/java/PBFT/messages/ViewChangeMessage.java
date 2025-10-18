package PBFT.messages;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class ViewChangeMessage extends SignedProtoMessage {

    public static final short MESSAGE_ID = 1004;

    private final long novaView;
    private final List<CommitMessage> doneOper;
    private final List<PrePrepareMessage> notDoneOper;
    private final Host replica;
    private byte[] sign;

    public ViewChangeMessage(long view, Host replica, byte[] signature, List<CommitMessage> doneOper, List<PrePrepareMessage> notDoneOper) {
        super(MESSAGE_ID);
        this.novaView = view;
        this.sign = signature;
        this.replica = replica;
        this.doneOper = doneOper;
        this.notDoneOper = notDoneOper;
    }

    public long getNovaView() {
        return novaView;
    }

    public byte[] getSign() {
        return sign;
    }

    public void setSign(byte[] signature) {
        this.sign = signature;
    }

    public Host getReplica() {
        return replica;
    }

    public List<CommitMessage> getDoneOper() {
        return doneOper;
    }

    public List<PrePrepareMessage> getNotDoneOper() {
        return notDoneOper;
    }

    public final static SignedMessageSerializer<ViewChangeMessage> serializer = new SignedMessageSerializer<ViewChangeMessage>() {

        @Override
        public void serializeBody(ViewChangeMessage m, ByteBuf out) throws IOException {

            out.writeLong(m.novaView);

            Host.serializer.serialize(m.replica, out);

            if (m.doneOper != null) {
                out.writeInt(m.doneOper.size());
                for (CommitMessage cm : m.doneOper) {
                    CommitMessage.serializer.serialize(cm, out);
                }
            } else {
                out.writeInt(0);
            }

            if (m.notDoneOper != null) {
                out.writeInt(m.notDoneOper.size());
                for (PrePrepareMessage ppm : m.notDoneOper) {
                    PrePrepareMessage.serializer.serialize(ppm, out);
                }
            } else {
                out.writeInt(0);
            }

            out.writeInt(m.sign == null ? 0 : m.sign.length);
            if (m.sign != null) {
                out.writeBytes(m.sign);
            }

        }

        @Override
        public ViewChangeMessage deserializeBody(ByteBuf in) throws IOException {

            long novaView = in.readLong();

            Host replica = Host.serializer.deserialize(in);

            int doneSize = in.readInt();
            List<CommitMessage> doneOper = new ArrayList<>(doneSize);
            for (int i = 0; i < doneSize; i++) {
                doneOper.add(CommitMessage.serializer.deserialize(in));
            }

            int notDoneSize = in.readInt();
            List<PrePrepareMessage> notDoneOper = new ArrayList<>(notDoneSize);
            for (int i = 0; i < notDoneSize; i++) {
                notDoneOper.add(PrePrepareMessage.serializer.deserialize(in));
            }

            int len = in.readInt();
            byte[] signature = new byte[len];
            if (len > 0) {
                in.readBytes(signature);
            }

            return new ViewChangeMessage(novaView, replica, signature, doneOper, notDoneOper);
        }

    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return ViewChangeMessage.serializer;
    }

    public static byte[] getBytesForSigning(ViewChangeMessage msg) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try {
            dos.writeLong(msg.getNovaView());

            dos.writeUTF(msg.getReplica().toString());

            if (msg.getDoneOper() != null) {
                dos.writeInt(msg.getDoneOper().size());
                for (CommitMessage cm : msg.getDoneOper()) {
                    dos.write(cm.getDigest());
                }
            } else {
                dos.writeInt(0);
            }

            if (msg.getNotDoneOper() != null) {
                dos.writeInt(msg.getNotDoneOper().size());
                for (PrePrepareMessage ppm : msg.getNotDoneOper()) {
                    dos.write(ppm.getDigest());
                }
            } else {
                dos.writeInt(0);
            }

            dos.flush();

        } catch (IOException ex) {
            throw new RuntimeException("Erro ao gerar assinatura para view message.");
        }
        return bos.toByteArray();
    }
}

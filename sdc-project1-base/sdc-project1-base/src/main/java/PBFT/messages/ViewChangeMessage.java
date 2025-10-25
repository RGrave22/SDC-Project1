package PBFT.messages;

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

    public ViewChangeMessage(long view, Host replica, List<CommitMessage> doneOper, List<PrePrepareMessage> notDoneOper) {
        super(MESSAGE_ID);
        this.novaView = view;
        this.replica = replica;
        this.doneOper = doneOper;
        this.notDoneOper = notDoneOper;
    }

    public long getNovaView() {
        return novaView;
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

            return new ViewChangeMessage(novaView, replica, doneOper, notDoneOper);
        }

    };

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return ViewChangeMessage.serializer;
    }
}

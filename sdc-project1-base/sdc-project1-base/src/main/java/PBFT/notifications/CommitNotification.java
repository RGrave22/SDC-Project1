package PBFT.notifications;

import PBFT.requests.AppendRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class CommitNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 1009;

    private final long seqNo;
    private final AppendRequest req;

    public CommitNotification(long seqNo, AppendRequest req) {
        super(NOTIFICATION_ID);
        this.seqNo = seqNo;
        this.req = req;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public AppendRequest getRequest() {
        return req;
    }

}

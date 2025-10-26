package SMR.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class CheckPointRequest extends ProtoRequest {

    public static final short REQUEST_ID = 1011;

    private final long lastAppliedSeqNo;

    public CheckPointRequest(long lastAppliedSeqNo) {
        super(REQUEST_ID);
        this.lastAppliedSeqNo = lastAppliedSeqNo;
    }

    public long getLastAppliedSeqNo() {
        return lastAppliedSeqNo;
    }
}

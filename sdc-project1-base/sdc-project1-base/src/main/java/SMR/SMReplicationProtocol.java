package SMR;

import java.io.IOException;
import java.util.Properties;

import PBFT.PBFTAgreement;
import PBFT.requests.AppendRequest;
import SMR.timers.ViewChangeTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;

public class SMReplicationProtocol extends GenericProtocol {

    public static final short PROTO_ID = 1000;
    public static final String PROTO_NAME = "SMReplication";

    private PBFTAgreement agreement;

    public SMReplicationProtocol() {
        super(PROTO_NAME, PROTO_ID);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        this.agreement = new PBFTAgreement(this);

        agreement.init(props);

        registerRequestHandler(AppendRequest.REQUEST_ID, this::onAppend);

        setupPeriodicTimer(new ViewChangeTimer(), 3000, 3000);

        registerTimerHandler(ViewChangeTimer.TIMER_ID, (ViewChangeTimer t) -> {
            agreement.checkPrimaryTimeout();
        });

    }

    private void onAppend(AppendRequest req, short fromProto) {
        agreement.propose(req);
    }

}

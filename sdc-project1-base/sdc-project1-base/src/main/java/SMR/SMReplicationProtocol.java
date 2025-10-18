package SMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import PBFT.PBFTAgreement;
import PBFT.notifications.DeliverEntryNotification;
import PBFT.requests.AppendRequest;
import SMR.timers.ViewChangeTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;

public class SMReplicationProtocol extends GenericProtocol {

    public static final short PROTO_ID = 1000;
    public static final String PROTO_NAME = "SMReplication";

    private PBFTAgreement agreement;
    private final Map<Long, AppendRequest> log = new TreeMap<>();

    public SMReplicationProtocol() {
        super(PROTO_NAME, PROTO_ID);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        this.agreement = new PBFTAgreement();

        registerRequestHandler(AppendRequest.REQUEST_ID, this::onAppend);

        subscribeNotification(DeliverEntryNotification.NOTIFICATION_ID, this::onDeliverEntry);

        setupPeriodicTimer(new ViewChangeTimer(), 3000, 3000);

        registerTimerHandler(ViewChangeTimer.TIMER_ID, (ViewChangeTimer t) -> {
            agreement.checkPrimaryTimeout();
        });

    }

    private void onAppend(AppendRequest req, short fromProto) {
        agreement.propose(req);
    }

    private void onDeliverEntry(DeliverEntryNotification notification, short sourceProto) {

        AppendRequest req = new AppendRequest(notification.getRequestID(), notification.getClientID(), null, notification.getClientRequest(), notification.getServerSignature(), notification.getSeqNo());

        log.put(notification.getSeqNo(), req);

    }

    public List<AppendRequest> getStateMachineLog() {
        return new ArrayList<>(log.values());
    }

}

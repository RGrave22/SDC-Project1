package SMR;

import java.io.IOException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import PBFT.PBFTAgreement;
import PBFT.notifications.CommitNotification;
import PBFT.notifications.NewViewNotification;
import PBFT.requests.AppendRequest;
import SMR.notifications.DeliverEntryNotification;
import SMR.timers.ViewChangeTimer;
import common.events.NeighborDown;
import common.events.NeighborUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

public class SMReplicationProtocol extends GenericProtocol {

    public static final short PROTO_ID = 1000;
    public static final String PROTO_NAME = "SMReplication";

    private final short underlyingProtocolID = 1007;

    private PBFTAgreement agreement;
    private final ConcurrentMap<Long, AppendRequest> log;
    private final Map<UUID, AppendRequest> pendingRequests;
    private final List<Host> neighbors;
    private long lastApplied = 0;
    private final Map<Host, PublicKey> publicKeys;
    private int f;

    public SMReplicationProtocol() {
        super(PROTO_NAME, PROTO_ID);
        this.log = new ConcurrentSkipListMap<>();
        this.pendingRequests = new ConcurrentHashMap<>();
        this.neighbors = new ArrayList<>();
        this.publicKeys = new ConcurrentHashMap<>();
        this.f = 1;
    }

    public void setAgreement(PBFTAgreement agreement) {
        System.out.println("TESTE");
        this.agreement = agreement;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        registerRequestHandler(AppendRequest.REQUEST_ID, this::onAppend);

        subscribeNotification(CommitNotification.NOTIFICATION_ID, this::onCommitNotification);

        subscribeNotification(NewViewNotification.NOTIFICATION_ID, this::onNewView);

        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUpNotification);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDownNotification);

        setupPeriodicTimer(new ViewChangeTimer(), 3000, 3000);

        registerTimerHandler(ViewChangeTimer.TIMER_ID, (ViewChangeTimer t, long v) -> {
            if (agreement != null) {
                agreement.checkPrimaryTimeout();
            }
        });

    }

    public void uponNeighborUpNotification(NeighborUp notification, short sourceProtoID) {
        Host h = notification.getNeighbor();
        if (!neighbors.contains(h)) {
            this.neighbors.add(h);
        }
        this.publicKeys.put(h, notification.getPublicKey());
        this.f = Math.max((neighbors.size() - 1) / 3, 1);
        if (agreement != null) {
            agreement.setF(f);
            agreement.setNeighbor(new ArrayList<>(neighbors));
            agreement.setPublicKeys(new HashMap<>(publicKeys));
            agreement.updatePrimary();
        }
    }

    public void uponNeighborDownNotification(NeighborDown notification, short sourceProtoID) {
        Host h = notification.getNeighbor();
        if (neighbors.size() > 4 && neighbors.contains(h)) {
            this.neighbors.remove(h);
            this.publicKeys.remove(h);
            this.f = Math.max((neighbors.size() - 1) / 3, 1);
            if (agreement != null) {
                agreement.setF(f);
                agreement.setNeighbor(new ArrayList<>(neighbors));
                agreement.setPublicKeys(new HashMap<>(publicKeys));
                agreement.updatePrimary();
            }
        }
    }

    private void onAppend(AppendRequest req, short fromProto) {
        System.err.println("A Receber...");
        if (agreement != null) {
            pendingRequests.put(req.getRequestID(), req);
            sendRequest(req, underlyingProtocolID);
        }
    }

    private void onCommitNotification(CommitNotification notification, short sourceProto) {

        long seqNo = notification.getSeqNo();
        AppendRequest req = notification.getRequest();

        log.putIfAbsent(seqNo, req);

        pendingRequests.remove(req.getRequestID());

        tryApplyPending(seqNo);

    }

    public List<AppendRequest> getStateMachineLog() {
        return new ArrayList<>(log.values());
    }

    private void onNewView(NewViewNotification notification, short sourceProto) {

    }

    private void tryApplyPending(long seqNo) {

        long next = lastApplied + 1;

        while (true) {

            AppendRequest req = log.get(next);
            if (req == null) {
                break;
            }

            lastApplied = next;

            triggerNotification(new DeliverEntryNotification(req.getRequestID(), req.getClientID(), req.getClientRequest(), req.getServerSignature(), seqNo));

            next++;
        }
    }

}

package PBFT;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import PBFT.messages.CommitMessage;
import PBFT.messages.NewViewMessage;
import PBFT.messages.PrePrepareMessage;
import PBFT.messages.PrepareMessage;
import PBFT.messages.ViewChangeMessage;
import PBFT.notifications.DeliverEntryNotification;
import PBFT.notifications.NewViewNotification;
import PBFT.requests.AppendRequest;
import common.events.ChannelAvailable;
import common.events.NeighborDown;
import common.events.NeighborUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.SignaturesHelper;

public class PBFTAgreement extends GenericProtocol {

    public static final short PROTO_ID = 1007;
    public static final String PROTO_NAME = "PBFTAgreement";

    private final Map<Long, PBFTLogEntry> log;

    private static final long PRIMARY_TIMEOUT = 5000;
    private long lastPrimaryResponseTime;

    private final Map<Long, Set<Host>> viewChangeVotes;
    private final Map<Long, List<ViewChangeMessage>> receivedViewChanges;
    private final Set<Long> startedViews;
    private final Set<Long> sentViewChange;
    private PrivateKey privateKey;
    private final Map<Host, PublicKey> publicKeys;

    private int channelID;
    private final List<Host> neighbors;
    private Host self;
    private Host primary;

    private long seqNo;
    private int f;
    private long currentView;

    public PBFTAgreement() {
        super(PROTO_NAME, PROTO_ID);
        this.seqNo = 0;
        this.f = 1;
        this.currentView = 0;
        this.log = new HashMap<>();
        this.lastPrimaryResponseTime = System.currentTimeMillis();
        this.viewChangeVotes = new HashMap<>();
        this.receivedViewChanges = new HashMap<>();
        this.startedViews = new HashSet<>();
        this.sentViewChange = new HashSet<>();
        this.publicKeys = new HashMap<>();
        this.neighbors = new ArrayList<>();
    }

    private boolean amPrimary() {
        return self != null && primary != null && primary.equals(self);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::handleChannelAvailableNotification);
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUpNotification);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDownNotification);

        registerRequestHandler(AppendRequest.REQUEST_ID, this::handleRequest);

    }

    public void handleChannelAvailableNotification(ChannelAvailable notification, short sourceProto) {

        this.self = notification.getMyHost();
        this.privateKey = notification.getMyPrivateKey();
        this.channelID = notification.getChannelID();

        registerSharedChannel(this.channelID);
        setDefaultChannel(this.channelID);

        //Register Message Serializers
        registerMessageSerializer(channelID, PrePrepareMessage.MESSAGE_ID, PrePrepareMessage.serializer);
        registerMessageSerializer(channelID, PrepareMessage.MESSAGE_ID, PrepareMessage.serializer);
        registerMessageSerializer(channelID, CommitMessage.MESSAGE_ID, CommitMessage.serializer);
        registerMessageSerializer(channelID, ViewChangeMessage.MESSAGE_ID, ViewChangeMessage.serializer);
        registerMessageSerializer(channelID, NewViewMessage.MESSAGE_ID, NewViewMessage.serializer);

        //Setup Message Handlers
        try {
            registerMessageHandler(channelID, PrePrepareMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onPrePrepare((PrePrepareMessage) msg, from));
            registerMessageHandler(channelID, PrepareMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onPrepare((PrepareMessage) msg, from));
            registerMessageHandler(channelID, CommitMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onCommit((CommitMessage) msg, from));
            registerMessageHandler(channelID, ViewChangeMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onViewChange((ViewChangeMessage) msg, from));
            registerMessageHandler(channelID, NewViewMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onNewView((NewViewMessage) msg, from));
        } catch (HandlerRegistrationException e) {
            throw new RuntimeException("Erro ao iniciar mensagens.");
        }
    }

    public void uponNeighborUpNotification(NeighborUp notification, short sourceProtoID) {
        this.neighbors.add(notification.getNeighbor());
        this.publicKeys.put(notification.getNeighbor(), notification.getPublicKey());
        f = (neighbors.size() - 1) / 3;
    }

    public void uponNeighborDownNotification(NeighborDown notification, short sourceProtoID) {
        this.neighbors.remove(notification.getNeighbor());
        this.publicKeys.remove(notification.getNeighbor());
        f = Math.max((neighbors.size() - 1) / 3, 1);
    }

    public void handleRequest(AppendRequest req, short sourceProto) {
        if (amPrimary()) {
            propose(req);
        }
    }

    public void propose(AppendRequest req) {
        if (!amPrimary()) {
            return;
        }

        long seq = ++seqNo;
        byte[] digest = SignaturesHelper.digest(req.getClientRequest());
        PBFTLogEntry entry = new PBFTLogEntry(seq, currentView, req, digest);
        log.put(seq, entry);

        PrePrepareMessage ppm = new PrePrepareMessage(self, currentView, seq, digest, req);
        broadcast(ppm);
    }

    private void broadcast(ProtoMessage msg) {
        for (Host h : neighbors) {
            if (!h.equals(self)) {
                sendMessage(msg, h);
            }
        }
    }

    private void onPrePrepare(PrePrepareMessage msg, Host from) {

        onMessageFromPrimary();

        byte[] digest = SignaturesHelper.digest(msg.getRequest().getClientRequest());
        if (!Arrays.equals(digest, msg.getDigest())) {
            return;
        }

        PBFTLogEntry entry = new PBFTLogEntry(msg.getSeq(), msg.getView(), msg.getRequest(), digest);
        log.put(msg.getSeq(), entry);

        PrepareMessage prepare = new PrepareMessage(msg.getView(), msg.getSeq(), digest, self);
        broadcast(prepare);
    }

    private void onPrepare(PrepareMessage msg, Host from) {

        PBFTLogEntry entry = log.get(msg.getSeq());
        if (entry == null) {
            return;
        }

        entry.addPrepare(from);
        if (entry.getPreparesCount() > 2 * f) {
            CommitMessage cm = new CommitMessage(msg.getView(), msg.getSeq(), entry.getDigest(), self);
            broadcast(cm);
        }
    }

    private void onCommit(CommitMessage msg, Host from) {

        PBFTLogEntry entry = log.get(msg.getSeq());
        if (entry == null) {
            return;
        }

        entry.addCommit(from);
        if (entry.getCommitsCount() >= 2 * f + 1 && !entry.isDelivered()) {
            entry.markDelivered();
            AppendRequest req = entry.getRequest();
            triggerNotification(new DeliverEntryNotification(
                    req.getRequestID(),
                    req.getClientID(),
                    req.getClientRequest(),
                    req.getServerSignature(),
                    entry.getSeq()
            ));
        }
    }

    public void onViewChange(ViewChangeMessage msg, Host from) {

        try {
            PublicKey pub = publicKeys.get(from);
            byte[] data = ViewChangeMessage.getBytesForSigning(msg);
            if (pub == null || !SignaturesHelper.checkSignature(data, msg.getSign(), pub)) {
                return;
            }
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            return;
        }

        long v = msg.getNovaView();

        receivedViewChanges.computeIfAbsent(v, k -> new ArrayList<>()).add(msg);
        viewChangeVotes.computeIfAbsent(v, k -> new HashSet<>()).add(from);

        Set<Host> votes = viewChangeVotes.get(v);

        if (!amPrimary() && votes.size() >= f + 1 && !sentViewChange.contains(v)) {
            startViewChange(v);
            sentViewChange.add(v);
        }

        if (votes.size() >= (2 * f + 1)) {

            if (v > currentView) {
                currentView = v;
            }

            primary = neighbors.get((int) (currentView % neighbors.size()));
            if (amPrimary() && !startedViews.contains(v)) {
                startedViews.add(v);
                startNewView(v);
            }
        }
    }

    private void startViewChange(long v) {

        List<PrePrepareMessage> prePrePare = collectPendingPrePrepares(v);
        List<CommitMessage> commits = collectCommits(v);

        byte[] sign = null;

        ViewChangeMessage vcm = new ViewChangeMessage(v, self, sign, commits, prePrePare);

        try {
            byte[] data = ViewChangeMessage.getBytesForSigning(vcm);
            sign = SignaturesHelper.generateSignature(data, privateKey);
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            throw new RuntimeException("Erro ao assinar a mensagem", e);
        }

        vcm.setSign(sign);

        broadcast(vcm);

        if (amPrimary()) {
            long maxSeq = log.keySet().stream().mapToLong(Long::longValue).max().orElse(seqNo);
            seqNo = maxSeq;
            for (PBFTLogEntry entry : log.values()) {
                if (!entry.isDelivered()) {
                    propose(entry.getRequest());
                }
            }
        }
    }

    private void startNewView(long v) {

        List<ViewChangeMessage> vChanges = receivedViewChanges.get(v);
        List<PrePrepareMessage> prePrepares = collectPendingPrePrepares(v);

        byte[] sign = null;

        NewViewMessage nvm = new NewViewMessage(v, vChanges, prePrepares, sign);

        try {
            byte[] data = NewViewMessage.getBytesForSigning(nvm);
            sign = SignaturesHelper.generateSignature(data, privateKey);
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            throw new RuntimeException("Erro ao assinar a mensagem new view message", e);
        }

        nvm.setSign(sign);

        broadcast(nvm);

        triggerNotification(new NewViewNotification(v));

        if (amPrimary()) {
            for (PBFTLogEntry entry : log.values()) {
                if (!entry.isDelivered()) {
                    propose(entry.getRequest());
                }
            }
        }

    }

    public void onNewView(NewViewMessage msg, Host from) {

        try {
            PublicKey pub = publicKeys.get(from);
            byte[] data = NewViewMessage.getBytesForSigning(msg);
            if (pub == null || !SignaturesHelper.checkSignature(data, msg.getSign(), pub)) {
                return;
            }
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            return;
        }

        onMessageFromPrimary();

        for (PrePrepareMessage ppm : msg.getPrePreparedChanges()) {
            if (!log.containsKey(ppm.getSeq())) {
                PBFTLogEntry entry = new PBFTLogEntry(ppm.getSeq(), ppm.getView(), ppm.getRequest(), ppm.getDigest());
                log.put(ppm.getSeq(), entry);
                PrepareMessage prepare = new PrepareMessage(ppm.getView(), ppm.getSeq(), ppm.getDigest(), self);
                broadcast(prepare);
            }
        }

        if (msg.getNovaView() > currentView) {
            currentView = msg.getNovaView();
            primary = neighbors.get((int) (currentView % neighbors.size()));
        }
    }

    public void onMessageFromPrimary() {
        lastPrimaryResponseTime = System.currentTimeMillis();
    }

    public void checkPrimaryTimeout() {
        if (!amPrimary() && System.currentTimeMillis() - lastPrimaryResponseTime > PRIMARY_TIMEOUT) {
            startViewChange(currentView + 1);
        }
    }

    private List<PrePrepareMessage> collectPendingPrePrepares(long newView) {
        List<PrePrepareMessage> pending = new ArrayList<>();
        for (PBFTLogEntry e : log.values()) {
            if (!e.isDelivered() && e.getView() <= newView) {
                PrePrepareMessage ppm = new PrePrepareMessage(self, e.getView(), e.getSeq(), e.getDigest(),
                        e.getRequest());
                pending.add(ppm);
            }
        }
        return pending;
    }

    private List<CommitMessage> collectCommits(long newView) {
        List<CommitMessage> done = new ArrayList<>();
        for (PBFTLogEntry e : log.values()) {
            if (e.isDelivered() && e.getView() <= newView) {
                CommitMessage cm = new CommitMessage(e.getView(), e.getSeq(), e.getDigest(),
                        self);
                done.add(cm);
            }
        }
        return done;
    }

}
